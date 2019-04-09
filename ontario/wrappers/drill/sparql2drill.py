
__author__ = 'Kemele M. Endris'

from ontario.sparql.parser import queryParser as qp
from multiprocessing import Process, Queue
from ontario.wrappers.mysql.utils import *
from ontario.sparql.parser.services import Filter, Expression, Argument, unaryFunctor, binaryFunctor
from ontario.model.rml_model import TripleMapType, SubjectMap
from pydrill.client import PyDrill
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('ontario.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class DrillWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.drill = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        if ':' in self.url:
            self.host, self.port = self.url.split(':')
        else:
            self.host = self.url
            self.port = '8047'

        if len(self.datasource.mappings) == 0:
            self.mappings = self.config.load_mappings(self.datasource.mappingfiles, self.rdfmts)
        else:
            # self.mappings = self.config.mappings
            self.mappings = self.datasource.mappings

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):
        """
        Entry point for query execution on csv files
        :param querystr: string query
        :return:
        """
        from time import time
        # start = time()
        # print("Start:", start)
        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []
        # querytxt = query
        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)

        query_filters = [f for f in self.query.body.triples[0].triples if isinstance(f, Filter)]

        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset

        sqlquery, projvartocols, coltotemplates, filenametablename = self.translate(query_filters)
        # print(sqlquery)
        # totalres = 0
        if sqlquery is None or len(sqlquery) == 0:
            queue.put("EOF")
            return []
        try:
            start = time()
            try:
                self.drill = PyDrill(host=self.host, port=self.port)
            except Exception as ex:
                print("Exception while connecting to Drill", ex)
                queue.put("EOF")
                return
            if not self.drill.is_active():
                print('Exception: Please run Drill first')
                queue.put("EOF")
                return
            # print("Drill Initialization cost:", time() - start)
            logger.info("Drill Initialization cost:" + str(time() - start))
            start = time()
            if isinstance(sqlquery, list):
                sqlquery = [sql for sql in sqlquery if sql is not None and len(sql) > 0]
                if len(sqlquery) > 3:
                    sqlquery = " UNION ".join(sqlquery)
            if isinstance(sqlquery, list):
                sqlquery = [sql for sql in sqlquery if sql is not None and len(sql) > 0]
                # logger.info(" UNION ".join(sqlquery))
                processqueues = []
                processes = []
                res_dict = []
                for sql in sqlquery:
                    # processquery = Queue()
                    # self.run_union(sql, queue, projvartocols, coltotemplates, limit, processquery, res_dict)
                    # print(sql)
                    processquery = Queue()
                    processqueues.append(processquery)
                    p = Process(target=self.run_union, args=(sql, queue, projvartocols, coltotemplates, limit, processquery, res_dict,))
                    p.start()
                    processes.append(p)

                while len(processqueues) > 0:
                    toremove = []
                    try:
                        for q in processqueues:
                            if q.get(False) == 'EOF':
                                toremove.append(q)
                        for p in processes:
                            if p.is_alive():
                                p.terminate()
                    except:
                        pass
                    for q in toremove:
                        processqueues.remove(q)
                logger.info("Done running:")
                sw = " UNION ".join(sqlquery)
                logger.info(sw)
            else:
                card = 0
                # if limit == -1:
                limit = 1000
                if offset == -1:
                    offset = 0
                logger.info(sqlquery)
                # print(sqlquery)
                while True:
                    query_copy = sqlquery + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                    cardinality = self.process_result(query_copy, queue, projvartocols, coltotemplates)
                    card += cardinality
                    if cardinality < limit:
                        break

                    offset = offset + limit
            # print("Exec in Drill took:", time() - start)
            logger.info("Exec in Drill took:" + str(time() - start))
        except Exception as e:
            print("Exception ", e)
            pass
        # print('End:', time(), "Total results:", totalres)
        # print("Drill finished after: ", (time()-start))
        queue.put("EOF")

    def run_union(self, sql, queue, projvartocols, coltotemplates, limit, processqueue, res_dict):

        card = 0
        # if limit == -1:
        limit = 1000
        offset = 0
        while True:
            query_copy = sql + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            cardinality = self.process_result(query_copy, queue, projvartocols, coltotemplates, res_dict)
            card += cardinality
            if cardinality < limit:
                break

            offset = offset + limit

        processqueue.put("EOF")

    def process_result(self, sql, queue, projvartocols, coltotemplates, res_dict=None):
        c = 0
        try:
            if not self.drill.is_active():
                try:
                    self.drill = PyDrill(host=self.host, port=self.port)
                except Exception as ex:
                    print("Exception while connecting to Drill for query processing", ex)
                    return 0
            try:
                results = self.drill.query(sql, timeout=1000)
            except Exception as ex:
                print("Exception while running query to Drill for query processing", ex)
                return 0

            for row in results:
                c += 1
                # if res_dict is not None:
                #     rowtxt = ",".join(list(row.values()))
                #     if rowtxt in res_dict:
                #         continue
                #     else:
                #         res_dict.append(rowtxt)

                res = {}
                skip = False
                for r in row:
                    if row[r] == 'null':
                        skip = True
                        break
                    if '_' in r and r[:r.find("_")] in projvartocols:
                        s = r[:r.find("_")]
                        if s in res:
                            val = res[s]
                            if 'http://' in row[r]:
                                res[s] = row[r]
                            else:
                                res[s] = val.replace('{' + r[r.find("_") + 1:] + '}', row[r].replace(" ", '_'))
                        else:
                            if 'http://' in r:
                                res[s] = r
                            else:
                                res[s] = coltotemplates[s].replace('{' + r[r.find("_") + 1:] + '}',
                                                                   row[r].replace(" ", '_'))
                    elif r in projvartocols and r in coltotemplates:
                        if 'http://' in row[r]:
                            res[r] = row[r]
                        else:
                            res[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}', row[r].replace(" ", '_'))
                    else:
                        res[r] = row[r]

                if not skip:
                    queue.put(res)
                    # if 'keggCompoundId' in res:
                    #     print(res['keggCompoundId'])
            return c
        except Exception as e:
            print("Exception while processing drill results", e, sql)
            logger.error(sql)
            logger.error("Exception while processing results:" + str(e))
            import traceback
            traceback.print_stack()
            return c

    def get_so_variables(self, triples, proj):
        tvars = []
        for t in triples:
            if not t.subject.constant:
                tvars.append(t.subject.name)
            # exclude variables that are not projected
            if not t.theobject.constant:# and t.theobject.name in proj:
                tvars.append(t.theobject.name)

        return tvars

    def getsqlfil(self, l, r, op, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias):
        if r is not None and '?' in r.name:
            var = r.name
            val = l.name
        else:
            var = l.name
            val = r.name
        # print(val)
        if '(' in var and ')' in var:
            var = var[var.find('(') + 1:var.find(')')]

        if len(var_pred_map) == 0 or var == self.star['triples'][0].subject.name:
            subjcol = subjmap.value
            splits = subjcol.split('{')
            coltotemplates[var[1:]] = subjcol
            column = []
            for sp in splits[1:]:
                column.append(sp[:sp.find('}')])

            if len(column) > 1:
                objfilters = []
                for col in column:
                    vcolumn = "`" + col + '`'

                    if '<' in val and '>' in val:
                        val = val.replace('<', '').replace('>', '')
                    if '"' not in val and "'" not in val:
                        val = "'" + val + "'"

                    if op == 'REGEX':
                        val = "LOWER('%" + val[1:-1] + "%')"
                        objectfilter = 'LOWER(' + tablealias + '.' + vcolumn + ") LIKE " + val
                    else:
                        objectfilter = tablealias + '.' + vcolumn + op + val
                    objfilters.append(objectfilter)

                return " AND ".join(objfilters)
            elif len(column) == 1:
                column = "`" + column[0] + '`'

                if '<' in val and '>' in val:
                    val = val.replace('<', '').replace('>', '').replace(splits[0], '')

                if '"' not in val and "'" not in val:
                    val = "'" + val + "'"

                if op == 'REGEX':
                    val = "LOWER('%" + val[1:-1] + "%')"
                    objectfilter = 'LOWER(' + tablealias + '.' + column + ") LIKE " + val
                else:
                    objectfilter = tablealias + '.' + column + op + val

                return objectfilter
        if var not in var_pred_map:
            return None

        p = var_pred_map[var]
        pmap, omap = predicate_object_map[p]
        if omap.objectt.resource_type == TripleMapType.TEMPLATE:
            coltotemplates[var[1:]] = omap.objectt.value
            splits = omap.objectt.value.split('{')
            column = []
            for sp in splits[1:]:
                column.append(sp[:sp.find('}')])
            val = val.replace(splits[0], "")

            if len(column) == 1:
                column = column[0]

        elif omap.objectt.resource_type == TripleMapType.REFERENCE:
            column = omap.objectt.value
        else:
            column = []
        if isinstance(column, list):
            if len(column) > 0:
                column = column[0]
        column = "`" + column + '`'

        if '<' in val and '>' in val:
            val = val.replace('<', '').replace('>', '')
        if '"' not in val and "'" not in val:
            val = "'" + val + "'"

        if op == 'REGEX':
            val = "LOWER('%" + val[1:-1] + "%')"
            objectfilter = 'LOWER(' + tablealias + '.' + column + ") LIKE " + val
        else:
            objectfilter = tablealias + '.' + column + op + val

        return objectfilter

    def get_Expression_value(self, exp, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias):

        left = exp.left
        right = exp.right
        op = exp.op

        if op in unaryFunctor:
            if isinstance(left, Expression) and isinstance(left.left, Argument):
                left = left.left
                fil = self.getsqlfil(left, right, op, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias)
                return fil

        elif op in binaryFunctor:
            if op == 'REGEX' and right.desc is not False:
                if isinstance(left, Expression):
                    if 'xsd:string' in left.op:
                        left = left.left
                        fil = self.getsqlfil(left, right, op, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias)
                        return fil
                    # else:
                    #     left = self.get_Expression_value(left, var_pred_map, subjmap,predicate_object_map, coltotemplates, tablealias)
                    #     right = self.get_Expression_value(right, var_pred_map, subjmap, predicate_object_map, coltotemplates,tablealias)
                else:
                    fil = self.getsqlfil(left, right, op, var_pred_map, subjmap, predicate_object_map,
                                         coltotemplates, tablealias)
                    return fil
                # return op + "(" + str(left) + "," + right.name + "," + right.desc + ")"
            else:
                return op + "(" + str(left) + "," + str(right) + ")"

        elif right is None:
            return op + str(left)

        else:
            if isinstance(left, Argument) and isinstance(right, Argument):
                fil = self.getsqlfil(left, right, op, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias)
                return fil
            if isinstance(left, Expression) and isinstance(right, Expression):
                leftexp = self.get_Expression_value(left, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias)
                rightexp = self.get_Expression_value(right, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias)
                if op == '||' or op == '|':
                    if leftexp is None or rightexp is None:
                        return None
                    return '(' + leftexp + ' OR ' + rightexp + ')'
                else:
                    if leftexp is None or rightexp is None:
                        return None
                    return '(' + leftexp + ' AND ' + rightexp + ')'
            # print(op, type(left), left, type(right), right)
            return "(" + str(exp.left) + " " + exp.op + " " + str(exp.right)

    def get_obj_filter(self, f, var_pred_map, subjmap, predicate_object_map, coltotemplates, tablealias):
        return self.get_Expression_value(f.expr, var_pred_map, subjmap, predicate_object_map, coltotemplates,
                                         tablealias)

    def makeJoin(self, mapping_preds, query_filters):

        coltotemplates = {}
        projections = {}
        projvartocol = {}
        objectfilters = []
        constfilters = []
        fromclauses = []
        database_name = ""
        i = 0
        tm_tablealias = {}
        subjects = {}

        projvartocols = {}
        query = ""

        for tm, predicate_object_map in mapping_preds.items():
            sparqlprojected = set(self.get_so_variables(self.star['triples'], [c.name for c in self.query.args]))
            tablealias = 'Ontario_' + str(i)
            if isinstance(predicate_object_map, SubjectMap):
                subjvar = self.star['triples'][0].subject.name
                var = subjvar
                var_pred_map = {subjvar: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'}
                if predicate_object_map.subject.resource_type == TripleMapType.TEMPLATE:
                    coltotemplates[subjvar[1:]] = predicate_object_map.subject.value
                    splits = predicate_object_map.subject.value.split('{')
                    column = []
                    for sp in splits[1:]:
                        column.append(sp[:sp.find('}')])
                    if len(column) == 1:
                        column = column[0]
                elif predicate_object_map.subject.resource_type == TripleMapType.REFERENCE:
                    column = predicate_object_map.subject.value
                else:
                    column = []
                if isinstance(column, list):
                    j = 0
                    for col in column:
                        if '[*]' in col:
                            col = col.replace('[*]', '')
                            vcolumn = "`" + col + '`'
                            projections[var[1:] + '_Ontario_' + str(j)] = "FLATTEN(" + tablealias + "." + vcolumn + ") AS " + var[1:] + '_Ontario_' + str(j)
                        else:
                            vcolumn = "`" + col + '`'
                            projections[var[1:] + '_Ontario_' + str(j)] = tablealias + "." + vcolumn + " AS " + var[1:] + '_Ontario_' + str(j)
                        projvartocol.setdefault(var[1:], []).append(col)
                        objectfilters.append(tablealias + '.' + vcolumn + " is not null ")
                        objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                        j += 1
                else:
                    col = column
                    if '[*]' in col:
                        col = col.replace('[*]', '')
                        column = "`" + col + '`'
                        projections[var[1:]] = "FLATTEN(" + tablealias + "." + column + ") AS `" + var[1:] + '`'
                    else:
                        column = "`" + column + '`'
                        projections[var[1:]] = tablealias + "." + column + " AS `" + var[1:] + '`'
                    projvartocol[var[1:]] = col
                    objectfilters.append(tablealias + '.' + column + " is not null ")
                    objectfilters.append(tablealias + '.' + column + " <> '' ")
            else:
                var_pred_map = {var: pred for pred, var in self.star['predicates'].items() if
                                pred in predicate_object_map}
                column = []
                for var in sparqlprojected:
                    if var not in var_pred_map:
                        continue
                    p = var_pred_map[var]
                    pmap, omap = predicate_object_map[p]
                    if omap.objectt.resource_type == TripleMapType.TEMPLATE:
                        coltotemplates[var[1:]] = omap.objectt.value
                        splits = omap.objectt.value.split('{')
                        column = []
                        for sp in splits[1:]:
                            column.append(sp[:sp.find('}')])
                        if len(column) == 1:
                            column = column[0]
                    elif omap.objectt.resource_type == TripleMapType.REFERENCE:
                        column = omap.objectt.value
                    else:
                        column = []
                    if isinstance(column, list):
                        j = 0
                        for col in column:
                            if '[*]' in col:
                                col = col.replace('[*]', '')
                                vcolumn = "`" + col + '`'
                                projections[var[1:] + '_Ontario_' + str(j)] = "FLATTEN(" + tablealias + "." + vcolumn + ") AS " + var[1:] + '_Ontario_' + str(j)
                            else:
                                vcolumn = "`" + col + '`'
                                projections[var[1:] + '_Ontario_' + str(j)] = tablealias + "." + vcolumn + " AS `" + var[1:] + '_Ontario_' + str(j) + '`'

                            projvartocol.setdefault(var[1:], []).append(col)
                            objectfilters.append(tablealias + '.' + vcolumn + " is not null ")
                            objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                            j += 1
                    else:
                        col = column
                        if '[*]' in col:
                            col = col.replace('[*]', '')
                            column = "`" + col + '`'
                            projections[var[1:]] = "FLATTEN(" + tablealias + "." + column + ") AS `" + var[1:] + '`'
                        else:
                            column = "`" + column + '`'
                            projections[var[1:]] = tablealias + "." + column + " AS `" + var[1:] + '`'
                        projvartocol[var[1:]] = col
                        objectfilters.append(tablealias + '.' + column + " is not null ")
                        objectfilters.append(tablealias + '.' + column + " <> '' ")
            for f in query_filters:
                #if len(set(f.getVars()).intersection(list(var_pred_map.keys()))) == len(set(f.getVars())):
                fil = self.get_obj_filter(f, var_pred_map, self.mappings[tm].subject_map.subject, predicate_object_map, coltotemplates, tablealias)
                if fil is not None and len(fil) > 0:
                    constfilters.append(fil)
            tm_tablealias[tablealias] = tm

            triplemap = self.mappings[tm]
            subjects[tm] = triplemap.subject_map.subject

            logicalsource = triplemap.logical_source
            data_source = logicalsource.data_source
            # tablename = data_source.name
            # database_name = logicalsource.iterator  #TODO: this is not correct, only works for LSLOD-Custom experiment
            database_name = data_source.name
            if '/' in database_name:
                database_name = database_name.split('/')[-1]
            tablename = data_source.name
            # TODO: change the paths, this works only for LSLOD-experiment
            if self.datasource.dstype == DataSourceType.LOCAL_TSV:
                # fileext = 'dfs.`/data/tsv/' + database_name + '/' + tablename + '.tsv`'
                fileext = 'dfs.`/data/tsv/' + tablename + '`'
            elif self.datasource.dstype == DataSourceType.LOCAL_CSV:
                # fileext = 'dfs.`/data/csv/' + database_name + '/' + tablename + '.csv`'
                fileext = 'dfs.`/data/csv/' + tablename + '`'
            elif self.datasource.dstype == DataSourceType.LOCAL_JSON:
                # fileext = 'dfs.`/data/json/' + database_name + '/' + tablename + '.json`'
                fileext = 'dfs.`/data/json/' + tablename + '`'
            elif self.datasource.dstype == DataSourceType.HADOOP_TSV:
                # fileext = 'hdfs.`/user/kemele/data/tsv/' + database_name + '/' + tablename + '.tsv`'
                fileext = 'hdfs.`/user/kemele/data/tsv/' + tablename + '`'
            elif self.datasource.dstype == DataSourceType.HADOOP_CSV:
                # fileext = 'hdfs.`/user/kemele/data/csv/' + database_name + '/' + tablename + '.csv`'
                fileext = 'hdfs.`/user/kemele/data/csv/' + tablename + '`'
            elif self.datasource.dstype == DataSourceType.HADOOP_JSON:
                # fileext = 'hdfs.`/user/kemele/data/json/' + database_name + '/' + tablename + '.json`'
                fileext = 'hdfs.`/user/kemele/data/json/' + tablename + '`'
            else:
                fileext = ''

            fromclauses.append(fileext + ' ' + tablealias)
            i += 1

            for var, p in var_pred_map.items():

                if '?' not in var:
                    pmap, omap = predicate_object_map[p]

                    if omap.objectt.resource_type == TripleMapType.TEMPLATE:
                        # omap.objectt.value
                        splits = omap.objectt.value.split('{')
                        column = []
                        for sp in splits[1:]:
                            column.append(sp[:sp.find('}')])

                        var = var.replace(splits[0], '').replace('}', '')
                        if '<' in var and '>' in var:
                            var = var[1:-1]
                        var = "'" + var + "'"
                    elif omap.objectt.resource_type == TripleMapType.REFERENCE:
                        column = omap.objectt.value
                        if "'" not in var and '"' not in var:
                            var = "'" + var + "'"
                        if '"' in var:
                            var = "'" + var[1:-1] + "'"
                    else:
                        column = []
                    if isinstance(column, list):
                        j = 0
                        for col in column:
                            vcolumn = "`" + col + '`'
                            constfilters.append(tablealias + "." + vcolumn + " = " + var)
                            j += 1
                    else:
                        column = "`" + column + '`'
                        constfilters.append(tablealias + "." + column + " = " + var)

        subj = self.star['triples'][0].subject.name if not self.star['triples'][0].subject.constant else None
        invalidsubj = False
        if subj is not None:
            filtersadded = []
            for tm, subject in subjects.items():
                subjcol = subject.value
                tablealias = [v for v in tm_tablealias if tm_tablealias[v] == tm][0]
                splits = subjcol.split('{')
                coltotemplates[subj[1:]] = subjcol
                column = []
                for sp in splits[1:]:
                    column.append(sp[:sp.find('}')])

                if len(column) > 1:
                    j = 0
                    for col in column:
                        vcolumn = "`" + col + '`'
                        projections[subj[1:] + '_Ontario_' + str(j)] = tablealias + "." + vcolumn + " AS `" + subj[1:] + '_Ontario_' + str(j) + '`'
                        projvartocol.setdefault(subj[1:], []).append(col)
                        objectfilters.append(tablealias + '.' + vcolumn + " is not null ")
                        objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                        j += 1
                elif len(column) == 1:
                    col = column[0]
                    column = "`" + col + '`'
                    projections[subj[1:]] = tablealias + "." + column + " AS `" + subj[1:] + '`'
                    projvartocol[subj[1:]] = col

                    objectfilters.append(tablealias + '.' + column + " is not null ")
                    objectfilters.append(tablealias + '.' + column + " <> '' ")
        else:
            subj = self.star['triples'][0].subject.name
            for tm, subject in subjects.items():
                subjcol = subject.value
                tablealias = [v for v in tm_tablealias if tm_tablealias[v] == tm][0]
                splits = subjcol.split('{')
                column = []
                for sp in splits[1:]:
                    column.append(sp[:sp.find('}')])

                if len(splits[0]) > 0 and splits[0] not in subj:
                    invalidsubj = True
                    break
                var = subj.replace(splits[0], '').replace('}', '')

                if '<' in var and '>' in var:
                    var = var[1:-1]
                var = "'" + var + "'"
                # if isinstance(column, list):
                j = 0
                for col in column:
                    vcolumn = "`" + col + '`'
                    constfilters.append(tablealias + "." + vcolumn + " = " + var)
                    j += 1
        if invalidsubj:
            mapping_preds = []

        if len(subjects) > 1:
            aliases = list(tm_tablealias.keys())
            raliases = aliases.copy()
            raliases.reverse()
            compared = []
            for a1 in aliases:
                for a2 in aliases:
                    if a1 + a2 in compared:
                        continue
                    if a1 == a2:
                        continue
                    compared.append(a1 + a2)
                    compared.append(a2 + a1)
                    subj1 = subjects[tm_tablealias[a1]].value
                    subj2 = subjects[tm_tablealias[a2]].value
                    column1 = None
                    column2 = None
                    splits = subj1.split('{')
                    for sp in splits:
                        if '}' in sp:
                            column1 = sp[:sp.find('}')]
                            break
                    splits = subj2.split('{')
                    for sp in splits:
                        if '}' in sp:
                            column2 = sp[:sp.find('}')]
                            break
                    column1 = '`' + column1 + '`'
                    column2 = '`' + column2 + '`'
                    if column1 == column2:
                        objectfilters.append(a1 + '.' + column1 + "=" + a2 + "." + column2)
        objectfilters.extend(constfilters)
        if len(mapping_preds) > 0:
            fromcaluse = "\n FROM " + ", ".join(list(set(fromclauses)))
            distinct = ""
            if self.query.distinct:
                distinct = "DISTINCT "
            projections = " SELECT  " + distinct + ", ".join(list(set(projections.values())))
            if len(objectfilters) > 0:
                whereclause = "\n WHERE " + "\n\t AND ".join(list(set(objectfilters)))
            else:
                whereclause = ""

            sqlquery = projections + " " + fromcaluse + " " + whereclause
            return sqlquery, projvartocol, coltotemplates, database_name

        return query, projvartocols, coltotemplates, database_name

    def makeunion(self, tounions, query_filters, subjectunions=False):

        coltotemplates = {}
        projvartocols = {}
        database_name = ""
        unions = []
        rdfmts = list(tounions.keys())
        rdfmts = list(reversed(sorted(rdfmts)))
        # print(rdfmts)
        for rdfmt in rdfmts:
            mappingpreds = tounions[rdfmt]
            if subjectunions:
                for tm, submaps in mappingpreds.items():
                    un, projvartocols, coltotemplates, database_name = self.makeJoin({tm: submaps}, query_filters)
                    if un is not None and len(un) > 0:
                        unions.append(un)
            else:
                un, projvartocols, coltotemplates, database_name = self.makeJoin(mappingpreds, query_filters)
                if un is not None and len(un) > 0:
                    unions.append(un)

        #query = " UNION ".join(unions)
        # print(query)
        return unions, projvartocols, coltotemplates, database_name

    def translate(self, query_filters):
        rdfmts = self.star['rdfmts']
        starpreds = list(self.star['predicates'].keys())
        star_preds = [p for p in starpreds if '?' not in p]
        if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in star_preds:
            star_preds.remove('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')

        touninon = {}
        completematch = {}

        if len(star_preds) == 0:
            subjectonly = False
            for tm, triplemap in self.mappings.items():
                for rdfmt in triplemap.subject_map.rdf_types:
                    if rdfmt in rdfmts:
                        if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in starpreds:
                            touninon.setdefault(rdfmt, {})[tm] = triplemap.subject_map
                            subjectonly = True
                        else:
                            touninon.setdefault(rdfmt, {})[tm] = triplemap.predicate_object_map
            if len(touninon) > 1 or subjectonly:
                return self.makeunion(touninon, query_filters, subjectonly)
            elif len(touninon) == 1:
                query, projvartocols, coltotemplates, database_name = self.makeJoin(touninon[list(touninon.keys())[0]],
                                                                                    query_filters)
                return query, projvartocols, coltotemplates, database_name
            else:
                return None, None, None, None
        else:
            mapping_preds = {tm: triplemap for tm, triplemap in self.mappings.items() for p in star_preds if p in triplemap.predicate_object_map}
            for tm, triplemap in mapping_preds.items():
                for rdfmt in triplemap.subject_map.rdf_types:
                    if rdfmt in rdfmts and len(set(star_preds).intersection(list(triplemap.predicate_object_map.keys()))) == len(set(star_preds)):
                        completematch[rdfmt] = {}
                        completematch[rdfmt][tm] = triplemap.predicate_object_map
                    if rdfmt in rdfmts and len(set(star_preds).intersection(list(triplemap.predicate_object_map.keys()))) > 0:
                        touninon.setdefault(rdfmt, {})[tm] = triplemap.predicate_object_map
            if len(completematch) > 0:
                if len(completematch) == 1:
                    query, projvartocols, coltotemplates, database_name = self.makeJoin(
                        touninon[list(touninon.keys())[0]], query_filters)
                    return query, projvartocols, coltotemplates, database_name
                else:
                    return self.makeunion(completematch, query_filters)
            elif len(touninon) > 1:
                return self.makeunion(touninon, query_filters)
            elif len(touninon) == 1:
                query, projvartocols, coltotemplates, database_name = self.makeJoin(touninon[list(touninon.keys())[0]],
                                                                                    query_filters)
                return query, projvartocols, coltotemplates, database_name
            else:
                return None, None, None, None
