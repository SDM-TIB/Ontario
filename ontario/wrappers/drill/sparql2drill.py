from ontario.sparql.parser import queryParser as qp
from mysql import connector
from mysql.connector import errorcode
from multiprocessing import Process, Queue
from ontario.wrappers.mysql.utils import *
from ontario.sparql.parser.services import Filter, Expression, Argument
from ontario.model.rml_model import TripleMapType
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
            print("Drill Initialization cost:", time() - start)
            start = time()
            if isinstance(sqlquery, list) and len(sqlquery) > 3:
                sqlquery = " UNION ".join(sqlquery)
            if isinstance(sqlquery, list):
                logger.info(" UNION ".join(sqlquery))
                processqueues = []
                processes = []
                res_dict = []
                for sql in sqlquery:
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
            else:
                card = 0
                # if limit == -1:
                limit = 1000
                if offset == -1:
                    offset = 0
                logger.info(sqlquery)
                while True:
                    query_copy = sqlquery + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                    cardinality = self.process_result(query_copy, queue, projvartocols, coltotemplates)
                    card += cardinality
                    if cardinality < limit:
                        break

                    offset = offset + limit
            print("Exec in Drill took:", time() - start)
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

        results = self.drill.query(sql)
        c = 0
        for row in results:
            c += 1
            if res_dict is not None:
                rowtxt = ",".join(list(row.values()))
                if rowtxt in res_dict:
                    continue
                else:
                    res_dict.append(rowtxt)

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

    def get_so_variables(self, triples, proj):
        tvars = []
        for t in triples:
            if not t.subject.constant:
                tvars.append(t.subject.name)
            # exclude variables that are not projected
            if not t.theobject.constant and t.theobject.name in proj:
                tvars.append(t.theobject.name)

        return tvars

    def get_obj_filter(self, f, var_pred_map, predicate_object_map, coltotemplates, tablealias):
        l = f.expr.left
        r = f.expr.right
        op = f.expr.op
        if op.upper() == 'REGEX' and isinstance(l, Expression):
            l = l.left

        if r is not None and '?' in r.name:
            var = r.name
            val = l.name
        else:
            var = l.name
            val = r.name

        if '(' in var and ')' in var:
            var = var[var.find('(') + 1:var.find(')')]

        p = var_pred_map[var]
        pmap, omap = predicate_object_map[p]
        if omap.objectt.resource_type == TripleMapType.TEMPLATE:
            coltotemplates[l[1:]] = omap.objectt.value
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
            val = "'%" + val[1:-1] + "%'"
            objectfilter = tablealias + '.' + column + " LIKE " + val
        else:
            objectfilter = tablealias + '.' + column + op + val

        return objectfilter

    def makeJoin(self, mapping_preds, query_filters):

        coltotemplates = {}
        projections = {}
        projvartocol = {}
        objectfilters = []
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
            var_pred_map = {var: pred for pred, var in self.star['predicates'].items() if pred in predicate_object_map}
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
                        vcolumn = "`" + col + '`'
                        projections[var[1:] + '_Ontario_' + str(j)] = tablealias + "." + vcolumn + " AS `" + var[1:] + '_Ontario_' + str(j) + '`'
                        projvartocol.setdefault(var[1:], []).append(col)
                        objectfilters.append(tablealias + '.' + vcolumn + " is not null ")
                        objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                        j += 1
                else:
                    col = column
                    column = "`" + column + '`'
                    projections[var[1:]] = tablealias + "." + column + " AS `" + var[1:] + '`'
                    projvartocol[var[1:]] = col
                    objectfilters.append(tablealias + '.' + column + " is not null ")
                    objectfilters.append(tablealias + '.' + column + " <> '' ")
            for f in query_filters:
                if len(set(f.getVars()).intersection(list(var_pred_map.keys()))) == len(set(f.getVars())):
                    fil = self.get_obj_filter(f, var_pred_map, predicate_object_map, coltotemplates, tablealias)
                    objectfilters.append(fil)
            tm_tablealias[tablealias] = tm

            triplemap = self.mappings[tm]
            subjects[tm] = triplemap.subject_map.subject

            logicalsource = triplemap.logical_source
            data_source = logicalsource.data_source
            tablename = data_source.name
            database_name = logicalsource.iterator  #TODO: this is not correct, only works for LSLOD-Custom experiment
            if self.datasource.dstype == DataSourceType.LOCAL_TSV:
                fileext = 'dfs.`/data/tsv/' + database_name + '/' + tablename +  '.tsv`'
            elif self.datasource.dstype == DataSourceType.LOCAL_CSV:
                fileext = 'dfs.`/data/csv/' + database_name + '/' + tablename + '.csv`'
            elif self.datasource.dstype == DataSourceType.LOCAL_JSON:
                fileext = 'dfs.`/data/json/' + database_name + '/' + tablename + '.json`'
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
                        var = "'" + var +  "'"
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
                            objectfilters.append(tablealias + "." + vcolumn + " = " + var)
                            j += 1
                    else:
                        column = "`" + column + '`'
                        objectfilters.append(tablealias + "." + column + " = " + var)

        subj = self.star['triples'][0].subject.name if not self.star['triples'][0].subject.constant else None
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
                        projections[subj[1:] + '_Ontario_' + str(j)] = tablealias + "." + vcolumn + " AS " + subj[ 1:] + '_Ontario_' + str(j)
                        projvartocol.setdefault(subj[1:], []).append(col)
                        objectfilters.append(tablealias + '.' + vcolumn + " is not null ")
                        objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                        j += 1
                elif len(column) == 1:
                    col = column[0]
                    column = "`" + col + '`'
                    projections[subj[1:]] = tablealias + "." + column + " AS " + subj[1:]
                    projvartocol[subj[1:]] = col

                    objectfilters.append(tablealias + '.' + column + " is not null ")
                    objectfilters.append(tablealias + '.' + column + " <> '' ")

                # if len(tm_tablealias) > 1:
                #     for tm1, t1 in tm_tablealias.items():
                #         if tm == tm1:
                #             continue
                #         if t1 + tablealias not in filtersadded and tablealias + t1 not in filtersadded:
                #             objectfilters.append(t1 + '.' + column + ' = ' + tablealias + '.' + column)
                #             filtersadded.append(t1 + tablealias)
                #             filtersadded.append(tablealias + t1)
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

        if len(mapping_preds) > 0:
            fromcaluse = "\n FROM " + ", ".join(list(set(fromclauses)))
            projections = " SELECT " + ", ".join(list(set(projections.values())))
            if len(objectfilters) > 0:
                whereclause = "\n WHERE " + "\n\t AND ".join(list(set(objectfilters)))
            else:
                whereclause = ""

            sqlquery = projections + " " + fromcaluse + " " + whereclause
            return sqlquery, projvartocol, coltotemplates, database_name

        return query, projvartocols, coltotemplates, database_name

    def makeunion(self, tounions, query_filters):

        coltotemplates = {}
        projvartocols = {}
        database_name = ""
        unions = []

        for rdfmt, tm in tounions.items():
            mappingpreds = tounions[rdfmt]
            un, projvartocols, coltotemplates, database_name = self.makeJoin(mappingpreds, query_filters)
            unions.append(un)

        #query = " UNION ".join(unions)
        # print(query)
        return unions, projvartocols, coltotemplates, database_name

    def translate(self, query_filters):
        rdfmts = self.star['rdfmts']
        star_preds = list(self.star['predicates'].keys())
        if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in star_preds:
            star_preds.remove('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
        mapping_preds = {tm: triplemap for tm, triplemap in self.mappings.items() for p in star_preds if p in triplemap.predicate_object_map }
        # if len(set(star_preds).intersection(['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'] + list(triplemap.predicate_object_map.keys()))) == len(set(star_preds))
        touninon = {}
        completematch = {}
        for tm, triplemap in mapping_preds.items():
            for rdfmt in triplemap.subject_map.rdf_types:
                if rdfmt in rdfmts and len(set(star_preds).intersection(list(triplemap.predicate_object_map.keys()))) ==  len(set(star_preds)):
                    completematch[rdfmt] = {}
                    completematch[rdfmt][tm] = triplemap.predicate_object_map
                if rdfmt in rdfmts and \
                        len(set(star_preds).intersection(list(triplemap.predicate_object_map.keys()))) > 0:
                    touninon.setdefault(rdfmt, {})[tm] = triplemap.predicate_object_map
        if len(completematch) > 0:
            if len(completematch) ==1:
                query, projvartocols, coltotemplates, database_name = self.makeJoin(touninon[list(touninon.keys())[0]], query_filters)
                return query, projvartocols, coltotemplates, database_name
            else:
                return self.makeunion(completematch, query_filters)
        elif len(touninon) > 1:
            return self.makeunion(touninon, query_filters)
        elif len(touninon) == 1:
            query, projvartocols, coltotemplates, database_name = self.makeJoin(touninon[list(touninon.keys())[0]], query_filters)
            return query, projvartocols, coltotemplates, database_name
        else:
            return None, None, None, None
