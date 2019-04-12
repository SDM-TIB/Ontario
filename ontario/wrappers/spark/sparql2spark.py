from ontario.sparql.parser import queryParser as qp
from multiprocessing import Process, Queue
from ontario.wrappers.mysql.utils import *
from ontario.sparql.parser.services import Filter, Expression, Argument, unaryFunctor, binaryFunctor
from ontario.model.rml_model import TripleMapType
from pyspark.sql import SparkSession
import json
import hashlib
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('ontario.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class SPARKWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.spark = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        if ':' in self.url:
            self.host, self.port = self.url.split(':')
        else:
            self.host = self.url
            self.port = '7077'

        if len(self.datasource.mappings) == 0:
            self.mappings = self.config.load_mappings(self.datasource.mappingfiles, []) # self.rdfmts
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
        # logger.info(sqlquery)
        if self.spark is None:
            # url = 'spark://node3.research.tib.eu:7077' # self.mapping['url']
            params = {
                "spark.driver.cores": "24",
                "spark.executor.cores": "24",
                "spark.cores.max": "48",
                "spark.default.parallelism": "24",
                "spark.executor.memory": "64g",
                "spark.driver.memory": "32g",
                "spark.driver.maxResultSize": "32g",
                "spark.python.worker.memory": "32g",
                "spark.local.dir": "/tmp",
                "spark.debug.maxToStringFields": "50"
            }

            start = time()
            # self.config['params']
            self.spark = SparkSession.builder.master('local[*]') \
                .appName("OntarioSparkWrapper" + str(self.datasource.url) + query)
            for p in params:
                self.spark = self.spark.config(p, params[p])

            self.spark = self.spark.getOrCreate()
            logger.info("SPARK Initialization cost:" + str(time()-start))
        start = time()

        for filename, tablename in filenametablename.items():
            # schema = self.make_schema(schemadict[filename])
            # filename = "hdfs://node3.research.tib.eu:9000" + filename
            # filename = self.datasource.url + "/" + filename
            # filename = "/media/kemele/DataHD/LSLOD-flatfiles/" + filename
            # print(filename)
            if self.datasource.dstype == DataSourceType.LOCAL_JSON or \
                    self.datasource.dstype == DataSourceType.SPARK_JSON:
                df = self.spark.read.json(filename)
            elif self.datasource.dstype == DataSourceType.HADOOP_JSON:
                filename = "hdfs://node3.research.tib.eu:9000" + filename
                df = self.spark.read.json(filename)
            elif self.datasource.dstype == DataSourceType.HADOOP_TSV or \
                    self.datasource.dstype == DataSourceType.HADOOP_CSV:
                filename = "hdfs://node3.research.tib.eu:9000" + filename
                df = self.spark.read.csv(filename, inferSchema=True, sep='\t'
                                         if self.datasource.dstype == DataSourceType.HADOOP_TSV else ',',
                                         header=True)
            else:
                df = self.spark.read.csv(filename, inferSchema=True,
                                         sep='\t' if self.datasource.dstype == DataSourceType.LOCAL_TSV or \
                                                     self.datasource.dstype == DataSourceType.SPARK_TSV else ',',
                                         header=True)

            df.createOrReplaceTempView(tablename)
        logger.info("time for reading file" + str(filenametablename) + str(time() - start))

        totalres = 0
        try:

            runstart = time()
            if isinstance(sqlquery, list) and len(sqlquery) > 3:
                sqlquery = " UNION ".join(sqlquery)
            # print(sqlquery)
            if isinstance(sqlquery, list):
                logger.info(" UNION ".join(sqlquery))
                res_dict = []
                for sql in sqlquery:
                    cardinality = self.process_result(sql, queue, projvartocols, coltotemplates, res_dict)
                    totalres += cardinality
            else:
                logger.info(sqlquery)
                cardinality = self.process_result(sqlquery, queue, projvartocols, coltotemplates)
                totalres += cardinality
            logger.info("Exec in SPARK took:" + str(time() - runstart))
        except Exception as e:
            print("Exception ", e)
            pass
        # print('SPARK End:', time(), "Total results:", totalres)
        # print("SPARK finished after: ", (time()-start))
        queue.put("EOF")
        self.spark.stop()

    def process_result(self, sql, queue, projvartocols, coltotemplates, res_dict=None):
        c = 0
        result = self.spark.sql(sql).toJSON()
        # print('count-----',result.count())
        for row in result.collect():
            c += 1
            row = json.loads(row)
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
                if not isinstance(row[r], str):
                    row[r] = str(row[r])
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
            if not t.theobject.constant: # and t.theobject.name in proj:
                tvars.append(t.theobject.name)

        return tvars

    def getsqlfil(self, l, r, op, var_pred_map, subjmap,predicate_object_map, coltotemplates, tablealias):
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
                leftexp = self.get_Expression_value(left, var_pred_map, subjmap,predicate_object_map, coltotemplates, tablealias)
                rightexp = self.get_Expression_value(right, var_pred_map, subjmap,predicate_object_map, coltotemplates, tablealias)
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
        return self.get_Expression_value(f.expr, var_pred_map, subjmap, predicate_object_map, coltotemplates,tablealias)

    def makeJoin(self, mapping_preds, query_filters):

        coltotemplates = {}
        projections = {}
        projvartocol = {}
        objectfilters = []
        constfilters = []
        fromclauses = []

        database_names = {}
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
                        # objectfilters.append(tablealias + '.' + vcolumn + " <> '' ")
                        j += 1
                else:
                    col = column
                    column = "`" + column + '`'
                    projections[var[1:]] = tablealias + "." + column + " AS `" + var[1:] + '`'
                    projvartocol[var[1:]] = col
                    objectfilters.append(tablealias + '.' + column + " is not null ")
                    # objectfilters.append(tablealias + '.' + column + " <> '' ")
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
            if self.datasource.dstype == DataSourceType.LOCAL_TSV or self.datasource.dstype == DataSourceType.SPARK_TSV:
                # fileext = self.datasource.url + '/' + database_name + '/' + tablename + '.tsv'
                #fileext = '/data/tsv/' + database_name + '/' + tablename + '.tsv'
                fileext = '/data/tsv/' + tablename
            elif self.datasource.dstype == DataSourceType.LOCAL_CSV or self.datasource.dstype == DataSourceType.SPARK_CSV:
                # fileext = self.datasource.url + '/' + database_name + '/' + tablename + '.csv'
                # fileext = '/data/csv/' + database_name + '/' + tablename + '.csv'
                fileext = '/data/csv/' + tablename
            elif self.datasource.dstype == DataSourceType.LOCAL_JSON or self.datasource.dstype == DataSourceType.SPARK_JSON:
                # fileext = self.datasource.url + '/' + database_name + '/' + tablename + '.json'
                #fileext = '/data/json/' + database_name + '/' + tablename + '.json'
                fileext = '/data/json/' + tablename
                #fileext = '/media/kemele/DataHD/LSLOD-flatfile/json/' + tablename
            else:
                fileext = ''

            database_names[fileext] = str(hashlib.md5(str(tablename).encode()).hexdigest())

            fromclauses.append(database_names[fileext] + ' ' + tablealias)
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
            return sqlquery, projvartocol, coltotemplates, database_names

        return query, projvartocols, coltotemplates, database_names

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
                    un, projvartocols, coltotemplates, database_name = self.makeJoin({tm:submaps}, query_filters)
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
