import hashlib
import json
from pyspark.sql import SparkSession
from ontario.sparql.parser import queryParser as qp
from hdfs import Config
from multiprocessing import Process, Queue
from ontario.config.model import DataSourceType
from ontario.wrappers.spark.utils import *
from pyspark.sql.types import *
from ontario.wrappers.hadoop import SparkHDFSClient
from pprint import pprint


class SPARKXMLWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):

        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.spark = None
        self.df = None
        self.result = None
        self.datasource = datasource
        self.star = star
        self.query = None
        self.prefixes = {}

        self.mappings = {tm: self.datasource.mappings[tm] for tm in self.datasource.mappings \
                         for rdfmt in self.rdfmts if rdfmt in self.datasource.mappings[tm]}

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):
        """
        Entry point for query execution on csv files
        :param querystr: string query
        :return:
        """
        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []
        querytxt = query
        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)
        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset
        ds = self.star['datasource']
        from time import time
        start = time()
        if self.spark is None:
            url = 'spark://node3.research.tib.eu:7077'  # self.mapping['url']
            params = {
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                "spark.cores.max": "6",
                "spark.default.parallelism": "4",
                "spark.executor.memory": "6g",
                "spark.driver.memory": "12g",
                "spark.driver.maxResultSize": "8g",
                "spark.python.worker.memory": "10g",
                "spark.local.dir": "/tmp" # /data/kemele/tmp
            }
            # self.config['params']
            self.spark = SparkSession.builder.master(url) \
                .appName("OntarioSparkCSVTSVWrapper" + str(self.datasource) + querytxt)
            for p in params:
                self.spark = self.spark.config(p, params[p])

            self.spark = self.spark.getOrCreate()
        print("Spark XML init time:", time()-start)
        sqlquery, coltotemplates, projvartocols, filenametablenamemap, schemas = self.translate()
        print(sqlquery)
        # print(coltotemplates, projvartocols, tablename)
        print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_")
        start = time()

        for filename, tablename in filenametablenamemap.items():
            schema = self.make_schema(schemas[filename])
            rowTag = filename[filename.rfind('/') + 1:]
            filename = "hdfs://node3.research.tib.eu:9000" + filename[:filename.find('$/')]
            df = self.spark.read.format("com.databricks.spark.xml").option("rootTag", "*").option("rowTag",
                                                                                                  rowTag).load(filename, schema=schema )
            df.createOrReplaceTempView(tablename)
        print("Spark XML reading file time:", time() - start)
        if len(sqlquery) == 0:
            queue.put("EOF")
            return
        # sqlquery = """{0}""".format(sqlquery)
        # print(sqlquery)
        result = self.spark.sql(sqlquery).toJSON()
        i = 0
        for row in result.toLocalIterator():
            row = json.loads(row)
            for r in row:
                if r in projvartocols and r in coltotemplates:
                    plh = coltotemplates[r][coltotemplates[r].find('{')+1: coltotemplates[r].find('}')]
                    row[r] = coltotemplates[r].replace('{' + plh + '}', row[r].replace(" ", '_'))
            queue.put(row)
            i += 1
        self.spark.stop()
        queue.put("EOF")

    def _get_str_field(self, f, strvalue):

        if isinstance(strvalue, str):
            return StructField(strvalue, StringType())
        if isinstance(strvalue, list):
            return StructField(f, ArrayType(StructType([
                self._get_str_field(f, v) for v in strvalue
                ])))
        if isinstance(strvalue, dict):
            return StructField(f, StructType([self._get_str_field(v, strvalue[v]) for v in strvalue]) )

    def make_schema(self, schemadict):

        schema = []
        for f in schemadict:
            if isinstance(schemadict[f], str):
                schema.append(self._get_str_field(f, f))
            else:
                v = schemadict[f]
                schema.append(self._get_str_field(f, v[f]))
        print(schema)
        return StructType(schema)

    def translate(self):
        var2maps = {}
        sqlquery = ""
        coltotemplates = {}
        projvartocols = {}
        print("Spark translate ------------------------------------")
        # pprint(self.star)
        if 'startriples' in self.star:
            for s in self.star['startriples']:
                star = self.star['startriples'][s]
                rdfmts = star['rdfmts']
                predicates = star['predicates']
                triples = star['triples']

                for m in rdfmts:
                    res1 = var2map(self.mappings, m, predicates, triples, self.prefixes)
                    var2maps[m] = res1
        else:
            rdfmts = self.star['rdfmts']
            predicates = self.star['predicates']
            triples = self.star['triples']
            for m in rdfmts:
                res2 = var2map(self.mappings, m, predicates, triples, self.prefixes)
                var2maps[m] = res2

        # pprint(var2maps)
        qcols = self.get_varmaps(var2maps)
        # pprint(qcols)
        if len(qcols) == 0:
            print("Cannot get mappings for this subquery", self.query)
            return []
        sqlquery, projvartocols, coltotemplates, filenametablenamemap, schema = self.translate_4_msql_df(qcols)

        print(coltotemplates)
        print(projvartocols)
        pprint(schema)
        return sqlquery, coltotemplates, projvartocols, filenametablenamemap, schema

    def translate_4_msql_df(self, qcols):
        ifilters = []
        sparqlprojected = [c.name for c in self.query.args]
        fromclauses = []
        projections = {}
        subjectfilters = []
        objectfilters = []
        projvartocols = {}
        lateralviews = {}
        i = 0
        subjtablemap = {}
        coltotemplates = {}
        filenametablenamemap = {}
        schemas = {}
        for filename, varmaps in qcols.items():
            triples = varmaps['triples']
            tablename = str(hashlib.md5(str(filename).encode()).hexdigest())
            variablemap = varmaps['varmap']
            coltotemplates.update(varmaps['coltotemp'])
            filenametablenamemap[filename] = tablename
            tablealias = 'TandemT' + str(i)
            fromcaluse = getLVFROMClause(tablename, tablealias)
            fromclauses.append(fromcaluse)
            subjtablemap[tablealias] = varmaps['subjcol']
            # fprojections, projvartocol = getProjectionClause(variablemap, sparqlprojected, tablealias)
            fprojections, wherenotnull, projvartocol, flateralviews = getLVProjectionClause(variablemap, sparqlprojected,
                                                                                          tablealias)
            if wherenotnull is not None:
                objectfilters.extend(wherenotnull)
            if fprojections is not None:
                projections.update(fprojections)
            if projvartocols is not None:
                projvartocols.update(projvartocol)
            if flateralviews is not None:
                lateralviews.update(flateralviews)
            '''
              Case I: If subject is constant
            '''
            subjectfilter = getSubjectFilters(ifilters, tablealias)
            if subjectfilter is not None:
                subjectfilters.extend(subjectfilter)
            '''
              Case II: If predicate + object are constants
            '''
            # objectfilter = getObjectFilters(self.mappings, self.prefixes, triples, varmaps, tablealias, sparqlprojected, self.query)
            objectfilter, projvartocolobj, lateralviewsobj, schema = getLVObjectFilters(self.prefixes, triples,
                                                                                              varmaps, tablealias,
                                                                                              sparqlprojected)
            if projvartocolobj is not None:
                projvartocols.update(projvartocolobj)
            if objectfilter is not None:
                objectfilters.extend(objectfilter)
            if lateralviewsobj is not None:
                lateralviews.update(lateralviewsobj)
            schemas[filename] = schema
            i += 1

        if len(subjtablemap) > 1:
            aliases = list(subjtablemap.keys())
            raliases = aliases.copy()
            raliases.reverse()
            compared = []
            for a1, a2 in zip(aliases, raliases):
                if a1+a2 in compared:
                    continue
                compared.append(a1+a2)
                compared.append(a2+a1)
                subj1 = subjtablemap[a1][0].strip()
                subj2 = subjtablemap[a2][0].strip()
                column1 = subj1
                column2 = subj2
                if '[' in subj1:
                    column1 = '`' + subj1[:subj1.find('[')] + "`._VALUE"
                elif '/' in subj1 and '[*]' not in subj1:
                    column1 = subj1.replace('/', '.')
                    column1 = "`" + column1[:column1.find('.')] + '`' + column1[column1.find('.'):]
                else:
                    column1 = '`' + column1 + '`'
                if '[' in subj2:
                    column2 = '`' + subj2[:subj2.find('[')] + "`._VALUE"
                elif '/' in subj2 and '[*]' not in subj2:
                    column2 = subj2.replace('/', '.')
                    column2 = "`" + column2[:column2.find('.')] + '`' + column2[column2.find('.'):]
                else:
                    column2 = '`' + column2 + '`'

                objectfilters.append(a1 + '.' + column1 + "=" + a2 + "." + column2)

        # lateralviews = list(set(lateralviews))
        lateralviewslst = []
        for v, ex in lateralviews.items():
            lateralviewslst.append(ex + " AS " + v)
        fromcaluse = " FROM " + ", ".join(fromclauses)
        projections = " SELECT " + ", ".join(list(set(projections.values())))
        subjfilter = " AND ".join(subjectfilters) if len(subjectfilters) > 0 else None
        whereclause = " WHERE " + " AND ".join(list(set(objectfilters)))
        if subjfilter is not None:
            if len(objectfilters) > 0:
                whereclause += " AND " + subjfilter
            else:
                whereclause += subjfilter
        lateral = ""
        if len(lateralviewslst) > 0:
            for l in lateralviewslst:
                lateral += " LATERAL VIEW " + l + " "
        sqlquery = projections + " " + fromcaluse + lateral + " " + whereclause

        return sqlquery, projvartocols, coltotemplates, filenametablenamemap, schemas

    def translate_4_sql_df(self, varmaps, filename):
        ifilters = []
        triples = varmaps['triples']

        sparqlprojected = [c.name for c in self.query.args]
        tablename = str(hashlib.md5(str(filename).encode()).hexdigest())

        variablemap = varmaps['varmap']
        predobjdict, needselfjoin, maxnumofobj = getPredObjDict(triples, self.prefixes)
        fromcaluse = getFROMClause(tablename, maxnumofobj)
        projections, wherenotnull, projvartocol, lateralviews = getLVProjectionClause(variablemap, sparqlprojected, maxnumofobj)
        print("LVProjection: ", projections, wherenotnull, projvartocol, lateralviews)
        '''
          Case I: If subject is constant
        '''
        subjectfilters, firstfilter = getSubjectFilters(ifilters, maxnumofobj)
        '''
          Case II: If predicate + object are constants
          prefixes, triples, varmaps, tablealias, sparqlprojected
        '''
        objectfilters, firstfilter, projvartocolobj, lateralviewsobj = getLVObjectFilters(self.prefixes, triples, varmaps, tablename, sparqlprojected)
        print("LVObjFilters:", objectfilters, firstfilter, projvartocolobj, lateralviewsobj)
        objectfilters =" AND ".join( list(set(objectfilters + wherenotnull)))
        projvartocol.update(projvartocolobj)
        lateralviews =  list(set(lateralviews + lateralviewsobj))
        print('final: notnulls --', objectfilters)
        print('projs -- ', projvartocol)
        print("laterals - ", lateralviews)
        simplefilters = None
        if subjectfilters is not None or objectfilters or simplefilters:
            whereclause = " WHERE "
            if subjectfilters is not None:
                whereclause += subjectfilters
            if objectfilters is not None:
                whereclause += " " + objectfilters
            if simplefilters is not None:
                whereclause += " " + simplefilters
        else:
            whereclause = ""
        lateral = ""
        if len(lateralviews) > 0:
            for l in lateralviews:
                lateral += " LATERAL VIEW " + l + " "
        sqlquery = projections + " " + fromcaluse + lateral + " " + whereclause
        print(sqlquery)
        print("=======================")
        return sqlquery, projvartocol, tablename

    def get_varmaps(self, var2maps):
        qcols = {}
        for m in var2maps:
            for col in var2maps[m]:

                if col in qcols:
                    col2temp = var2maps[m][col]['coltotemp']
                    for c in col2temp:
                        qcols[col]['coltotemp'][c] = col2temp[c]
                    qcols[col]['subjcol'].extend(var2maps[m][col]['subjcol'])
                    varmap = var2maps[m][col]['varmap']
                    for v in varmap:
                        # TODO: this overwrites variables from previous star (try to prevent that)
                        # If the map reference is different then this could potentially return wrong results
                        qcols[col]['varmap'][v] = varmap[v]

                    qcols[col]['triples'].extend(var2maps[m][col]['triples'])
                    qcols[col]['triples'] = list(set(qcols[col]['triples']))

                    predmap = var2maps[m][col]['predmap']
                    for p in predmap:
                        qcols[col]['predmap'][p] = predmap[p]
                else:
                    qcols[col] = var2maps[m][col]
                    qcols[col]['subjcol'] = qcols[col]['subjcol']
        return qcols


class SPARKCSVTSVWrapper(object):

    def __init__(self, ds, config, rdfmts, star):
        self.rdfmts = rdfmts
        self.url = ds.url
        self.datasource = ds
        self.params = ds.params
        self.config = config
        self.spark = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        self.mappings = {tm: self.datasource.mappings[tm] for tm in self.datasource.mappings \
                         for rdfmt in self.rdfmts if rdfmt in self.datasource.mappings[tm]}

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):
        """
        Entry point for query execution on csv files
        :param querystr: string query
        :return:
        """
        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []

        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)
        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset

        sqlquery, coltotemplates, projvartocols, filenametablename, schemadict = self.translate()
        print(sqlquery)
        # print(coltotemplates, projvartocols, tablename)
        print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_")
        from time import time
        ds = self.star['datasource']
        if self.spark is None:
            url = 'spark://node3.research.tib.eu:7077' # self.mapping['url']
            params = {
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                "spark.cores.max": "6",
                "spark.default.parallelism": "4",
                "spark.executor.memory": "6g",
                "spark.driver.memory": "12g",
                "spark.driver.maxResultSize": "8g",
                "spark.python.worker.memory": "10g",
                "spark.local.dir": "/tmp"
            }

            start = time()
            # self.config['params']
            self.spark = SparkSession.builder.master(url) \
                .appName("OntarioSparkCSVTSVWrapper" + str(self.datasource) + query)
            for p in params:
                self.spark = self.spark.config(p, params[p])

            self.spark = self.spark.getOrCreate()
            print("Initialization cost:", time()-start)
        start = time()

        for filename, tablename in filenametablename.items():
            schema = self.make_schema(schemadict[filename])
            filename = "hdfs://node3.research.tib.eu:9000" + filename
            df = self.spark.read.csv(filename, sep='\t' if ds.dstype == DataSourceType.SPARK_TSV else ',', header=True)
            df.createOrReplaceTempView(tablename)
        print("time for reading file", time() - start)
        if len(sqlquery) == 0:
            queue.put("EOF")
            return
        # sqlquery = """{0}""".format(sqlquery)
        # print(sqlquery)
        result = self.spark.sql(sqlquery).toJSON()
        i = 0
        for row in result.toLocalIterator():
            row = json.loads(row)
            skip = False
            for r in row:
                if row[r] == 'null':
                    skip = True
                    break
                if r in projvartocols and r in coltotemplates:
                    row[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}',  row[r].replace(" ", '_'))
            if not skip:
                queue.put(row)
                i += 1

        self.spark.stop()
        queue.put("EOF")

    def _get_str_field(self, f, strvalue):

        if isinstance(strvalue, str):
            return StructField(strvalue, StringType())
        if isinstance(strvalue, list):
            return StructField(f, ArrayType(StructType([
                self._get_str_field(f, v) for v in strvalue
                ])))
        if isinstance(strvalue, dict):
            return StructField(f, StructType([self._get_str_field(v, strvalue[v]) for v in strvalue]) )

    def make_schema(self, schemadict):

        schema = []
        for f in schemadict:
            if isinstance(schemadict[f], str):
                schema.append(self._get_str_field(f, f))
            else:
                v = schemadict[f]
                schema.append(self._get_str_field(f, v[f]))
        print(schema)
        return StructType(schema)

    def translate(self):
        var2maps = {}
        print("Spark translate ------------------------------------")
        if 'startriples' in self.star:
            for s in self.star['startriples']:
                star = self.star['startriples'][s]
                rdfmts = star['rdfmts']
                predicates = star['predicates']
                triples = star['triples']

                for m in rdfmts:
                    res1 = var2map(self.mappings, m, predicates, triples, self.prefixes)
                    var2maps[m] = res1
        else:
            rdfmts = self.star['rdfmts']
            predicates = self.star['predicates']
            triples = self.star['triples']
            for m in rdfmts:
                res2 = var2map(self.mappings, m, predicates, triples, self.prefixes)
                var2maps[m] = res2

        qcols = self.get_varmaps(var2maps)
        if len(qcols) == 0:
            print("Cannot get mappings for this subquery", self.query)
            return []

        sqlquery, projvartocols, coltotemplates, filenametablenamemap, schema = self.translate_4_msql_df(qcols)

        print(coltotemplates)
        print(projvartocols)
        return sqlquery, coltotemplates, projvartocols, filenametablenamemap, schema

    def translate_4_msql_df(self, qcols):
        ifilters = []
        sparqlprojected = [c.name for c in self.query.args]
        fromclauses = []
        projections = {}
        subjectfilters = []
        objectfilters = []
        projvartocols = {}
        i = 0
        subjtablemap = {}
        coltotemplates = {}
        filenametablenamemap = {}

        schemas = {}
        for filename, varmaps in qcols.items():
            triples = varmaps['triples']
            tablename = str(hashlib.md5(str(filename).encode()).hexdigest())
            variablemap = varmaps['varmap']
            coltotemplates.update(varmaps['coltotemp'])
            filenametablenamemap[filename] = tablename
            tablealias = 'TandemT' + str(i)
            fromcaluse = getFROMClause(tablename, tablealias)
            fromclauses.append(fromcaluse)
            subjtablemap[tablealias] = varmaps['subjcol']
            fprojections, projvartocol = getProjectionClause(variablemap, sparqlprojected, tablealias)
            if fprojections is not None:
                projections.update(fprojections)
            if projvartocols is not None:
                projvartocols.update(projvartocol)
            '''
              Case I: If subject is constant
            '''
            subjectfilter = getSubjectFilters(ifilters, tablealias)
            if subjectfilter is not None:
                subjectfilters.extend(subjectfilter)
            '''
              Case II: If predicate + object are constants
            '''
            objectfilter, schema = getObjectFilters(self.mappings, self.prefixes, triples, varmaps, tablealias, sparqlprojected, self.query)
            if objectfilter is not None:
                objectfilters.extend(objectfilter)

            schemas[filename] = schema
            print(filename, schema)
            i += 1

        if len(subjtablemap) > 1:
            aliases = list(subjtablemap.keys())
            raliases = aliases.copy()
            raliases.reverse()
            compared = []
            for a1, a2 in zip(aliases, raliases):
                if a1+a2 in compared:
                    continue
                compared.append(a1+a2)
                compared.append(a2+a1)
                subj1 = subjtablemap[a1][0].strip()
                subj2 = subjtablemap[a2][0].strip()
                column1 = subj1
                column2 = subj2
                if '[' in subj1:
                    column1 = '`' + subj1[:subj1.find('[')] + "`._VALUE"
                elif '/' in subj1 and '[*]' not in subj1:
                    column1 = subj1.replace('/', '.')
                    column1 = "`" + column1[:column1.find('.')] + '`' + column1[column1.find('.'):]
                else:
                    column1 = '`' + column1 + '`'
                if '[' in subj2:
                    column2 = '`' + subj2[:subj2.find('[')] + "`._VALUE"
                elif '/' in subj2 and '[*]' not in subj2:
                    column2 = subj2.replace('/', '.')
                    column2 = "`" + column2[:column2.find('.')] + '`' + column2[column2.find('.'):]
                else:
                    column2 = '`' + column2 + '`'

                objectfilters.append(a1 + '.' + column1 + "=" + a2 + "." + column2)

        fromcaluse = " FROM " + ", ".join(fromclauses)
        projections = " SELECT " + ", ".join(list(projections.values()))
        subjfilter = " AND ".join(subjectfilters) if len(subjectfilters) > 0 else None
        whereclause = " WHERE " + " AND ".join(objectfilters)
        if subjfilter is not None:
            if len(objectfilters) > 0:
                whereclause += " AND " + subjfilter
            else:
                whereclause += subjfilter

        sqlquery = projections + " " + fromcaluse + " " + whereclause
        return sqlquery, projvartocols, coltotemplates, filenametablenamemap, schemas

    def get_varmaps(self, var2maps):
        qcols = {}
        for m in var2maps:
            for col in var2maps[m]:
                if col in qcols:
                    col2temp = var2maps[m][col]['coltotemp']
                    for c in col2temp:
                        qcols[col]['coltotemp'][c] = col2temp[c]
                    qcols[col]['subjcol'].extend(var2maps[m][col]['subjcol'])
                    varmap = var2maps[m][col]['varmap']
                    for v in varmap:
                        # TODO: this overwrites variables from previous star (try to prevent that)
                        # If the map reference is different then this could potentially return wrong results
                        qcols[col]['varmap'][v] = varmap[v]

                    qcols[col]['triples'].extend(var2maps[m][col]['triples'])
                    qcols[col]['triples'] = list(set(qcols[col]['triples']))
                    predmap = var2maps[m][col]['predmap']
                    for p in predmap:
                        qcols[col]['predmap'][p] = predmap[p]
                else:
                    qcols[col] = var2maps[m][col]
                    qcols[col]['subjcol'] = qcols[col]['subjcol']

        return qcols


def var2map(mapping, rdfmt, starpredicates, triples, prefixes):

    coltotemplate = dict()
    res = dict()
    for s in mapping:
        if rdfmt not in mapping[s]:
            continue
        smap = mapping[s][rdfmt]
        subject = smap.subject
        predintersects = set(starpredicates).intersection(set(list(smap.predicateObjMap.keys())))
        if len(predintersects) != len(set(starpredicates)):
            continue
        # TODO: for now only template subject types are supported.
        # Pls. include Reference (in case col values are uris) and Costants (in case collection is about one subject)
        if smap.subjectType == TermType.TEMPLATE or smap.subjectType == TermType.REFERENCE:
            varmap = {}
            predmaps = {}
            predobjConsts = {}
            subjectCols = smap.subjectCols

            for t in triples:
                if not t.subject.constant:
                    coltotemplate[t.subject.name[1:]] = subject
                for subj in subjectCols:
                    if subj not in varmap and not t.subject.constant:
                        varmap[t.subject.name] = str(subj)
                if t.predicate.constant and not t.theobject.constant:
                    pred = getUri(t.predicate, prefixes)[1:-1]
                    predobj = smap.predicateObjMap[pred]

                    if predobj.objectType == TermType.REFERENCE:
                        pp = predobj.object
                    elif predobj.objectType == TermType.TEMPLATE:
                        pp = predobj.object[predobj.object.find('{') + 1: predobj.object.find('}')]

                    elif predobj.objectType == TermType.CONSTANT:
                        predobjConsts[pred] = predobj.object
                        continue
                    else:
                        tpm = predobj.object
                        rmol = list(mapping[tpm].keys())[0]
                        rsubject = mapping[tpm][rmol].subject

                        rsubj = mapping[tpm][rmol].joinChild
                        rsubject = rsubject.replace(mapping[tpm][rmol].joinParent, rsubj)
                        pp = rsubj
                        coltotemplate[t.theobject.name[1:]] = rsubject
                    if pp is not None:
                        varmap[t.theobject.name] = pp
                        predmaps[pred] = pp

            if len(varmap) > 0:
                res.setdefault(smap.source, {})['varmap'] = varmap
                res[smap.source]['coltotemp'] = coltotemplate
                res[smap.source]['subjcol'] = subjectCols
                res[smap.source]['triples'] = triples
                res[smap.source]['predmap'] = predmaps
                res[smap.source]['predObjConsts'] = predobjConsts

    return res


def var2mapx(mapping, rdfmt, starpredicates, triples, prefixes):
    coltotemplate = dict()
    res = dict()
    for s in mapping:
        if rdfmt not in mapping[s]:
            continue
        smap = mapping[s][rdfmt]
        subject = smap['subject']
        if subject is None:
            continue
        # TODO: for now only template subject types are supported.
        # Pls. include Reference (in case col values are uris) and Costants (in case collection is about one subject)
        if smap['subjtype'] == TermType.TEMPLATE:
            varmap = {}
            predmaps = {}

            subjCols = smap['subjectCol']
            subjprefixes = smap['subjectPrefix']

            predObjMap = smap['predObjMap']

            predmap = {p: predObjMap[p] for p in predObjMap}
            subjevarmap = {}
            for t in triples:
                coltotemplate[t.subject.name[1:]] = subject if isinstance(subjprefixes, str) else subjprefixes
                if t.subject.name not in varmap and not t.subject.constant:
                    subjevarmap[t.subject.name] = subjCols
                if t.predicate.constant and not t.theobject.constant:
                    pred = getUri(t.predicate, prefixes)[1:-1]

                    pp = [predmap[p]['object'] for p in predmap if p == pred and predmap[p]['objType'] == TermType.REFERENCE]
                    pp.extend([predmap[p]['object'][predmap[p]['object'].find('{') + 1: predmap[p]['object'].find('}')] for p in predmap if p == pred and predmap[p]['objType'] == TermType.TEMPLATE])

                    temps = [predmap[p]['object'] for p in predmap if p == pred and predmap[p]['objType'] == TermType.TEMPLATE]
                    if len(temps) > 0:
                        coltotemplate[t.theobject.name[1:]] = temps[0]
                    tpm = [predmap[p]['object'] for p in predmap if p == pred and predmap[p]['objType'] == TermType.TRIPLEMAP]
                    for tp in tpm:
                        rmol = list(mapping[tp].keys())[0]
                        rsubject = mapping[tp][rmol]['subject']
                        rsubj = rsubject[rsubject.find('{') + 1: rsubject.find('}')]
                        iter = mapping[tp][rmol]['ls']['iterator']
                        iter = iter[iter.find('$')+1:]
                        topiter = smap['ls']['iterator']
                        topiter = topiter[topiter.find("$")+1:]
                        if topiter in iter:
                            iter = iter[iter.find(topiter[topiter.rfind('/')+1:])+len(topiter):]
                            iter = iter[iter.find('/')+1:]
                        pp.append(iter +'/' + rsubj)
                        coltotemplate[t.theobject.name[1:]] = rsubject
                    if len(pp) > 0:
                        varmap[t.theobject.name] = pp[0]
                        predmaps[pred] = pp[0]
                    else:
                        varmap = {}
                        break

            if len(varmap) > 0:
                varmap.update(subjevarmap)
                filename = smap['ls']['source']
                if smap['ls']['iterator'] != "*":
                    filename = smap['ls']['source'] + '/' + smap['ls']['iterator']

                res.setdefault(filename, {}).setdefault('varmap', {}).update(varmap)
                res[filename].setdefault('coltotemp', {}).update(coltotemplate)
                res[filename].setdefault('subjcol', []).append(subjCols)
                res[filename].setdefault('subjprefix', []).append(subjprefixes)
                res[filename].setdefault('triples', []).extend(triples)
                res[filename].setdefault('predmap', {}).update(predmaps)

    return res


def getPredObjDict(triplepatterns, prefixes):
    predobjdict = {}
    needselfjoin = False
    maxnumofobj = 0
    for t in triplepatterns:
        if t.predicate.constant:
            pred = getUri(t.predicate, prefixes)[1:-1]
            pred = pred+t.subject.name
            if pred in predobjdict:
                needselfjoin = True
                val = predobjdict[pred]
                if type(val) == list:
                    predobjdict[pred].append(t)
                else:
                    predobjdict[pred] = [val, t]
                if len(predobjdict[pred]) > maxnumofobj:
                    maxnumofobj = len(predobjdict[pred])
            else:
                predobjdict[pred] = t

    return predobjdict, needselfjoin, maxnumofobj


if __name__ == "__main__":
    from ontario.config.model import *
    datasource = DataSource("http://drugbank",
                            "/ontario/datasets/ensembl/jsons",
                            DataSourceType.SPARK_JSON,
                            name="CT")
    cl = SparkHDFSClient(datasource)
    from pprint import pprint
    for r in cl.list_collections():
        pprint(r)
        filename = r['db'] + '/' + r['document']
        if r['document'] == "homo_sapiens.json":
            rr, l = cl.get_documents(filename, limit=1)
            for re in rr:
                pprint(re)
            break