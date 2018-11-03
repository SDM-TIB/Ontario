import hashlib
import json
from pyspark.sql import SparkSession
from ontario.sparql.parser import queryParser as qp
from mysql import connector
from mysql.connector import errorcode
from multiprocessing import Process, Queue
from ontario.config.model import DataSourceType
from ontario.wrappers.mysql.utils import *
from json import load
from ontario.wrappers.hadoop import SparkHDFSClient
from pprint import pprint


class MySQLWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.mysql = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        username = None
        password = None
        if datasource.params is not None and len(datasource.params) > 0:
            if isinstance(datasource.params, dict):
                username = datasource.params['username'] if 'username' in datasource.params else None
                password = datasource.params['password'] if 'password' in datasource.params else None
            else:
                maps = datasource.params.split(';')
                for m in maps:
                    params = m.split(':')
                    if len(params) > 0:
                        if 'username' == params[0]:
                            username = params[1]
                        if 'password' == params[0]:
                            password = params[1]
        if ':' in self.url:
            host, port = self.url.split(':')
        else:
            host = self.url
            port = '3306'
        try:
            if username is None:
                self.mysql = connector.connect(user='root', host=self.url)
            else:
                self.mysql = connector.connect(user=username, password=password, host=host, port=port)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        except Exception as ex:
            print("Exception while connecting to Mysql", ex)

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

        sqlquery, coltotemplates, projvartocols, filenametablename, filenameiteratormap = self.translate()
        print(sqlquery)
        cursor = self.mysql.cursor()
        db = ""
        for fn in filenameiteratormap:
            db = filenameiteratormap[fn]['iterator']
        cursor.execute("use " + db)
        cursor.execute(sqlquery)
        header = [h[0] for h in cursor._description]

        for line in cursor:
            row = {}
            for i in range(len(line)):
                row[header[i]] = str(line[i])
                res = {}
                for r in row:
                    if '_' in r and r[:r.find("_")] in projvartocols:
                        s = r[:r.find("_")]
                        if s in res:
                            val = res[s]
                            res[s] = val.replace('{' + r[r.find("_") + 1:] + '}', row[r].replace(" ", '_'))
                        else:
                            res[s] = coltotemplates[s].replace('{' + r[r.find("_") + 1:] + '}',
                                                               row[r].replace(" ", '_'))
                    elif r in projvartocols and r in coltotemplates:
                        res[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}', row[r].replace(" ", '_'))
                    else:
                        res[r] = row[r]
                queue.put(res)

        queue.put("EOF")

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

        sqlquery, projvartocols, coltotemplates, filenametablenamemap, filenameiteratormap = self.translate_4_msql_df(qcols)

        print(coltotemplates)
        print(projvartocols)
        return sqlquery, coltotemplates, projvartocols, filenametablenamemap, filenameiteratormap

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
        filenameiteratormap = {}
        for filename, varmaps in qcols.items():
            triples = varmaps['triples']
            tablename = varmaps['source']
            variablemap = varmaps['varmap']

            coltotemplates.update(varmaps['coltotemp'])

            filenametablenamemap[filename] = tablename
            # filenameiteratormap[filename] = varmaps['iterator']
            filenameiteratormap[filename] = {'iterator': varmaps['iterator'], 'source': varmaps['source']}
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
            objectfilter = getObjectFilters(self.mappings, self.prefixes, triples, varmaps, tablealias, sparqlprojected, self.query)
            if objectfilter is not None:
                objectfilters.extend(objectfilter)

            i += 1

        if len(subjtablemap) > 1:
            aliases = list(subjtablemap.keys())
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
        return sqlquery, projvartocols, coltotemplates, filenametablenamemap, filenameiteratormap

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
