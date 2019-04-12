
__author__ = 'Kemele M. Endris'

from neo4j.v1 import GraphDatabase, basic_auth
from ontario.wrappers.neo4j.neo4j_utils import *
from ontario.sparql.parser import queryParser as qp
from multiprocessing import Queue
from pprint import pprint


class Neo4jClient(object):
    def __init__(self, url=None, username=None, passwd=None):
        self.url = url
        self.username = username
        self.password = passwd
        self.client = None
        self.driver = None

        self.create_session()

    def create_session(self, db=None):
        if self.driver is None:
            neo4jurl = self.url
            if neo4jurl is None:
                neo4jurl = 'bolt://localhost:7687'
                self.username = 'neo4j'
                self.password = '1234'

            if self.username is None:
                self.username = 'neo4j'
                self.password = '1234'

            self.driver = GraphDatabase.driver(neo4jurl,
                                               auth=basic_auth(self.username, self.password),
                                               max_connection_lifetime=30*60)

    def list_labels(self):
        if self.driver is None:
            self.create_session()
        cypher = "CALL db.labels() yield label as lbl " \
                 "WITH lbl " \
                 "MATCH (n) " \
                 "WHERE lbl in labels(n) " \
                 "RETURN lbl, count(*) as card "
        cypher = "Call db.labels"
        session = self.driver.session()
        results = []
        for r in session.run(cypher):
            r = dict(r)
            rr = {
                "db": self.url,
                "document": r['label'],
                "count": self.count_instance(r['label'])
            }
            results.append(rr)

        return results

    def count_instance(self, lbl):
        if self.driver is None:
            self.create_session()
        cypher = "MATCH (n:" + lbl + ") " \
                 "RETURN count(*) as card "
        session = self.driver.session()
        for r in session.run(cypher):
            return dict(r)['card']
        return -1

    def list_properties(self, lbl):
        if self.driver is None:
            self.create_session()

        cypher = "MATCH (n:" + lbl + ") " \
                 "UNWIND keys(n) as prop " \
                 "RETURN prop "
        session = self.driver.session()
        props = ["nodeTypes"]
        for p in session.run(cypher):
            props.append(dict(p)['prop'])

        cypher = "MATCH (n:" + lbl + ") - [r] -> (o)  " \
                 "RETURN type(r) as nrel "

        session = self.driver.session()
        rel = []
        for r in session.run(cypher):
            rel.append(dict(r)['nrel'])
        result = props + rel
        return sorted(list(set(result)))

    def get_sample(self, lbl, limit=25):
        if self.driver is None:
            self.create_session()
        nlimit = limit + 100
        cypher = "MATCH (n:" + lbl + ") " \
                 "RETURN n " \
                 "LIMIT " + str(limit)

        cypher = "MATCH (n:" + lbl + ") - [r] ->(o) " \
                 "RETURN  id(n) as id, n, labels(n) as otherlabels, type(r) as nrel, labels(o) as obj " \
                 "LIMIT " + str(nlimit)
        print(cypher)
        session = self.driver.session()
        nodes = {}
        for r in session.run(cypher):
            node = dict(dict(r)['n'])
            rel = dict(r)['nrel']
            obj = dict(r)['obj']
            ol = dict(r)['otherlabels']
            if dict(r)['id'] not in nodes:
                nodes[dict(r)['id']] = node
                node['nodeTypes'] = ol
            node = nodes[dict(r)['id']]
            node.setdefault(rel, []).extend(obj)

            node[rel] = list(set(node[rel]))
        results = []
        res = list(nodes.values())
        for r in res:
            rr = r.copy()
            for p in rr:
                if isinstance(rr[p], list):
                    rr[p] = " | ".join(rr[p])
            results.append(rr)

        return results[:limit], len(results[:limit])


class SPARQL2Cypher(object):
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
        self.prefixes = {}
        self.query = None

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

        self.neo_client = Neo4jClient(self.datasource.url, username, password)
        print(self.datasource)
        self.mappings = self.datasource.mappings
        # {tm: self.datasource.mappings[tm] for tm in self.datasource.mappings \
        #  for rdfmt in self.rdfmts if rdfmt in self.datasource.mappings[tm]}

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):

        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []

        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)

        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset
        cypher, coltotemplates, projvartocols = self.translate()
        print("Cypher: ", cypher)
        print(coltotemplates, '||', projvartocols)
        if cypher is not None and len(cypher) > 5:
            session = self.neo_client.driver.session()
            results = session.run(cypher)
            for r in results:
                rr = dict(r)
                # check if collect is used in one of the variables
                lstps = []
                for p in rr:
                    if isinstance(rr[p], list):
                        lstps.append(p)
                resulsts = []
                if len(lstps) > 0:
                    reslist = [rr.copy()]
                    for p1 in lstps:
                        rrp1 = rr[p1]
                        for v in rrp1:
                            for res in reslist:
                                res[p1] = v
                                resulsts.append(res.copy())
                        reslist = resulsts
                        resulsts = []
                    resulsts = reslist
                    for row in resulsts:
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
                                res[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}',
                                                                   row[r].replace(" ", '_'))
                            else:
                                res[r] = row[r]
                        queue.put(res)
                else:
                    res = {}
                    row = rr
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
        return []

    def translate(self):
        cypher = ""
        var2maps = {}
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

        print("Var2Maps:")
        pprint(var2maps)
        qcols = self.get_varmaps(var2maps)
        print("Qcols:")
        pprint(qcols)
        if len(qcols) == 0:
            print("Cannot get mappings for this subquery", self.query)
            return []

        if len(qcols) == 1:
            dbcol = list(qcols.keys())[0]
            varmaps = qcols[dbcol]
            lbl = dbcol
            cypher, projvartocols = self.translate_4_col(varmaps, lbl)

            coltotemplates = varmaps['coltotemp']
            return cypher, coltotemplates, projvartocols

        return cypher

    def translate_4_col(self, varmaps, lbl):
        triples = varmaps['triples']
        predobjdict, needselfjoin, maxnumofobj = getPredObjDict(triples, self.prefixes)
        filters = self.get_filters(triples)
        sparqlprojected = [c.name for c in self.query.args]
        print("projections:", sparqlprojected)
        creturn, projvartocol, projFilter = getReturnClause(self.mappings, varmaps['varmap'], sparqlprojected, varmaps['relationprops'])

        '''
          Case I: If subject is constant
        '''
        ifilters = [] # add values if subject is constant

        subjectfilters, firstfilter = getSubjectFilters(ifilters, maxnumofobj)
        '''
          Case II: If predicate + object are constants
        '''
        objectfilters, nontnulls = getObjectFilters(self.mappings,
                                                    self.prefixes,
                                                    triples,
                                                    varmaps,
                                                    maxnumofobj,
                                                    sparqlprojected,
                                                    self.query)

        cypher = " MATCH "
        cwhere = ""
        relfilter = []
        if len(varmaps['relationprops']) > 0:
            firstrel = True
            i = 0
            for rel in varmaps['relationprops']:
                if not firstrel:
                    cypher += ", "
                cypher += "(n:" + lbl + ") - [r" + str(i) + ":" + rel + "] -> (" + varmaps['relationprops'][rel] + "_" + rel + ":" + varmaps['relationprops'][rel] + ") "
                firstrel = False
                i += 1
                # relfilter.append("o." + rel.strip() + " IS NOT NULL ")
            # cypher += " - [r] -> (o) "
            # for re in varmaps['relationprops']:
            #     relfilter.append("o." + re.strip() + " IS NOT NULL ")
        else:
            cypher += "(n:" + lbl + ")"
        projFilter += relfilter
        projFilter += nontnulls

        if len(projFilter) > 0:
            cwhere += " AND ".join(list(set(projFilter)))
        if subjectfilters is not None and len(subjectfilters) > 0:
            cwhere += " AND ".join(subjectfilters)
        if objectfilters is not None and len(objectfilters) > 0:
            cwhere += " AND " + objectfilters

        if len(cwhere.strip()) > 0:
            cypher += " WHERE " + cwhere
        cypher += creturn

        return cypher, projvartocol

    def get_filters(self, triples):
        filters = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", getUri(t.theobject, self.prefixes)[1:-1])
                   for t in triples if t.predicate.constant and t.theobject.constant and
                   getUri(t.predicate, self.prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

        return filters

    def get_pred_vars(self, triples):
        predvars = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", t.theobject.name)
                    for t in triples if t.predicate.constant and not t.theobject.constant]
        return predvars

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


def check_neo4j_client():
    neo = Neo4jClient("bolt://localhost:7688", "neo4j", "1234")
    results = neo.list_labels()
    # pprint(results)
    for r in results:
        print("===================")
        print(r)
        print(r['document'], ': ', r['count'])
        print("===================")
        print("Properties:")
        pprint(neo.list_properties(r['document']))
        pprint(neo.get_sample(r['document']))


if __name__ == "__main__":
    check_neo4j_client()

