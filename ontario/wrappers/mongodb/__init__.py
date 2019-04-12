
__author__ = 'Kemele M. Endris'

from pymongo import MongoClient
from ontario.sparql.parser import queryParser as qp
from ontario.wrappers.mongodb.s2m_utils import *
from multiprocessing import Queue


class MongoDBClient(object):

    def __init__(self, url=None, username=None, passwd=None):
        print(url, username, passwd)
        self.url = url
        self.username = username
        self.passwd = passwd
        self.client = None

    def clreate_client(self):
        if self.url is not None and self.username is not None and self.passwd is not None:
            self.client = MongoClient(self.url, self.username, self.passwd)
        elif self.url is not None:
            self.client = MongoClient(self.url)
        else:
            self.client = MongoClient("localhost:27017")

    def listDatabases(self):
        if self.client is None:
            self.clreate_client()
        dbs = self.client.list_databases()
        return [d for d in dbs if d['name'] not in ['admin', 'config', 'local']]

    def listCollections(self, dbname):
        if self.client is None:
            self.clreate_client()
        db = self.client.get_database(dbname)
        colls = db.list_collections()
        return [{"name": c['name'], 'type':c['type'], 'count': db.get_collection(c['name']).count()} for c in colls if c['name'] != 'delete_me']

    def get_documents(self, dbname, colname, limit=15):
        if self.client is None:
            self.clreate_client()
        db = self.client.get_database(dbname)
        coll = db.get_collection(colname)
        docs = list(coll.find().limit(limit))
        count = coll.find().count()

        return docs, count

    def get_db_coll_obj(self, dbname, colname):
        self.clreate_client()
        db = self.client.get_database(dbname)
        coll = db.get_collection(colname)
        return db, coll


class SPARQL2MongoDB(object):

    def __init__(self, federation, ds, config, rdfmts, star, configmgr):
        self.federation = federation
        self.datasource = ds
        self.rdfmts = rdfmts
        self.url = ds.url
        self.params = ds.params
        self.config = config
        self.spark = None
        self.df = None
        self.result = None
        self.configmgr = configmgr
        self.star = star

        username = ds.params['username'] if 'username' in ds.params else None
        if username is not None:
            password = ds.params['password'] if 'password' in ds.params else None
        else:
            password = None
        self.mongo_client = MongoDBClient(self.datasource.url, username, password)

        #rmlmgr = RMLManager(self.configmgr.endpoint, self.configmgr.federation, ds.rid)
        self.mappings = {}# rmlmgr.loadAll()

    def getGrouping(self, subjectColumn, unwind, predmap, sparqlprojected):
        group = {'_id': '$'+subjectColumn.strip(), subjectColumn.strip(): {'$addToSet': '$' + subjectColumn.strip()}}
        #{ $group: { _id:'$product', productFeature:{$addToSet:'$productFeature'}}}
        for uw in unwind:
            group[uw] = {'$addToSet': '$' + uw.strip()}

        for p in predmap:
            if predmap[p] in unwind:
                continue
            group[predmap[p]] = {'$addToSet': '$' + predmap[p].strip()}

        return group

    def getProjections(self, triplepatterns, vartoColumnMap, maxnumofobj, sparqlprojected):
        projvartocol = {}
        projections = {}
        for var in sparqlprojected:
            for m in vartoColumnMap:
                if var in vartoColumnMap[m]:
                    column = vartoColumnMap[m][var]
                    projections[var[1:]] = '$' + column.strip()
                    projvartocol[var[1:]] = column.strip()

                else:
                    print('no mapping for', sparqlprojected, var)

        return projections, projvartocol

    def getNotNULLs(self, predvars, predmap, sparqlprojected):
        nnull = []
        #for m in predmap:
        for v in predvars:
            if v[0] not in predmap:
                continue

            if v[2] in sparqlprojected:
                #{$and: [{productPropertyNumeric4:{$ne:null}}, {productPropertyNumeric4:{$ne:''}}]}
                nn = {predmap[v[0]].strip(): {'$ne': 'null'}}
                nmpty = {predmap[v[0]].strip(): {'$ne': ''}}
                nnull.append(nn)
                nnull.append(nmpty)

        return nnull

    def getFiltermaps(self, filters, sparlfilters, predmap, arrayop, predobjmap):
        filtersmap = {}
        match = {}
        unwind = []
        #for m in predmap:
        for f in filters:
            if f[0] not in predmap:
                continue
            if f[0] in filtersmap:
                filtersmap[f[0]].append(f[2])
            else:
                filtersmap[f[0]] = [f[2]]

        for v in filtersmap:
            val = filtersmap[v]
            if len(val) > 1:
                match[predmap[v]] = {arrayop: list(set(val))}
                unwind.append(predmap[v])
            else:
                match[predmap[v]] = val[0]
        sparqlfiltered = self.getFilters(sparlfilters, predmap, predobjmap)
        for f in sparqlfiltered:
            match[f] = sparqlfiltered[f]
        '''for f in sparqlfiltered:
            if f in mquery:
                if type(mquery[f]) == dict:
                    if type(sparqlfilters[f]) == dict:
                        mquery[f].update(sparqlfilters[f])
                    else:
                        mquery[f]["$in"].append(sparqlfilters[f])
                else:
                    if type(sparqlfilters[f]) == dict:
                        mquery[f] = {"$eq": mquery[f]}
                        mquery[f].update(sparqlfilters[f])
                    else:
                        mquery[f] = {"$in": [mquery[f]]}
                        mquery[f]["$in"].append(sparqlfilters[f])
            else:
                mquery[f] = sparqlfilters[f]
        '''

        return match, list(set(unwind))

    def getMatching(self, triplepatterns, filters, sparlfilters, predmap, ifilters, sparqlprojected, arrayop='$in'):
        match = {}
        #If predicate is constant and object is a variable
        # predvars = [(t.predicate.name[1:-1], " = ", t.theobject.name)
        #             for t in triplepatterns if t.predicate.constant and not t.theobject.constant]
        predvars = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", t.theobject.name)
                    for t in triplepatterns if t.predicate.constant and not t.theobject.constant]

        if len(filters) == 0 and len(predvars) == 0:
            return None

        nnull = self.getNotNULLs(predvars, predmap, sparqlprojected)

        predobjmap = {}
        for t in triplepatterns:
            if not t.subject.constant:
                predobjmap[t.subject.name] = t.subject.name
            if t.predicate.constant and not t.theobject.constant:
                predobjmap[t.predicate.name[1:-1]] = t.theobject.name

        match, unwind = self.getFiltermaps(filters, sparlfilters, predmap, arrayop, predobjmap)
        # IF subject is constant
        if len(ifilters) > 0:
            match[ifilters[0][0]] = ifilters[0][2]

        if len(nnull) > 0:
            match['$and'] = nnull

        return match, unwind

    def getFilters(self, filters, predmap, predobjmap):
        fquery = {}

        for f in filters:
            r = ""
            l = ""
            if isinstance(f.expr.left, Argument) and isinstance(f.expr.right, Argument):
                left = f.expr.left
                if left.constant:
                    if "<" in left.name:
                        left = "'" + left.name[1:-1] + "'"
                    else:
                        left = left.name
                    r = left
                else:
                    left = left.name
                    l = left

                right = f.expr.right
                if right.constant:
                    if "<" in right.name:
                        right = "'" + right.name[1:-1] + "'"
                    else:
                        right = right.name
                    r = right
                else:
                    right = right.name
                    l = right
                if "'" not in r and '"' not in r:
                    r = int(r)
                else:
                    r = r.replace('"', '').replace("'", '').strip()
                op = "$eq"
                if f.expr.op == '>':
                    op = "$gt"
                elif f.expr.op == '<':
                    op = "$lt"
                elif f.expr.op == '>=':
                    op = "$gte"
                elif f.expr.op == '<=':
                    op = "$lte"
                elif f.expr.op == '!=':
                    op = "$ne"

                for k in predobjmap:
                    v = predobjmap[k]
                    if v == l:
                        #for m in predmap:
                        for kk in predmap:
                            vv = predmap[kk]
                            if k == kk:
                                if op == "$eq":
                                    fquery[vv] = r
                                else:
                                    fquery[vv] = {op: r}

        return fquery

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):

        self.query = qp.parse(query)
        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset

        sparql = self.query

        pipeline, db, col, coltotemplates, projvartocols = self.translate()
        '''mquery, mproj, cmpquery = self.rewrite(sparql)
        mproj["_id"] = 0

        pipeline = []
        if len(mquery) > 0:
            pipeline.append({"$match": mquery})

        pipeline.append({"$project": mproj})
        '''
        if sparql.limit > 0:
            if sparql.offset > 0:
                pipeline.append({"$limit": int(sparql.limit) + int(sparql.offset)})
                pipeline.append({"$skip": int(sparql.offset)})
            else:
                pipeline.append({"$limit": int(sparql.limit)})
        print(pipeline)
        db, collection = self.mongo_client.get_db_coll_obj(db, col)
        result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
        for doc in result:
            for r in doc:
                if isinstance(doc[r], int):
                    doc[r] = str(doc[r])
                if projvartocols[r] in coltotemplates:
                    doc[r] = coltotemplates[projvartocols[r]].replace('{' + projvartocols[r] + '}', doc[r].replace(" ", '_'))
            queue.put(doc)
        queue.put("EOF")

        return result

    def translate(self):

        pipeline = []

        self.prefixes = getPrefs(self.query.prefs)
        coltotemplates = {}

        # projvartocols = {}
        # for s in self.star:
        # ds = self.star['datasource']
        # predicates = self.star['predicates']
        # rdfmts = self.star['rdfmts']

        triples = self.star['triples']

        if len(self.mappings) == 0:
            return "Empty Mapping"
        varmap, coltotemplate, subjcols, dbcols, starcollects = vartocolumnmapping(self.mappings, triples, self.rdfmts, self.prefixes)

        for c in coltotemplate:
            coltotemplates[c] = coltotemplate[c]
        dd = dbcols[list(dbcols.keys())[0]]
        db = dd['db']
        col = dd['col']
        if len(varmap) > 0:
            sparqlprojected = [c.name for c in self.query.args]
            for subj in subjcols:
                pipeline, db, col, projvartocol = self.to_mongo_ql(triples, [], subj, varmap, sparqlprojected, dbcols)
                # pipeline.append(subj_pipeline)
                return pipeline, db, col, coltotemplate, projvartocol

        return pipeline, db, col, coltotemplate, {}

    def to_mongo_ql(self, triplepatterns, sparqlfilters, subjectColumn, variablemap, sparqlprojected, dbcols):

        pipeline = []
        ifilters = []
        predobjdict, needselfjoin, maxnumofobj = getPredObjDict(triplepatterns, self.prefixes)

        # IF predicate + object are constants
        filters = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", getUri(t.theobject, self.prefixes)[1:-1])
                   for t in triplepatterns if t.predicate.constant and t.theobject.constant and
                   getUri(t.predicate, self.prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

        predmap = self.get_pred_map(subjectColumn)
        dd = dbcols[list(dbcols.keys())[0]]
        db = dd['db']
        col = dd['col']

        projections, projvartocol = self.getProjections(triplepatterns, variablemap, maxnumofobj, sparqlprojected)
        projections["_id"] = 0

        if maxnumofobj > 1:
            ormatch, unwind = self.getMatching(triplepatterns, filters, sparqlfilters, predmap, ifilters, sparqlprojected)
            grouping = self.getGrouping(subjectColumn, unwind, predmap, sparqlprojected)
            andmatch, unwind = self.getMatching(triplepatterns, filters, sparqlfilters, predmap, ifilters, sparqlprojected, "$all")

            pipeline.append({'$match': ormatch})
            pipeline.append({'$group': grouping})
            pipeline.append({'$match': andmatch})
            xunwind = {}
            xunwind['path'] = "$" + subjectColumn
            xunwind['preserveNullAndEmptyArrays'] = True
            pipeline.append({'$unwind': xunwind})
            for p in predmap:
                xunwind = {}
                xunwind['path'] = "$" + predmap[p]
                xunwind['preserveNullAndEmptyArrays'] = True
                pipeline.append({'$unwind': xunwind})
            pipeline.append({'$project': projections})
        else:
            match, unwind = self.getMatching(triplepatterns, filters, sparqlfilters, predmap, ifilters, sparqlprojected)
            if len(match) > 0:
                pipeline.append({'$match': match})

            pipeline.append({'$project': projections})

        return pipeline, db, col, projvartocol

    def get_pred_map(self, subj):

        mappings = self.mappings
        res = dict()
        predmap = dict()
        for s in mappings:
            for m in self.rdfmts:
                if m not in mappings[s]:
                    continue
                subject = mappings[s][m]['subject']
                predmap[subj] = subject[subject.find('{') + 1: subject.find('}')]
                predicates = mappings[s][m]['predConsts']
                predObjMap = mappings[s][m]['predObjMap']
                predobjmap = {p: predObjMap[p] for p in predicates if predObjMap[p]['objType'] == TermType.REFERENCE}
                for p in predobjmap:
                    predmap[p] = predobjmap[p]['object']
                predTripMap = {p: predObjMap[p] for p in predicates if predObjMap[p]['objType'] == TermType.TRIPLEMAP}
                for p in predTripMap:
                    if predTripMap[p]['object'] not in mappings:
                        continue
                    predobj = mappings[predTripMap[p]['object']]
                    for mo in predobj:
                        predsubj = mappings[predTripMap[p]['object']][mo]['subject']
                        predmap[p] = predsubj[predsubj.find('{') + 1: predsubj.find('}')]
                # TODO: include this if the joins are push down to wrappers
                predTempMap = {p: predObjMap[p] for p in predicates if predObjMap[p]['objType'] == TermType.TEMPLATE}
                #res[m] = predmap
        # predmap = {t.predicate[1:-1]: t.refmap.name for t in mapping.predicates}

        return predmap


if __name__ == "__main__":
    query = """
        prefix iasis: <http://project-iasis.eu/vocab/> 
         select * where {
             ?s a  iasis:CNV .
             ?s iasis:id ?id .
             ?s iasis:total_cn ?z .
             ?s iasis:cnv_isLocatedIn_sample ?sample
        }
    """
    ds = "http://tib.eu/dsdl/ontario/resource/COSMIC"

    # mgraph = "http://tib.eu/dsdl/ontario/g/Ontario-SDL"  # all-bio2rdf-wz-pubmed
    # print("******* ****************************")
    #
    # mgmgr = MTManager("http://node2.research.tib.eu:1500/sparql",
    #                  "admin",
    #                  "pw123", mgraph)
    # ds = mgmgr.get_data_source(ds)
    #
    # sm = SPARQL2Mongo("localhost:27017")
    # sm.translate(query)
    # exit()
    # client = MongoDBClient('node3.research.tib.eu:27017')
    # dbs = client.listDatabases()
    # from pprint import pprint
    # dbs = list(dbs)
    # for d in dbs:
    #     pprint(d)
    #     colls = client.listCollections(d['name'])
    #     for c in colls:
    #         print(c)
    #         docs, count = client.get_documents(d['name'], c['name'], 1)
    #         print(count)
    #         for dc in docs:
    #             pprint(dc)
