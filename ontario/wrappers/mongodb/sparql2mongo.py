
__author__ = 'Kemele M. Endris'

from ontario.sparql.parser import queryParser as qp
from ontario.wrappers.mongodb.s2m_utils import *
from ontario.sparql.parser.services import Argument
from multiprocessing import Queue
from ontario.wrappers.mongodb import MongoDBClient


class SPARQL2Mongo:
    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
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
        self.mongo_client = MongoDBClient(self.datasource.url, username, password)

        self.mappings = {tm: self.datasource.mappings[tm] for tm in self.datasource.mappings
                         for rdfmt in self.rdfmts if rdfmt in self.datasource.mappings[tm]}

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

        pipeline, db, col, coltotemplates, projvartocols = self.translate()
        if self.query.limit > 0:
            if self.query.offset > 0:
                pipeline.append({"$limit": int(self.query.limit) + int(self.query.offset)})
                pipeline.append({"$skip": int(self.query.offset)})
            else:
                pipeline.append({"$limit": int(self.query.limit)})
        # print("MongoDB query pipeline:")
        # pprint(pipeline)
        db, collection = self.mongo_client.get_db_coll_obj(db, col)
        result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
        for doc in result:
            for r in doc:
                if isinstance(doc[r], int):
                    doc[r] = str(doc[r])
                if r in projvartocols and r in coltotemplates:
                    doc[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}', doc[r].replace(" ", '_'))
            queue.put(doc)
        queue.put("EOF")

        return result

    def translate(self):

        pipeline = []
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

        qcols = self.get_varmaps(var2maps)
        if len(qcols) == 0:
            print("Cannot get mappings for this subquery", self.query)
            return []

        if len(qcols) == 1:
            dbcol = list(qcols.keys())[0]
            varmaps = qcols[dbcol]
            pipeline, projvartocols = self.translate_4_col(varmaps)
            db, col = dbcol.split('/')
            coltotemplates = varmaps['coltotemp']
            return pipeline, db, col, coltotemplates, projvartocols
        # else:
        #     pprint(qcols)

        return pipeline

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

    def translate_4_col(self, varmaps):
        pipeline = []
        sparqlfilters = []
        subjidfilters = []
        triples = varmaps['triples']
        predobjdict, needselfjoin, maxnumofobj = getPredObjDict(triples, self.prefixes)
        filters = self.get_filters(triples)
        sparqlprojected = [c.name for c in self.query.args]
        projections, projvartocol = self.get_projections(varmaps['varmap'], sparqlprojected)
        if maxnumofobj > 1:
            predmap = varmaps['predmap']
            ormatch, unwind = self.get_match_clause(triples, filters, sparqlfilters, varmaps, subjidfilters, sparqlprojected)
            grouping = self.getGrouping(varmaps['subjcol'], unwind, predmap, sparqlprojected)
            andmatch, unwind = self.get_match_clause(triples, filters, sparqlfilters, varmaps, subjidfilters, sparqlprojected, "$all")

            pipeline.append({'$match': ormatch})
            pipeline.append({'$group': grouping})
            pipeline.append({'$match': andmatch})
            for subjectColumn in varmaps['subjcol']:
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
            match, unwind = self.get_match_clause(triples, filters, sparqlfilters, varmaps, subjidfilters, sparqlprojected)
            if len(match) > 0:
                pipeline.append({'$match': match})

            pipeline.append({'$project': projections})

        return pipeline, projvartocol

    def get_filters(self, triples):
        filters = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", getUri(t.theobject, self.prefixes)[1:-1])
                   for t in triples if t.predicate.constant and t.theobject.constant and
                   getUri(t.predicate, self.prefixes)[1:-1] != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type']

        return filters

    def get_pred_vars(self, triples):
        predvars = [(getUri(t.predicate, self.prefixes)[1:-1], " = ", t.theobject.name)
                    for t in triples if t.predicate.constant and not t.theobject.constant]
        return predvars

    def get_projections(self, vartoColumnMap, sparqlprojected):
        projvartocol = {}
        projections = {}
        for var in sparqlprojected:
            if var in vartoColumnMap:
                column = vartoColumnMap[var]
                projections[var[1:]] = '$' + column.strip()
                projvartocol[var[1:]] = column.strip()

            else:
                print('no mapping for', sparqlprojected, var)

        projections["_id"] = 0
        return projections, projvartocol

    def get_match_clause(self, triples, filters, sparlfilters, varmaps, ifilters, sparqlprojected, arrayop='$in'):
        predmap = varmaps['predmap']
        subjcols = varmaps['subjcol']

        predvars = self.get_pred_vars(triples)
        # if len(filters) == 0 and len(predvars) == 0:
        #     return None

        nnull = self.getNotNULLs(predvars, predmap, sparqlprojected)
        for subj in subjcols:
            if subj in list(predmap.values()):
                continue
            nn = {subj.strip(): {'$ne': 'null'}}
            nmpty = {subj.strip(): {'$ne': ''}}
            nnull.append(nn)
            nnull.append(nmpty)

        predobjmap = {}
        for t in triples:
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

    def getNotNULLs(self, predvars, predmap, sparqlprojected):
        nnull = []
        for v in predvars:
            if v[0] not in predmap:
                continue
            if v[2] in sparqlprojected:
                nn = {predmap[v[0]].strip(): {'$ne': 'null'}}
                nmpty = {predmap[v[0]].strip(): {'$ne': ''}}
                nnull.append(nn)
                nnull.append(nmpty)

        return nnull

    def getFiltermaps(self, filters, sparlfilters, predmap, arrayop, predobjmap):
        filtersmap = {}
        match = {}
        unwind = []
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

    def getGrouping(self, subjectColumns, unwind, predmap, sparqlprojected):
        group = {'_id': '$'+subjectColumns[0].strip()}
        for subjectColumn in subjectColumns:
            group[subjectColumn.strip()] = {'$addToSet': '$' + subjectColumn.strip()}

        # { $group: { _id:'$product', productFeature:{$addToSet:'$productFeature'}}}
        for uw in unwind:
            group[uw] = {'$addToSet': '$' + uw.strip()}

        for p in predmap:
            if predmap[p] in unwind:
                continue
            group[predmap[p]] = {'$addToSet': '$' + predmap[p].strip()}

        return group


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

