__author__ = 'kemele'

import logging

import ontario.sparql.utilities as utils
from ontario.sparql.parser import queryParser
from ontario.sparql.parser.services import Service, Triple, Filter, Optional, UnionBlock, JoinBlock
from ontario.mediator.Tree import Tree
from ontario.config import OntarioConfiguration


class LakeCatalyst(object):

    def __init__(self, query, config):
        self.query = queryParser.parse(query)
        self.prefixes = utils.getPrefs(self.query.prefs)
        self.config = config
        self.relevant_mts = {}

    def decompose(self):
        return self.decomposeUnionBlock(self.query.body)

    def decomposeUnionBlock(self, ub):
        """
        Decompose a UnionBlock of a SPARQL query

        :param ub: UnionBlock
        :return:
        """
        ubs = []
        for jb in ub.triples:
            ubs.append(self.decomposeJoinBlock(jb))
        return ubs

    def decomposeJoinBlock(self, jb):
        """
        decompose a Join Block into a set of star-shaped subqueries and find a matching RDF Molecule Templates
        :param jb: JoinBlock
        :return:
        """
        bgp = []
        filters = []
        for tp in jb.triples:
            if isinstance(tp, Triple):
                bgp.append(tp)
            elif isinstance(tp, Filter):
                filters.append(tp)

        bgp = self.decomposeBGP(bgp, filters)
        return bgp

    def decomposeBGP(self, bgp, filters):
        """
        Decompose a BGP into star-shaped subqueries
        :param bgp:
        :return:
        """
        res = {}
        bgp_preds = self.get_preds(bgp)
        stars = self.bgp_stars(bgp)
        star_conn = self.getStarsConnections(stars)
        varpreds = {}
        star_preds = {}

        for s in stars:
            star = stars[s]
            preds = self.get_preds(star)
            star_preds[s] = preds
            typed = self.checkRDFTypeStatemnt(star)
            if typed is None:
                print("Subquery: ", stars[s], "\nCannot be executed, because it contains an RDF MT that "
                                              "does not exist in this federations of datasets.")
                return []
            if len(typed) > 0:

                for m in typed:
                    #properties = [p['predicate'] for p in typed[m]['predicates']]
                    properties = list(typed[m].predicates.keys())
                    if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in preds and \
                            'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' not in properties:
                        preds.remove('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
                        pinter = set(properties).intersection(set(preds))

                        if len(pinter) != len(set(preds)):
                            print("Subquery: ", stars[s], "\nCannot be executed, because it contains properties that "
                                                          "does not exist in this federations of datasets.")
                            return []
                        else:
                            self.relevant_mts[m] = typed[m]

                        preds.append('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
                    else:
                        pinter = set(properties).intersection(set(preds))
                        if len(pinter) != len(set(preds)):
                            print("Subquery: ", stars[s], "\nCannot be executed, because it contains properties that "
                                                          "does not exist in this federations of datasets.")
                            return []
                        else:
                            self.relevant_mts[m] = typed[m]

                res[s] = typed
                continue
            # if ssq contains at least one triple pattern with constant predicate
            mols = []
            if len(preds) > 0:
                mols = self.config.find_rdfmt_by_preds(preds)
                self.relevant_mts.update(mols)
                mols = mols.keys()

                if len(mols) == 0:
                    print("cannot find any matching for:", stars[s])
                    return []
            else:
                varpreds[s] = star
                continue

            # if len(mols) == 0:
            #     mols = self.config.metadata

            if len(mols) > 0:
                res.setdefault(s, []).extend(mols)
            else:
                print("cannot find any matching molecules for:", star)
                return []

        if len(varpreds) > 0:
            for s in varpreds:
                found = False
                for c in star_conn:
                    v = star_conn[c]
                    if s in v:
                        for m in res[c]:
                            mols = [self.config.metadata[r] for mt in m for p in self.relevant_mts[mt].predicates for r in p.ranges]
                            mols = [mt.uri for mt in mols]
                            res.setdefault(s, []).extend(mols)
                            found = True
                if not found:
                    mols = list(self.config.metadata.keys())
                    res.setdefault(s, []).extend(mols)

        # Connection between the selected RDF-MTs for stars of the BGP
        # This connection info is used to further eliminate non-relevant sources
        res_conn = self.getMTsConnection(res, bgp_preds)

        res = self.prune(star_conn, res_conn, res, stars)
        results = {}
        for s in res:
            results[s] = {}
            results[s]['predicates'] = star_preds[s]
            results[s]['filters'] = filters
            for m in res[s]:
                results[s][m] = stars[s]

        return results

    def prune(self, star_conn, res_conn, selectedmolecules, stars):
        newselected = {}
        res = {}
        counter = 0
        for s in selectedmolecules:
            if len(selectedmolecules[s]) == 1:
                newselected[s] = list(selectedmolecules[s])
                res[s] = list(selectedmolecules[s])
                counter += 1
            else:
                newselected[s] = []
                res[s] = []
        if counter == len(selectedmolecules):
            return res

        for s in selectedmolecules:
            sc = star_conn[s]

            for sm in selectedmolecules[s]:
                smolink = res_conn[sm]

                for c in sc:
                    cmols = selectedmolecules[c]
                    nms = [n for m in smolink for n in m['range'] if n in cmols]
                    if len(nms) > 0:
                        res[s].append(sm)
                        res[c].extend(nms)

        # check predicate level connections
        newfilteredonly = {}
        for s in res:
            sc = [c for c in star_conn if s in star_conn[c]]
            for c in sc:
                connectingtp = [utils.getUri(tp.predicate, self.prefixes)[1:-1]
                                for tp in stars[c] if tp.theobject.name == s]
                connectingtp = list(set(connectingtp))
                sm = selectedmolecules[s]
                for m in sm:
                    srange = [p for r in self.relevant_mts[m].predicates for p in self.relevant_mts[m].predicates[r].ranges if self.relevant_mts[m].predicates[r].predicate in connectingtp]
                    filteredmols = [r for r in res[s] if r in srange]
                    if len(filteredmols) > 0:
                        if s in newfilteredonly:
                            newfilteredonly[s].extend(filteredmols)
                        else:
                            res[s] = filteredmols

        for s in newfilteredonly:
            res[s] = list(set(newfilteredonly[s]))

        for s in res:
            if len(res[s]) == 0:
                res[s] = selectedmolecules[s]
            res[s] = list(set(res[s]))
        return res

    def get_preds(self, star):
        """
        Returns a set of predicates in a BGP/star-shaped subquery
        :param star: list of triple patterns
        :return: list of predicates
        """

        preds = [utils.getUri(tr.predicate, self.prefixes)[1:-1] for tr in star if tr.predicate.constant]

        return preds

    def bgp_stars(self, bgp):
        """
        Split BGP to a set of Star-shaped Subqueries and return a dictionary of stars' subject part to set of triples:
            stars = {'?s1': [tp1, tp2, ..], '?s2':[tp3, ..], ..}
        :param bgp: Basic Graph Pattern of a SPARQL query
        :return: stars = {'?s1': [tp1, tp2, ..], '?s2':[tp3, ..], ..}
        """
        stars = {}

        for tp in bgp:
            subj = tp.subject.name
            if subj in stars:
                stars[subj].append(tp)
            else:
                stars[subj] = [tp]

        return stars

    def getStarsConnections(self, stars):
        """
        extracts links between star-shaped sub-queries
        :param stars: map of star-shaped sub-queries with its root (subject) {subject: [triples in BGP]}
        :return: map of star-shaped sub-query root name (subject) with its connected sub-queries via its object node.
         {subj1: [subjn]} where one of subj1's triple pattern's object node is connected to subject node of subjn
        """
        conn = dict()
        for s in stars.copy():
            ltr = stars[s]
            conn[s] = []
            for c in stars:
                if c == s:
                    continue
                for t in ltr:
                    if t.theobject.name == c:
                        if c not in conn[s]:
                            conn[s].append(c)
                        break

        return conn

    def getMTsConnection(self, selectedmolecules, preds):
        mcons = {}
        smolecules = [m for s in selectedmolecules for m in selectedmolecules[s]]
        for s in selectedmolecules:
            mols = selectedmolecules[s]
            for m in mols:
                mcons[m] = [n for n in self.relevant_mts[m].predicates \
                            for r in self.relevant_mts[m].predicates[n].ranges \
                            if r in smolecules and self.relevant_mts[m].predicates[n].predicate in preds]
        return mcons

    def checkRDFTypeStatemnt(self, ltr):
        types = self.getRDFTypeStatement(ltr)
        typemols = {}
        for t in types:
            tt = utils.getUri(t.theobject, self.prefixes)[1:-1]
            mt = self.config.metadata[tt]
            typemols[tt] = mt
        if len(types) > 0 and len(typemols) == 0:
            return None

        return typemols

    def getRDFTypeStatement(self, ltr):
        types = []
        for t in ltr:
            if t.predicate.constant \
                    and (t.predicate.name == "a"
                         or t.predicate.name == "rdf:type"
                         or t.predicate.name == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") \
                    and t.theobject.constant:
                types.append(t)

        return types

    '''
    ===================================================
    ========= FILTERS =================================
    ===================================================
    '''

    def includeFilter(self, jb_triples, fl):
        fl1 = []
        for jb in jb_triples:

            if isinstance(jb, list):
                for f in fl:
                    fl2 = self.includeFilterAux(f, jb)
                    fl1 = fl1 + fl2
            elif isinstance(jb, UnionBlock):
                for f in fl:
                    fl2 = self.includeFilterUnionBlock(jb, f)
                    fl1 = fl1 + fl2
            elif isinstance(jb, Service):
                for f in fl:
                    fl2 = self.includeFilterAuxSK(f, jb.triples, jb)
                    fl1 = fl1 + fl2
        return fl1

    def includeFilterAux(self, f, sl):
        fl1 = []
        for s in sl:
            vars_s = set()
            for t in s.triples:
                vars_s.update(set(utils.getVars(t)))
            vars_f = f.getVars()
            if set(vars_s) & set(vars_f) == set(vars_f):
                s.include_filter(f)
                fl1 = fl1 + [f]
        return fl1

    def includeFilterUnionBlock(self, jb, f):
        fl1 = []
        for jbJ in jb.triples:
            for jbUS in jbJ.triples:
                if isinstance(jbUS, Service):
                    vars_s = set(jbUS.getVars())
                    vars_f = f.getVars()
                    if set(vars_s) & set(vars_f) == set(vars_f):
                        jbUS.include_filter(f)
                        fl1 = fl1 + [f]
        return fl1

    def includeFilterAuxSK(self, f, sl, sr):
        """
        updated: includeFilterAuxS(f, sl, sr) below to include filters that all vars in filter exists in any of the triple
        patterns of a BGP. the previous impl includes them only if all vars are in a single triple pattern
        :param f:
        :param sl:
        :param sr:
        :return:
        """
        fl1 = []
        serviceFilter = False
        fvars = dict()
        vars_f = f.getVars()

        for v in vars_f:
            fvars[v] = False
        bgpvars = set()

        for s in sl:
            bgpvars.update(set(utils.getVars(s)))
            vars_s = set()
            if (isinstance(s, Triple)):
                vars_s.update(set(utils.getVars(s)))
            else:
                for t in s.triples:
                    vars_s.update(set(utils.getVars(t)))

            if set(vars_s) & set(vars_f) == set(vars_f):
                serviceFilter = True

        for v in bgpvars:
            if v in fvars:
                fvars[v] = True
        if serviceFilter:
            sr.include_filter(f)
            fl1 = fl1 + [f]
        else:
            fs = [v for v in fvars if not fvars[v]]
            if len(fs) == 0:
                sr.include_filter(f)
                fl1 = fl1 + [f]
        return fl1

    def updateFilters(self, node, filters):
        return UnionBlock(node.triples, filters)


class MetaCatalyst(object):

    def __init__(self, star, config):
        self.star = star
        self.config = config
        self.mts = []
        self.triples = []
        self.predicates = []
        for m in star:
            if m == 'predicates':
                self.predicates = list(set(star[m]))
                continue
            elif m == 'filters':
                self.filters = list(set(star[m]))
                continue
            self.mts.append(m)
            self.triples = star[m]

    def decompose(self, prefixes):
        sources = {}
        for m in self.mts:
            mt = self.config.metadata[m]
            datasourses = self.config.datasources
            sources[m] = {datasourses[w].ID: list(set(mt.datasources[w]).intersection(self.predicates)) for w in mt.datasources\
                          if len(list(set(mt.datasources[w]).intersection(self.predicates))) == len(self.predicates)}
            if len(sources[m]) == 0:
                sources[m] = {datasourses[w].ID: list(set(mt.datasources[w]).intersection(self.predicates)) for w in
                              mt.datasources}

        results = {}

        for m in sources:
            for w in sources[m]:
                if w not in results:
                    results[w] = {}
                # print(w, m, sources[m][w])
                results[w]['filters'] = self.star['filters']
                results[w]['predicates'] = sources[m][w]
                results[w]['triples'] =[t for t in self.triples if utils.getUri(t.predicate, prefixes)[1:-1] in sources[m][w] or not t.predicate.constant]
                results[w].setdefault('rdfmts', []).append(m)

        return results


if __name__ == "__main__":
    query = """
            prefix iasis: <http://project-iasis.eu/vocab/> 
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
            prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 

            SELECT DISTINCT * WHERE {
                ?s a <http://project-iasis.eu/vocab/CGI> .
                ?s <http://project-iasis.eu/vocab/chromosome> ?chr .
                ?s  <http://project-iasis.eu/vocab/mutation_study_id> ?sid .
                ?s   <http://project-iasis.eu/vocab/mutation_strand> ?strand .
                ?s   <http://project-iasis.eu/vocab/mutation_isLocatedIn_gene> ?gene .
                ?gene <http://project-iasis.eu/vocab/label> ?label .

            }
        """

    query = """
        SELECT DISTINCT * WHERE {
              ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
              ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label .
              ?s <http://rdfs.org/ns/void#inDataset> ?dataset .
              ?s <http://purl.org/dc/terms/identifier> ?identifier .
              ?s <http://purl.org/dc/terms/title> ?title .
          }
    """
    configuration = OntarioConfiguration('/home/kemele/git/SDM/Ontario/configurations/biomed-configuration.json')
    dc = LakeCatalyst(query, configuration)

    quers = dc.decompose()
    import pprint
    pprint.pprint(quers)
    for s in quers[0]:
        meta = MetaCatalyst(quers[0][s], configuration)
        metadecomp = meta.decompose(dc.prefixes)
        pprint.pprint(metadecomp)
