from ontario.sparql.parser import queryParser as qp
from ontario.sparql.parser.services import Service, Triple, Filter, Optional, UnionBlock, JoinBlock


def getVars(sg):

    s = []
    if not sg.subject.constant:
        s.append(sg.subject.name)
    if not sg.theobject.constant:
        s.append(sg.theobject.name)
    return s


def getPrefs(ps):
    prefDict = dict()
    for p in ps:
         pos = p.find(":")
         c = p[0:pos].strip()
         v = p[(pos+1):len(p)].strip()
         prefDict[c] = v
    return prefDict


def getUri(p, prefs):
    if "'" in p.name or '"' in p.name:
        return p.name
    hasPrefix = prefix(p)
    if hasPrefix:
        (pr, su) = hasPrefix
        n = prefs[pr]
        n = n[:-1]+su+">"
        return n
    return p.name


def prefix(p):
    s = p.name
    pos = s.find(":")
    if (not (s[0] == "<")) and pos > -1:
        return s[0:pos].strip(), s[(pos+1):].strip()

    return None


def get_preds( star):
    """
    Returns a set of predicates in a BGP/star-shaped subquery
    :param star: list of triple patterns
    :return: list of predicates
    """

    preds = [getUri(tr.predicate, prefixes)[1:-1] for tr in star if tr.predicate.constant]

    return preds


def get_pred_objs(star):
    """
    Returns a key value of predicate:object in a BGP/star-shaped subquery
    :param star: list of triple patterns
    :return: list of predicates
    """

    preds = {getUri(tr.predicate, prefixes)[1:-1]:
                 (tr.theobject.name if tr.theobject.constant else tr.theobject.name[1:])
             for tr in star if tr.predicate.constant}

    return preds


def bgp_stars(bgp):
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


def getStarsConnections(stars):
    """
    extracts links between star-shaped sub-queries
    :param stars: map of star-shaped sub-queries with its root (subject) {subject: [triples in BGP]}
    :return: map of star-shaped sub-query root name (subject) with its connected sub-queries via its object node.
     {subj1: [subjn]} where one of subj1's triple pattern's object node is connected to subject node of subjn
    """
    conn = dict()
    star_objs = {}
    for s in stars:
        objs = [t.theobject.name for t in stars[s] if not t.theobject.constant]
        star_objs[s] = objs
        conn[s] = {"SO": [], "OO": []}

    subjects = list(set(stars.keys()))
    checked = []
    for s in star_objs:
        connections = set(star_objs[s]).intersection(subjects)
        if len(connections) > 0:
            for c in connections:
                conn[c]['SO'].append(s)
        for s2 in star_objs:
            if s == s2:
                continue
            if s2+s not in checked and s+s2 not in checked:
                connections = set(star_objs[s]).intersection(star_objs[s2])
                if len(connections) > 0:
                    # for c in connections:
                    conn[s2]['OO'].append(s)
                    conn[s]['OO'].append(s2)
            checked.extend([s2+s, s+s2])

    return conn


def getMTsConnection(selectedmolecules, preds, relevant_mts):
    mcons = {}
    smolecules = [m for s in selectedmolecules for m in selectedmolecules[s]]
    for s in selectedmolecules:
        mols = selectedmolecules[s]
        for m in mols:
            mcons[m] = [n for n in relevant_mts[m].predicates \
                        for r in relevant_mts[m].predicates[n].ranges \
                        if r in smolecules and relevant_mts[m].predicates[n].predicate in preds]
    return mcons


def checkRDFTypeStatemnt(ltr, config):
    types = getRDFTypeStatement(ltr)
    typemols = {}
    for t in types:
        tt = getUri(t.theobject, prefixes)[1:-1]
        mt = config.metadata[tt]
        typemols[tt] = mt
    if len(types) > 0 and len(typemols) == 0:
        return None

    return typemols


def getRDFTypeStatement(ltr):
    types = []
    for t in ltr:
        if t.predicate.constant \
                and (t.predicate.name == "a"
                     or t.predicate.name == "rdf:type"
                     or t.predicate.name == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") \
                and t.theobject.constant:
            types.append(t)

    return types


def prune(star_conn, res_conn, selectedmolecules, stars,relevant_mts):
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
        sc = star_conn[s]['SO']

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
            connectingtp = [getUri(tp.predicate, prefixes)[1:-1]
                            for tp in stars[c] if tp.theobject.name == s]
            connectingtp = list(set(connectingtp))
            sm = selectedmolecules[s]
            for m in sm:
                srange = [p for r in relevant_mts[m].predicates for p in relevant_mts[m].predicates[r].ranges if relevant_mts[m].predicates[r].predicate in connectingtp]
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


def decomposeUnionBlock(ub, config):
    r = []
    for jb in ub.triples:
        pjb = decomposeJoinBlock(jb, config)
        if pjb:
            r.append(pjb)
    return r


def decomposeJoinBlock(jb, config):

    tl = []
    ol = []
    ijb = []
    ub = []
    fl = []
    for bgp in jb.triples:
        if isinstance(bgp, Triple):
            tl.append(bgp)
        elif isinstance(bgp, Filter):
            fl.append(bgp)
        elif isinstance(bgp, Optional):
            ubb = decomposeUnionBlock(bgp.bgg, config)
            ol.extend(ubb)
        elif isinstance(bgp, UnionBlock):
            pub = decomposeUnionBlock(bgp, config)
            if pub:
                ub.extend(pub)
        elif isinstance(bgp, JoinBlock):
            pub = decomposeJoinBlock(bgp, config)
            if pub:
                ijb.extend(pub)

    tl_bgp = {}
    if tl is not None:
        bgp_preds = get_preds(tl)

        stars = bgp_stars(tl)
        bgpstars = {}
        mtres = {}
        relevant_mts = {}
        for s in stars:
            spred = get_pred_objs(stars[s])
            bgpstars[s] = {}
            bgpstars[s]['triples'] = stars[s]
            bgpstars[s]['predicates'] = spred
            rdfmts = config.find_rdfmt_by_preds(spred)
            bgpstars[s]['rdfmts'] = list(rdfmts.keys())
            mtres[s] = bgpstars[s]['rdfmts']
            relevant_mts.update(rdfmts)

        star_conn = getStarsConnections(stars)
        res_conn = getMTsConnection(mtres, bgp_preds, relevant_mts)
        # res_conn = prune(star_conn, res_conn, mtres, stars, relevant_mts)
        #
        # for s in res_conn:
        #     bgpstars[s]['rdfmts'] = res_conn[s]


        tl_bgp['stars'] = bgpstars
        tl_bgp['bgp_predicates'] = bgp_preds
        tl_bgp['stars_conn'] = star_conn

    return {
        "BGP": tl_bgp,
        "Optional": ol,
        "JoinBlock": ijb,
        "UnionBlock": ub,
        "Filter": fl
    }


def select_molecule(decomp, config):
    for ub in decomp:
        BGP = ub['BGP']
        optional = ub['Optional']
        joinBlock = ub['JoinBlock']
        unionBlock = ub['UnionBlock']
        filter = ub['Filter']


if __name__ == '__main__':
    from pprint import pprint
    query = open("../queries/complexqueries/CQ7").read()
    query = qp.parse(query)
    prefixes = getPrefs(query.prefs)
    print(query)
    from tests.config import OntarioConfiguration

    configfile = '/home/kemele/git/SemanticDataLake/Ontario/scripts/lslod_rdf-rdb.json'
    configuration = OntarioConfiguration(configfile)

    print("\nDecomposition:\n")
    r = decomposeUnionBlock(query.body, configuration)


    planonly = False

    select_molecule(r, configuration)

    pprint(r)