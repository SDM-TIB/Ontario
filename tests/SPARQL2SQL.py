from ontario.sparql.parser import queryParser as qp
from ontario.sparql.parser.services import Service, Triple, Filter, Optional, UnionBlock, JoinBlock
from ontario.mediator.Tree import makeBushyTree

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


def get_preds(star, prefixes):
    """
    Returns a set of predicates in a BGP/star-shaped subquery
    :param star: list of triple patterns
    :return: list of predicates
    """

    preds = [getUri(tr.predicate, prefixes)[1:-1] for tr in star if tr.predicate.constant]

    return preds


def get_pred_objs(star, prefixes):
    """
    Returns a key value of predicate:object in a BGP/star-shaped subquery
    :param star: list of triple patterns
    :return: list of predicates
    """

    preds = {getUri(tr.predicate, prefixes)[1:-1]:
                 (getUri(tr.theobject, prefixes) if tr.theobject.constant else tr.theobject.name)
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
            mcons[m] =[]
            [mcons[m].extend(relevant_mts[m].predicates[n].ranges) for n in relevant_mts[m].predicates \
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
        return {}

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

    # for s in selectedmolecules:
    #     sc = star_conn[s]['SO']
    #
    #     for sm in selectedmolecules[s]:
    #         smolink = res_conn[sm]
    #
    #         for c in sc:
    #             cmols = selectedmolecules[c]
    #             nms = [m for m in smolink if m in cmols]
    #             if len(nms) > 0:
    #                 res[s].append(sm)
    #                 res[c].extend(nms)

    # check predicate level connections
    newfilteredonly = {}
    for s in res:
        sc = [c for c in star_conn if s in star_conn[c]['SO']]
        for c in sc:
            connectingtp = [getUri(tp.predicate, prefixes)[1:-1]
                            for tp in stars[s] if tp.theobject.name == c]
            connectingtp = list(set(connectingtp))
            sm = selectedmolecules[s]
            for m in sm:
                srange = [p for r in relevant_mts[m].predicates
                            for p in relevant_mts[m].predicates[r].ranges
                            if relevant_mts[m].predicates[r].predicate in connectingtp]
                srange = list(set(srange).intersection(selectedmolecules[c]))
                if len(srange) == 0:
                    selectedmolecules[s].remove(m)
                if c in newfilteredonly:
                    newfilteredonly[c].extend(srange)
                else:
                    newfilteredonly[c] = srange
                newfilteredonly[c] = list(set(newfilteredonly[c]))

    already_checked = []
    for s in res:
        sc = [c for c in star_conn if s in star_conn[c]['SO']]
        for c in sc:
            if s+c in already_checked or c+s in already_checked:
                continue

            already_checked.extend([s+c, c+s])
            if c in newfilteredonly:
                c_newfilter = newfilteredonly[c].copy()
            else:
                c_newfilter = selectedmolecules[c].copy()
                newfilteredonly[c] = selectedmolecules[c].copy()
            if s in newfilteredonly:
                s_newfilter = newfilteredonly[s].copy()
            else:
                s_newfilter = selectedmolecules[s].copy()
                newfilteredonly[s] = selectedmolecules[s].copy()
            for m in s_newfilter:
                con = res_conn[m]
                if len(con) == 0:
                    continue
                new_res = list(set(con).intersection(c_newfilter))
                if len(new_res) == 0:
                    newfilteredonly[s].remove(m)

            for m in c_newfilter:
                con = res_conn[m]
                if len(con) == 0:
                    continue
                new_res = list(set(con).intersection(s_newfilter))
                if len(new_res) == 0:
                    newfilteredonly[c].remove(m)

    for s in newfilteredonly:
        res[s] = list(set(newfilteredonly[s]))

    for s in res:
        if len(res[s]) == 0:
            res[s] = selectedmolecules[s]
        res[s] = list(set(res[s]))
    return res


def decomposeUnionBlock(ub, config, prefixes):
    r = []
    for jb in ub.triples:
        pjb = decomposeJoinBlock(jb, config, prefixes)
        if pjb:
            r.append(pjb)
    return r


def decomposeJoinBlock(jb, config, prefixes):

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
            ubb = decomposeUnionBlock(bgp.bgg, config, prefixes)
            ol.extend(ubb)
        elif isinstance(bgp, UnionBlock):
            pub = decomposeUnionBlock(bgp, config, prefixes)
            if pub:
                ub.extend(pub)
        elif isinstance(bgp, JoinBlock):
            pub = decomposeJoinBlock(bgp, config, prefixes)
            if pub:
                ijb.extend(pub)

    tl_bgp = {}
    if tl is not None:
        bgp_preds = get_preds(tl, prefixes)
        stars = bgp_stars(tl)
        bgpstars, star_conn, mt_conn = decompose_SSQ(stars, bgp_preds, config, prefixes)
        tl_bgp['stars_conn'] = star_conn
        tl_bgp['mts_conn'] = mt_conn

        tl_bgp['stars'] = bgpstars
        tl_bgp['bgp_predicates'] = bgp_preds

    return {
        "BGP": tl_bgp,
        "Optional": ol,
        "JoinBlock": ijb,
        "UnionBlock": ub,
        "Filter": fl
    }


def decompose_SSQ(stars, bgp_preds, config, prefixes):
    bgpstars = {}
    mtres = {}
    relevant_mts = {}
    for s in stars:
        spred = get_pred_objs(stars[s], prefixes)
        bgpstars[s] = {}
        bgpstars[s]['triples'] = stars[s]
        bgpstars[s]['predicates'] = spred
        types = checkRDFTypeStatemnt(stars[s], config)
        if len(types) > 0:
            rdfmts = types
        else:
            rdfmts = config.find_rdfmt_by_preds(spred)

        bgpstars[s]['rdfmts'] = list(rdfmts.keys())
        mtres[s] = bgpstars[s]['rdfmts']
        relevant_mts.update(rdfmts)
    star_conn = getStarsConnections(stars)
    mt_conn = getMTsConnection(mtres, bgp_preds, relevant_mts)
    res = prune(star_conn, mt_conn, mtres, stars, relevant_mts)

    for s in res:
        bgpstars[s]['rdfmts'] = res[s]

    for s in res:
        datasources = {}
        for m in res[s]:
            for d in config.metadata[m].datasources:
                dspreds = config.metadata[m].datasources[d]
                preds = list(set(bgpstars[s]['predicates']).intersection(dspreds))
                if len(preds) > 0:
                    datasources.setdefault(d, {}).setdefault(m, []).extend(preds)
        bgpstars[s]['datasources'] = datasources

    return bgpstars, star_conn,mt_conn


def get_filters(triples, filters):
    result = []
    t_vars = []
    for t in triples:
        t_vars.extend(t.getVars())

    for f in filters:
        f_vars = f.getVars()
        if len(set(f_vars).intersection(t_vars)) == len(set(f_vars)):
            result.append(f)

    return result


def decompose_block(BGP, config, filters):
    joinplans = []
    services = []
    for s, star in BGP['stars'].items():
        dss = star['datasources']
        preds = star['predicates']
        sources = set()
        for ID, rdfmt in dss.items():
            for mt, mtpred in rdfmt.items():
                if len(set(preds).intersection(mtpred + ['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'])) == len(
                        set(preds)):
                    print(s, '-> source selected:', ID)
                    sources.add(ID)
                    break
        if len(sources) > 1:
            elems = [JoinBlock([Service(endpoint="<" + config.datasources[d].url + ">",
                                        triples=list(set(star['triples'])),
                                        datasource=config.datasources[d],
                                        rdfmts=star['rdfmts'],
                                        star=star,
                                        filters=get_filters(list(set(star['triples'])), filters))])
                                                for d in sources]
            ubl = UnionBlock(elems)
            joinplans = joinplans + [ubl]
        else:
            d = sources.pop()
            serv = Service(endpoint="<" + config.datasources[d].url + ">",
                           triples=list(set(star['triples'])),
                           datasource=config.datasources[d],
                           rdfmts=star['rdfmts'],
                           star=star,
                           filters=get_filters(list(set(star['triples'])), filters))
            services.append(serv)

    if services and joinplans:
        joinplans = services + joinplans
    elif services:
        joinplans = services

    return joinplans


def create_decomposed_query(decomp, config, pushdownssqjoins=False):
    unionblocks = []
    sblocks = []
    opblocks = []
    for ub in decomp:
        BGP = ub['BGP']
        joinplans = decompose_block(BGP, config, ub['Filter'])

        if len(ub['JoinBlock']) > 0:
            joinBlock = create_decomposed_query(ub['JoinBlock'], config, pushdownssqjoins)
            sblocks.append(JoinBlock(joinBlock))
        if len(ub['UnionBlock']) > 0:
            unionBlock = create_decomposed_query(ub['UnionBlock'], config, pushdownssqjoins)
            sblocks.append(UnionBlock(unionBlock))

        if len(ub['Optional']) > 0:
            opblocks.append(Optional(UnionBlock(create_decomposed_query(ub['Optional'], config, pushdownssqjoins))))

        gp = sblocks + joinplans + opblocks
        gp = UnionBlock([JoinBlock(gp)])
        unionblocks.append(gp)

    return unionblocks


def tree_block(BGP, config, filters):
    joinplans = []
    services = []
    filter_pushed = False
    non_match_filters = []
    for s, star in BGP['stars'].items():
        dss = star['datasources']
        preds = star['predicates']
        sources = set()
        star_filters = get_filters(list(set(star['triples'])), filters)
        for ID, rdfmt in dss.items():
            for mt, mtpred in rdfmt.items():
                if len(set(preds).intersection(mtpred + ['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'])) == len(
                        set(preds)):
                    sources.add(ID)
                    break
        if len(sources) > 1:

            elems = [JoinBlock([
                                makeBushyTree([Service(endpoint="<" + config.datasources[d].url + ">",
                                        triples=list(set(star['triples'])),
                                        datasource=config.datasources[d],
                                        rdfmts=star['rdfmts'],
                                        star=star)], star_filters)],
                               filters=star_filters)
                     for d in sources]

            ubl = UnionBlock(elems)
            joinplans = joinplans + [ubl]
        else:
            d = sources.pop()
            serv = Service(endpoint="<" + config.datasources[d].url + ">",
                           triples=list(set(star['triples'])),
                           datasource=config.datasources[d],
                           rdfmts=star['rdfmts'],
                           star=star,
                           filters=star_filters)
            services.append(serv)

        if len(filters) == len(star_filters):
            filter_pushed = True
        else:
            non_match_filters = list(set(filters).difference(star_filters))

    if services and joinplans:
        joinplans = services + joinplans
    elif services:
        joinplans = services

    # joinplans = makeBushyTree(joinplans, filters)

    return joinplans, non_match_filters if not filter_pushed else []


def create_plan_tree(decomp, config):
    unionblocks = []
    sblocks = []
    opblocks = []
    for ub in decomp:
        BGP = ub['BGP']
        joinplans, non_match_filters = tree_block(BGP, config, ub['Filter'])

        if len(ub['JoinBlock']) > 0:
            joinBlock = create_plan_tree(ub['JoinBlock'], config)
            sblocks.append(JoinBlock(joinBlock))
        if len(ub['UnionBlock']) > 0:
            unionBlock = create_plan_tree(ub['UnionBlock'], config)
            sblocks.append(UnionBlock(unionBlock))

        if len(ub['Optional']) > 0:
            opblocks.append(Optional(UnionBlock(create_plan_tree(ub['Optional'], config))))

        bgp_triples = []
        [bgp_triples.extend(BGP['stars'][s]['triples']) for s in BGP['stars']]
        gp = makeBushyTree(joinplans + sblocks, get_filters(bgp_triples, non_match_filters))

        gp = [gp] + opblocks

        gp = JoinBlock(gp)
        unionblocks.append(gp)

    return unionblocks


if __name__ == '__main__':
    from pprint import pprint
    query = open("../queries/simpleQueries/SQ5").read()
    # query = open("../queries/complexqueries/CQ2").read()
    query = qp.parse(query)
    prefixes = getPrefs(query.prefs)

    from tests.config import OntarioConfiguration
    from ontario.mediator.Planner import LakePlanner
    from multiprocessing import Queue
    from time import time
    from pprint import pprint

    configfile = '/home/kemele/git/SemanticDataLake/Ontario/scripts/lslod_rdf-rdb.json'
    #configfile = '../configurations/lslod-rdf-distributed.json'
    configuration = OntarioConfiguration(configfile)
    start = time()
    print("\nDecomposition:\n")
    r = decomposeUnionBlock(query.body, configuration, prefixes)

    planonly = False
    pprint(r)
    print(query)
    unionblocks = create_decomposed_query(r, configuration)
    query.body = UnionBlock(unionblocks)
    print(query)
    dectime = time() - start
    planstart = time()
    print("Tree : -----------------")
    query.body = UnionBlock(create_plan_tree(r, configuration))
    print(query)

    pl = LakePlanner(query, r, configuration)

    plan = pl.make_plan()
    print(plan)
    plantime = time() - planstart

    out = Queue()
    processqueue = Queue()
    plan.execute(out, processqueue)
    r = out.get()
    processqueue.put('EOF')
    i = 0

    while r != 'EOF':
        # pprint(r)
        r = out.get()
        i += 1

    exetime = time() - start
    print("total: ", i)

    print("Decomposition time: ", dectime)
    print("Planning time: ", plantime)
    print("total exe time:", exetime)

