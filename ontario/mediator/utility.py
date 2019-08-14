
__author__ = 'Kemele M. Endris'

from ontario.sparql.parser.services import UnionBlock, JoinBlock
from ontario.model import DataSourceType
from ontario.mediator.Tree import *


def push_down_join(services):
    new_services = []
    endpoints = [s.endpoint for s in services if s.datasource.dstype == DataSourceType.SPARQL_ENDPOINT]
    starsendp = {}
    services_to_remove = []
    for e in set(endpoints):
        servs = list(set([s for s in services if s.endpoint == e]))
        starsendp[e] = servs
        others = []
        while len(servs) > 1:
            done = False
            l = servs.pop(0)  # heapq.heappop(pq)
            lpq = servs  # heapq.nsmallest(len(pq), pq)

            for i in range(0, len(servs)):
                r = lpq[i]

                if len(set(l.getVars()) & set(r.getVars())) > 0:
                    servs.remove(r)
                    new_service = Service(endpoint="<" + e + ">",
                                          triples=sorted(l.triples + r.triples),
                                          datasource=l.datasource,
                                          rdfmts=list(set(l.rdfmts + r.rdfmts)),
                                          star=l.star,
                                          filters=list(set(l.filters + r.filters)))
                    servs.append(new_service)
                    done = True
                    services_to_remove.append(l)
                    services_to_remove.append(r)

                    break
            if not done:
                others.append(l)
        if len(servs) == 1:
            new_services.append(servs[0])
            new_services.extend(others)
        elif others:
            new_services.extend(others)
        for s in services_to_remove:
            if s in services:
                services.remove(s)

    [services.append(s) for s in new_services if s not in services]
    return services


def decompose_block(BGP, filters, config, isTreeBlock=False):
    joinplans = []
    services = []
    filter_pushed = False
    non_match_filters = []
    ssqs = list(BGP['stars'].keys())
    ssqs = sorted(ssqs)
    for s in ssqs:
        star = BGP['stars'][s]
        dss = star['datasources']
        preds = star['predicates']
        sources = set()
        star_filters = get_filters(list(set(star['triples'])), filters)
        for ID, rdfmt in dss.items():
            for mt, mtpred in rdfmt.items():
                ppred = [p for p in preds if '?' not in p]
                if len(set(preds).intersection(
                        mtpred + ['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'])) == len(set(preds)) or len(ppred) == 0:
                    sources.add(ID)
                    break
        if len(sources) > 1:
            sources = sorted(sources)
            if isTreeBlock:
                elems = [JoinBlock([
                            makeBushyTree([
                                    Service(
                                           endpoint="<" + config.datasources[d].url + ">",
                                           triples=star['triples'],
                                           datasource=config.datasources[d],
                                           rdfmts=list(star['datasources'][d].keys()),
                                           star=star)],
                                star_filters)
                        ], filters=star_filters) for d in sources]
            else:
                elems = [JoinBlock([Service(endpoint="<" + config.datasources[d].url + ">",
                                            triples=star['triples'],
                                            datasource=config.datasources[d],
                                            rdfmts=star['rdfmts'],
                                            star=star,
                                            filters=get_filters(list(set(star['triples'])), filters))])
                         for d in sources]
            ubl = UnionBlock(elems)
            joinplans = joinplans + [ubl]
        elif len(sources) == 1:
            d = sources.pop()
            serv = Service(endpoint="<" + config.datasources[d].url + ">",
                           triples=star['triples'],
                           datasource=config.datasources[d],
                           rdfmts=star['rdfmts'],
                           star=star,
                           filters=star_filters)
            services.append(serv)

        if len(filters) == len(star_filters):
            filter_pushed = True
        else:
            non_match_filters = list(set(filters).difference(star_filters))
    services = push_down_join(services)
    if services and joinplans:
        joinplans = services + joinplans
    elif services:
        joinplans = services

    # joinplans = makeBushyTree(joinplans, filters)

    return joinplans, non_match_filters if not filter_pushed else []


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
