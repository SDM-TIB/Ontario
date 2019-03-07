from ontario.sparql.parser.services import Service, Triple, Filter, Optional, UnionBlock, JoinBlock
from ontario.model import DataSourceType
from ontario.mediator.Tree import *


def push_down_join(services):
    new_services = []
    services_to_remove = []
    endpoints = [s.endpoint for s in services if s.datasource.dstype == DataSourceType.SPARQL_ENDPOINT]

    for e in set(endpoints):
        servs = set([s for s in services if s.endpoint == e])
        if len(servs) > 1:
            joinables = []
            for s in servs:
                for s2 in servs:
                    if s == s2:
                        continue
                    if s in services_to_remove and s2 in services_to_remove:
                        continue
                    if len(set(s.getVars()) & set(s2.getVars())) > 0:
                        new_service = Service(endpoint="<" + e + ">",
                                              triples=list(set(s.triples + s2.triples)),
                                              datasource=s.datasource,
                                              rdfmts=list(set(s.rdfmts + s2.rdfmts)),
                                              star=s.star,
                                              filters=list(set(s.filters + s2.filters)))
                        joinables.append(new_service)
                        services_to_remove.extend([s, s2])
                    elif s2 not in services_to_remove:
                        ext = []
                        for j in set(joinables):
                            if len(set(j.getVars()) & set(s2.getVars())) > 0:
                                new_service = Service(endpoint="<" + e + ">",
                                                      triples=list(set(j.triples + s2.triples)),
                                                      datasource=j.datasource,
                                                      rdfmts=list(set(j.rdfmts + s2.rdfmts)),
                                                      star=j.star,
                                                      filters=list(set(j.filters + s2.filters)))
                                joinables.append(new_service)
                                services_to_remove.extend([s2])
                                ext.append(j)
                        if len(ext) > 0:
                            for j in ext:
                                joinables.remove(j)

            new_services.extend(joinables)
    if len(services_to_remove) > 0:
        for s in set(services_to_remove):
            services.remove(s)
        services = services + list(set(new_services))

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
                if len(set(preds).intersection(
                        mtpred + ['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'])) == len(set(preds)):
                    sources.add(ID)
                    break
        if len(sources) > 1:
            sources = sorted(sources)
            if isTreeBlock:
                elems = [JoinBlock([
                            makeBushyTree([
                                    Service(
                                           endpoint="<" + config.datasources[d].url + ">",
                                           triples=list(set(star['triples'])),
                                           datasource=config.datasources[d],
                                           rdfmts=star['rdfmts'],
                                           star=star)],
                                star_filters)
                        ], filters=star_filters) for d in sources]
            else:
                elems = [JoinBlock([Service(endpoint="<" + config.datasources[d].url + ">",
                                            triples=list(set(star['triples'])),
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
