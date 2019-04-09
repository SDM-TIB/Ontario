
__author__ = 'Kemele M. Endris'

if __name__ == '__main__':
    from pprint import pprint
    query = open("queries/simpleQueries/SQ10").read()
    # query = open("queries/complexqueries/CQ9").read()

    from ontario.config import OntarioConfiguration
    from ontario.mediator.Planner import MetaWrapperPlanner
    from ontario.mediator.Decomposer import *
    from multiprocessing import Queue
    from time import time
    from pprint import pprint

    configfile = 'configurations/lslod-rdf-distributed.json'
    # configfile = 'configurations/lslod_rdf_rdb.json'
    # configfile = 'configurations/lslod_rdf_rdb_tsv_json.json'
    # configfile = 'configurations/lslod_tsv_rdb.json'
    configuration = OntarioConfiguration(configfile)
    start = time()
    print("\nDecomposition:\n")

    mc = MediatorCatalyst(query, configuration)
    r = mc.decompose()
    query = mc.query
    # r = decomposeUnionBlock(query.body, configuration, prefixes)
    planonly = False
    pprint(r)
    print("Decomposition : -----------------")
    print(query)
    dectime = time() - start
    planstart = time()
    print("Plan : -----------------")

    mwp = MetaWrapperPlanner(query, r, configuration)
    # query.body = UnionBlock(mwp.make_tree())
    # query.body = UnionBlock(create_plan_tree(r, configuration))

    # pl = LakePlanner(query, r, configuration)

    plan = mwp.make_plan()
    print(plan)
    plantime = time() - planstart
    # exit()
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

