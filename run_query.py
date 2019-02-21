from ontario.config import OntarioConfiguration
from ontario.mediator.Decomposer import LakeCatalyst
from ontario.mediator.Decomposer import MetaCatalyst
from ontario.mediator.Planner import LakePlanner

from ontario.config.model import DataSourceType
import ontario.sparql.utilities as utils
import getopt, sys
from ontario.mediator import Catalyst


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:q:c:p:s:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    query = None
    '''
    Supported output formats:
        - json (default)
        - nt
        - SPARQL-UPDATE (directly store to sparql endpoint)
    '''
    config = 'config/config.json'
    isstring = False
    planonly = False
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-q":
            query = arg
        elif opt == "-c":
            config = arg
        elif opt == '-p':
            planonly = eval(arg)
        elif opt == '-s':
            isstring = eval(arg)

    if not query:
        usage()
        sys.exit(1)

    if not isstring:
        query = open(query).read()

    return query, config, planonly


def usage():
    usage_str = ("Usage: {program} -q <query>  \n"
                 "-c <path/to/config.json> \n"
                 "-p <planOnly?> \n"
                 "where \n"
                 "\t<query> - SPARQL QUERY  \n"                 
                 "\t<path/to/config.json> - path to configuration file  \n"
                 "\t<planOnly?> - True to show only execution plan, False Otherwise \n")

    print(usage_str.format(program=sys.argv[0]),)


def finalize(processqueue):
    import os
    p = processqueue.get()

    while p != "EOF":
        try:
            os.kill(p, 9)
        except OSError as ex:
            pass
        p = processqueue.get()


if __name__ == "__main__":

    from time import time
    # query, config, planonly = get_options(sys.argv[1:])
    query = open('/home/kemele/git/MULDER/queries/polyweb/QE-3').read()
    # query = 'select distinct * where{?cnv <http://sels.insight.org/cancer-genomics/schema/disease> ?disease .	' \
    #         'FILTER (?disease = "Lung cancer" )} limit 10'
    config = 'scripts/polyweb-polystore.json'
    planonly = False
    ct = time()
    configuration = OntarioConfiguration(config)
    start = time()
    print("reading config finished! It took: (sec) ", (start-ct))     #, configuration.datasources)
    dc = Catalyst(query, configuration)
    # pprint.pprint(configuration.metadata)
    decomp = LakeCatalyst(dc.query, dc.config)
    dc.query = decomp.query
    decomposed_query = decomp.decompose()
    print("---------Decomposed query -------")
    import pprint
    pprint.pprint(decomposed_query)
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

    quers = dc.catalyze(decomposed_query)

    pprint.pprint(quers)
    dectime = time() - start
    print("Decomposition time: ", dectime)
    if len(quers) == 0:
        print('No results')
        exit()
    planstart = time()
    pl = LakePlanner(dc.query, quers, configuration)
    tree = pl.make_tree()

    print("0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0")
    print(pl.query)
    print("777777777777777777---PLAN----777777777777777777777777777")

    plan = pl.make_plan()
    pprint.pprint(plan)
    plantime = time() - planstart
    print("Decomposition time: ", dectime)
    print("Planning time: ", plantime)
    if planonly:
        exit()

    from multiprocessing import Queue

    out = Queue()
    processqueue = Queue()
    plan.execute(out, processqueue)
    r = out.get()
    processqueue.put('EOF')
    i = 0

    while r != 'EOF':
        pprint.pprint(r)
        r = out.get()
        i += 1

    exetime = time() - start
    print("total: ", i)

    print("Decomposition time: ", dectime)
    print("Planning time: ", plantime)
    print("total exe time:", exetime)
    finalize(processqueue)
