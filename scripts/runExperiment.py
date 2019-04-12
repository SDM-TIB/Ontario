#!/usr/bin/env python3

__author__ = 'Kemele M. Endris'

from ontario.config import OntarioConfiguration
from ontario.mediator.Decomposer import LakeCatalyst
from ontario.mediator.Planner import LakePlanner

import getopt
from ontario.mediator import Catalyst

import sys
import os
import signal

from multiprocessing import Process, Queue, active_children

from time import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('ontario.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def runQuery(queryfile, configfile, res, printResults, planonly):

    query = open(queryfile).read()
    pos = queryfile.rfind("/")
    qu = queryfile[pos + 1:]
    pos2 = qu.rfind(".")

    if pos2 > -1:
        qu = qu[:pos2]
    global qname
    global t1
    global tn
    global c1
    global cn
    global dt
    global pt

    c1 = 0
    cn = 0
    t1 = -1
    tn = -1
    dt = -1
    global time1
    qname = qu
    endpointType = 'V'
    logger.info("Query: " + qname)

    #config = ConfigFile(configfile)
    config = OntarioConfiguration(configfile)
    time1 = time()

    dc = Catalyst(query, config)
    decomp = LakeCatalyst(dc.query, dc.config)
    dc.query = decomp.query
    decomposed_query = decomp.decompose()

    new_query = dc.catalyze(decomposed_query)

    # mdq = MediatorDecomposer(query, config, tempType, joinlocally)
    # new_query = mdq.decompose()

    dt = time() - time1

    if new_query is None or len(new_query) == 0:  # if the query could not be answered by the endpoints
        time2 = time() - time1
        t1 = time2
        tn = time2
        pt = time2
        printInfo()
        return

    # planner = MediatorPlanner(new_query, adaptive, contactSource, endpointType, config)
    # plan = planner.createPlan()

    pl = LakePlanner(dc.query, new_query, config)
    tree = pl.make_tree()
    plan = pl.make_plan()

    logger.info("Plan:")
    logger.info(plan)
    pt = time() - time1
    if planonly:
        print(plan)
        print("Decomposition and Planning Time: ", pt)
        exit(0)

    processqueue = Queue()

    p2 = Process(target=plan.execute, args=(res, processqueue,))
    p2.start()
    p3 = Process(target=conclude, args=(res, p2, printResults))
    p3.start()
    signal.signal(12, onSignal1)

    while True:
        if not p3.is_alive():
            if p2.is_alive():
                try:
                    os.kill(p2.pid, 9)
                except OSError as ex:
                    # print("Exception while terminating execution process", ex)
                    continue
            else:
                break


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def conclude(res, p2, printResults):
    signal.signal(12, onSignal2)
    global t1
    global tn
    global c1
    global cn

    ri = res.get()

    if printResults:
        if ri == "EOF":
            nexttime(time1)
            print("Empty set.")
            printInfo()
            return

        while ri != "EOF":
            cn = cn + 1
            if cn == 1:
                time2 = time() - time1
                t1 = time2
                c1 = 1

            print(ri)
            ri = res.get(True)

        nexttime(time1)
        printInfo()

    else:
        if ri == "EOF":
            nexttime(time1)
            printInfo()
            return

        while ri != "EOF":
            cn = cn + 1
            if cn == 1:
                time2 = time() - time1
                t1 = time2
                c1 = 1

            ri = res.get(True)

        nexttime(time1)
        printInfo()

    p2.terminate()


def nexttime(time1):
    global tn

    time2 = time() - time1
    tn = time2


def printInfo():
    global tn
    global cn
    global c1
    global t1
    global pt
    global dt
    if tn == -1:
        tn = time() - time1
    lr = (qname + "\t" + str(dt) + "\t" + str(pt) + "\t" + str(t1) + "\t" + str(tn) + "\t" + str(c1) + "\t" + str(cn))

    print(lr)
    logger.info(lr)


def onSignal1(s, stackframe):
    cs = active_children()
    for c in cs:
        try:
            os.kill(c.pid, s)
        except OSError as ex:
            continue
    sys.exit(s)


def onSignal2(s, stackframe):
    printInfo()
    sys.exit(s)


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:c:q:t:s:r:j:p:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    configfile = None
    queryfile = None
    # buffersize = 1638400
    tempType = "MULDER"
    isEndpoint = True
    plan = "b"
    adaptive = True
    withoutCounts = False
    printResults = False
    planonly = False
    joinlocally = True
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-c":
            configfile = arg
        elif opt == "-q":
            queryfile = arg
        elif opt == "-t":
            tempType = arg
        elif opt == "-s":
            isEndpoint = arg == "True"
        elif opt == '-r':
            printResults = eval(arg)
        elif opt == '-p':
            planonly = eval(arg)
        elif opt == '-j':
            joinlocally = eval(arg)

    if not configfile or not queryfile:
        usage()
        sys.exit(1)

    return (configfile, queryfile, tempType, isEndpoint, plan,
            adaptive, withoutCounts, printResults, planonly, joinlocally)


def usage():
    usage_str = ("Usage: {program} -c <config.json_file>  -q <query_file> -t "
                 + "<templateType> -s <isEndpoint>  -r <result_file.tsv>"
                 + "\n where \n<isEndpoint> - a boolean value "
                 + "\n<result_file.tsv> - an results.tsv"
                   "\n")
    print(usage_str.format(program=sys.argv[0]), )


def main(argv):
    res = Queue()
    time1 = time()
    (configfile, queryfile, buffersize, isEndpoint, plan, adaptive, withoutCounts, printResults, planonly,
     joinlocally) = get_options(argv[1:])
    try:
        runQuery(queryfile, configfile, res, printResults, planonly)
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main(sys.argv)
