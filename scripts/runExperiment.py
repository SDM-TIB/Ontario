#!/usr/bin/env python3

__author__ = 'Kemele M. Endris'

from ontario.config import OntarioConfiguration
from ontario.mediator.Planner import MetaWrapperPlanner
from ontario.mediator.Decomposer import *

import getopt
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
    logger.info("Query: " + qname)

    configuration = OntarioConfiguration(configfile)
    time1 = time()

    mc = MediatorCatalyst(query, configuration)
    r = mc.decompose()
    new_query = mc.query
    dt = time() - time1

    if len(r) == 0 or new_query is None:  # if the query could not be answered by the endpoints
        time2 = time() - time1
        t1 = time2
        tn = time2
        pt = time2
        printInfo()
        return

    mwp = MetaWrapperPlanner(new_query, r, configuration)
    plan = mwp.make_plan()

    logger.info("Plan:")
    logger.info(plan)
    if isinstance(plan, list):
        time2 = time() - time1
        t1 = time2
        tn = time2
        pt = time2
        printInfo()
        return
    if planonly:
        print(plan)
        return
    pt = time() - time1
    p2 = Process(target=plan.execute, args=(res,))
    p2.start()
    p3 = Process(target=conclude, args=(res, p2, printResults,))
    p3.start()
    signal.signal(12, onSignal1)

    while True:
        if p2.is_alive() and not p3.is_alive():
            try:
                os.kill(p2.pid, 9)
            except Exception as ex:
                continue
            break
        elif not p2.is_alive() and not p3.is_alive():
            break


def conclude(res, p2, printResults, traces=False):
    signal.signal(12, onSignal2)
    global t1
    global tn
    global c1
    global cn
    global time1

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
            if traces:
                nexttime(time1)
                # printTraces()
            ri = res.get(True)

        nexttime(time1)
        printInfo()

    else:
        if ri == "EOF":
            nexttime(time1)
            # printTraces()
            printInfo()
            return

        while ri != "EOF":
            cn = cn + 1
            if cn == 1:
                time2 = time() - time1
                t1 = time2
                c1 = 1

            if traces:
                nexttime(time1)
                # printTraces()
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

    if tn == -1:
        tn = time() - time1
    lr = (qname + "\t" + str(dt) + "\t" + str(pt) + "\t" + str(t1) + "\t" + str(tn) + "\t" + str(c1) + "\t" + str(cn))

    print(lr)

    logger.info(lr)


# def printTraces():
#     global tn
#     global resulttrace
#     global cn
#
#     if tn == -1:
#         tn = time() - time1
#     l = (qname + "," + str(cn) + "," + str(tn))
#
#     resulttrace.write('\n' + l)


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
        elif opt == '-p':
            planonly = eval(arg)
        elif opt == '-j':
            joinlocally = eval(arg)
        elif opt == '-r':
            printResults = eval(arg)

    if not configfile or not queryfile:
        usage()
        sys.exit(1)

    return (configfile, queryfile, tempType, isEndpoint, plan, planonly,
            adaptive, withoutCounts, printResults, joinlocally)


def usage():
    usage_str = "Usage: {program} -c <config.json_file>  -q <query_file> -r <PrintResults?>\n Where: <config.json_file> - " \
                "path to RDF-MT configs\n " \
                "<query_file> - path to SPARQL query file \n " \
                "<PrintResults?> - boolean value (True - print rows / False - do not print rows) "
    print(usage_str.format(program=sys.argv[0]), )


def main(argv):
    res = Queue()
    time1 = time()
    (configfile, queryfile, buffersize, isEndpoint, plan, planonly, adaptive, withoutCounts, printResults,
     joinlocally) = get_options(argv[1:])
    try:
        runQuery(queryfile, configfile, res, printResults, planonly)
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main(sys.argv)

