from flask import (
    Blueprint, flash, g, redirect, render_template, session, Response, send_from_directory, request, url_for
)
from werkzeug.exceptions import abort
import os
from flask.json import jsonify
from time import time
import traceback
from multiprocessing import Process, Queue, active_children
import hashlib
import logging

from ontario.config import OntarioConfiguration
from ontario.mediator.Planner import MetaWrapperPlanner
from ontario.mediator.Decomposer import *

bp = Blueprint('query', __name__, url_prefix='/')


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

resqueues = {}

if 'CONFIG_FILE' in os.environ:
    configfile = os.environ['CONFIG_FILE']
else:
    configfile = '/configurations/config-demo.json'
configuration = OntarioConfiguration(configfile)


def finalize(processqueue):
    p = processqueue.get()
    while p != "EOF":
        try:
            os.kill(p, 9)
        except OSError as ex:
            print(ex)
            pass
        p = processqueue.get()


@bp.route("/nextresult", methods=['POST', 'GET'])
def get_next_result():
    vars = session['vars']
    start = session['start']
    first = session['first']
    total = session['total']
    if 'hashquery' in session and session['hashquery'] in resqueues:
        output = resqueues[session['hashquery']]['output']
        process = resqueues[session['hashquery']]['process']
    else:
        # total = time() - start
        return jsonify(execTime=total, firstResult=first, totalRows=1, result="EOF", error="Already finished")
    try:
        r = output.get()
        # total = time() - start
        if r == "EOF":
            finalize(process)
            del resqueues[session['hashquery']]
            del session['hashquery']

        return jsonify(vars=vars, result=r, execTime=total, firstResult=first, totalRows=1)
    except Exception as e:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        emsg = repr(traceback.format_exception(exc_type, exc_value,
                                               exc_traceback))
        logger.error("Exception while returning incremental results .. " + emsg)
        print("Exception: ")
        import pprint
        pprint.pprint(emsg)
        total = time() - start
        return jsonify(execTime=total, firstResult=first, totalRows=1, result= [], error= str(emsg))


@bp.route("/sparql", methods=['POST', 'GET'])
def sparql():
    if request.method == 'GET' or request.method == 'POST':
        try:
            query = request.args.get("query", '')
            # configfile = request.args.get('config', '/configurations/config-demo.json')
            isblocking = request.args.get('blocking', 1)
            if isblocking == 1:
                isblocking = True
            else:
                isblocking = False
            query = query.replace('\n', ' ').replace('\r', ' ')
            print('query:', query)
            logger.info(query)
            if not isblocking:
                session['hashquery'] = str(hashlib.md5(query.encode()).hexdigest())

            if query is None or len(query) == 0:
                return jsonify({"result": [], "error": "cannot read query"})

            output = Queue()
            variables, res, start, total, first, i, processqueue, alltriplepatterns = execute_query(query, output, isblocking)
            if not isblocking:
                resqueues[session['hashquery']] = {"output": output, "process": processqueue}
            if res is None or len(res) == 0:
                if not isblocking:
                    del resqueues[session['hashquery']]
                    del session['hashquery']
                return jsonify(vars=variables, result=[], execTime=total, firstResult=first, totalRows=-1)

            if variables is None:
                print('no results during decomposition', query)
                if not isblocking:
                    del resqueues[session['hashquery']]
                return jsonify({"result": [],
                                "error": "cannot execute query on this federation. No matching molecules found"})
            if not isblocking:
                session['start'] = start
                session['vars'] = variables
                session['first'] = first
                session['total'] = total
            processqueue.put("EOF")
            triplepatterns = []
            for t in alltriplepatterns:
                triplepatterns.append({
                    "s": t.subject.name,
                    'p': t.predicate.name,
                    'o': t.theobject.name
                })
            return jsonify(vars=variables, querytriples=triplepatterns, result=res, execTime=total, firstResult=first, totalRows=i)
        except Exception as e:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            emsg = repr(traceback.format_exception(exc_type, exc_value,
                                                   exc_traceback))
            logger.error("Exception  " + emsg)
            print ("Exception: ", e)
            import pprint
            pprint.pprint(emsg)
            return jsonify({"result": [], "error": str(emsg)})
    else:
        return jsonify({"result": [], "error": "Invalid HTTP method used. Use GET "})


def execute_query(query, output=Queue(), isblocking=True):


    logger.info("config loaded!")
    start = time()

    # dc = MediatorDecomposer(query, configuration)
    # quers = dc.decompose()

    mc = MediatorCatalyst(query, configuration)
    r = mc.decompose()
    new_query = mc.query

    logger.info("Mediator Decomposer: \n" + str(r))
    logger.info(new_query)
    if len(r) == 0 or new_query is None:
        logger.info("Query decomposer returns None")
        return None, None, 1, 1, 1, 0,None, []

    res = []
    # planner = MediatorPlanner(quers, True, clm, None, configuration)
    planner = MetaWrapperPlanner(new_query, r, configuration)
    plan = planner.make_plan()

    logger.info("Mediator Planner: \n" + str(plan))
    logger.info(plan)
    if isinstance(plan, list):
        logger.info("Query plan returns empty")
        return None, None, 1, 1, 1, 0, None, []

    processqueue = Queue()
    plan.execute(output, processqueue)

    i = 0
    r = output.get()
    if mc.query.query_type == 0:
        variables = [p.name[1:] for p in mc.query.args if not isinstance(p, Triple)]
    else:
        variables = []

    first = time() - start
    if isblocking:
        while r != "EOF":
            logger.info(r)
            if mc.query.query_type == 0 and ( len(variables) == 0 or (len(variables) == 1 and variables[0] == '*')):
                variables = [k for k in r.keys()]
            res.append(r)
            r = output.get()
            i += 1
        if r == "EOF":
            logger.info("END of results ....")
            session['total'] = time() - start
    else:
        if r == "EOF":
            logger.info("END of results ....")
            session['total'] = time() - start
        else:
            if mc.query.query_type == 0 and ( len(variables) == 0 or (len(variables) == 1 and variables[0] == '*')):
                variables = [k for k in r.keys()]
            res.append(r)
            i += 1
    total = time() - start
    return variables, res, start, total, first, i, processqueue, []
