
__author__ = 'Kemele M. Endris'

import urllib.parse as urlparse
from http import HTTPStatus
import requests
from multiprocessing import Queue
import logging


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger('mtupdate')
logger.setLevel(logging.INFO)
fileHandler = logging.FileHandler("{0}/{1}.log".format('.', 'ontario-update-log'))
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


def contactRDFSource(query, endpoint, outputqueue=Queue(), format="application/sparql-results+json"):
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]

    (server, path) = server.split("/", 1)
    # Formats of the response.
    json = format
    # Build the query and header.
    params = urlparse.urlencode({'query': query, 'format': json, 'timeout': 10000000})
    headers = {"Accept": "*/*", "Referer": endpoint, "Host": server}

    try:
        resp = requests.get(endpoint, params=params, headers=headers)
        if resp.status_code == HTTPStatus.OK:
            res = resp.text
            reslist = []
            if format != "application/sparql-results+json":
                return res

            try:
                res = res.replace("false", "False")
                res = res.replace("true", "True")
                res = eval(res)
            except Exception as ex:
                print("EX processing res", ex)

            if type(res) is dict:
                if "results" in res:
                    for x in res['results']['bindings']:
                        for key, props in x.items():
                            # Handle typed-literals and language tags
                            suffix = ''
                            if props['type'] == 'typed-literal':
                                if isinstance(props['datatype'], bytes):
                                    suffix = '' # "^^<" + props['datatype'].decode('utf-8') + ">"
                                else:
                                    suffix = '' # "^^<" + props['datatype'] + ">"
                            elif "xml:lang" in props:
                                suffix = '' # '@' + props['xml:lang']
                            try:
                                if isinstance(props['value'], bytes):
                                    x[key] = props['value'].decode('utf-8') + suffix
                                else:
                                    x[key] = props['value'] + suffix
                            except:
                                x[key] = props['value'] + suffix

                            if isinstance(x[key], bytes):
                                x[key] = x[key].decode('utf-8')
                        outputqueue.put(x)
                        reslist.append(x)
                    # reslist = res['results']['bindings']
                    return reslist, len(reslist)
                else:
                    outputqueue.put(res['boolean'])

                    return res['boolean'], 1

        else:
            print("Endpoint->", endpoint, resp.reason, resp.status_code, query)

    except Exception as e:
        print("Exception during query execution to", endpoint, ': ', e)

    return None, -2


def updateRDFSource(update, endpoint):
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]

    (server, path) = server.split("/", 1)
    # Build the header.
    headers = {"Accept": "*/*", "Referer": endpoint, "Host": server, "Content-type": "application/sparql-update"}

    try:
        resp = requests.post(endpoint, data=update, headers=headers)
        if resp.status_code == HTTPStatus.OK or resp.status_code == HTTPStatus.ACCEPTED or resp.status_code == HTTPStatus.NO_CONTENT:
            return True
        else:
            print("Update Endpoint->", endpoint, resp.reason, resp.status_code, update)
            logger.error(endpoint+" - " + str(resp.reason) + " - " + str(resp.status_code))
            logger.error("ERROR On: " + update)
    except Exception as e:
        print("Exception during update query execution to", endpoint, ': ', e, update)
        logger.error("Exception on update: " + endpoint + " " + str(e))
        logger.error("EXCEPTION ON: " + update)

    return False

