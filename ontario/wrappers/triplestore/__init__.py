from __future__ import division
__author__ = 'kemele'

import urllib
import urllib.parse as urlparse
import http.client as htclient
from http import HTTPStatus
import requests
from multiprocessing import Process, Queue, active_children


class RDFStore(object):
    def __init__(self, datasource, config):
        self.url = datasource.url
        self.datasource = datasource
        self.config = config

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=-1):

        server = self.url

        referer = server
        server = server.split("http://")[1]
        if '/' in server:
            (server, path) = server.split("/", 1)
        else:
            path = ""
        host_port = server.split(":")
        port = 80 if len(host_port) == 1 else host_port[1]
        card = 0
        if limit == -1:
            b, card = contactSourceAux(referer, server, path, port, query, queue)
        else:
            # Contacts the datasource (i.e. real endpoint) incrementally,
            # retreiving partial result sets combining the SPARQL sequence
            # modifiers LIMIT and OFFSET.

            # Set up the offset.
            offset = 0

            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                b, cardinality = contactSourceAux(referer, server, path, port, query_copy, queue)
                card += cardinality
                if cardinality < limit:
                    break

                offset = offset + limit

        # Close the queue
        queue.put("EOF")
        return b


def contactSourceAux(referer, server, path, port, query, queue):

    # Setting variables to return.
    b = None
    reslist = 0

    # Formats of the response.
    json = "application/sparql-results+json"
    if '0.0.0.0' in server:
        server = server.replace('0.0.0.0', 'localhost')
    # Build the query and header.
    # params = urllib.urlencode({'query': query})
    params = urlparse.urlencode({'query': query, 'format': json})
    headers = {"Accept": "*/*", "Referer": referer, "Host": server}

    js = "application/sparql-results+json"
    params = {'query': query, 'format': js}
    headers = {"User-Agent": "mulder", "Accept": js}

    resp = requests.get(referer, params=params, headers=headers)
    if resp.status_code == HTTPStatus.OK:
        res = resp.text.replace("false", "False")
        res = res.replace("true", "True")
        res = eval(res)
        reslist = 0

        if type(res) == dict:
            b = res.get('boolean', None)

            if 'results' in res:
                # print "raw results from endpoint", res
                for x in res['results']['bindings']:
                    for key, props in x.items():
                        # Handle typed-literals and language tags
                        suffix = ''
                        if props['type'] == 'typed-literal':
                            if isinstance(props['datatype'], bytes):
                                suffix = "^^<" + props['datatype'].decode('utf-8') + ">"
                            else:
                                suffix = "^^<" + props['datatype'] + ">"
                        elif "xml:lang" in props:
                            suffix = '@' + props['xml:lang']
                        try:
                            if isinstance(props['value'], bytes):
                                x[key] = props['value'].decode('utf-8') + suffix
                            else:
                                x[key] = props['value'] + suffix
                        except:
                            x[key] = props['value'] + suffix

                    queue.put(x)
                    reslist += 1
                # Every tuple is added to the queue.
                #for elem in reslist:
                    # print elem
                    #queue.put(elem)

        else:
            print ("the source " + str(server) + " answered in " + res.getheader("content-type") + " format, instead of"
                   + " the JSON format required, then that answer will be ignored")
    # print "b - ", b
    # print server, query, len(reslist)

    # print "Contact Source returned: ", len(reslist), ' results'
    return b, reslist

