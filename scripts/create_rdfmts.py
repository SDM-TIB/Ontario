#!/usr/bin/env python3.5

import getopt, sys
from pprint import pprint
import rdflib
from enum import Enum
import json
import logging
import time
import urllib.parse as urlparse
import http.client as htclient
from http import HTTPStatus
import requests


xsd = "http://www.w3.org/2001/XMLSchema#"
owl = ""
rdf = ""
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
mtonto = "http://tib.eu/dsdl/ontario/ontology/"
mtresource = "http://tib.eu/dsdl/ontario/resource/"


prefixes = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
    prefix rev: <http://purl.org/stuff/rev#> 
    prefix schema: <http://schema.org/> 
    prefix xsd: <http://www.w3.org/2001/XMLSchema#> 
    prefix base: <http://tib.de/ontario/mapping#> 
    prefix iasis: <http://project-iasis.eu/vocab/> 
    prefix hydra: <http://www.w3.org/ns/hydra/core#> 
"""

metas = ['http://www.w3.org/ns/sparql-service-description',
         'http://www.openlinksw.com/schemas/virtrdf#',
         'http://www.w3.org/2000/01/rdf-schema#',
         'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
         'http://purl.org/dc/terms/Dataset',
         'http://bio2rdf.org/dataset_vocabulary:Endpoint',
         'http://www.w3.org/2002/07/owl#',
         "http://purl.org/goodrelations/",
         'http://www.ontologydesignpatterns.org/ont/d0.owl#',
         'http://www.wikidata.org/',
         'http://dbpedia.org/ontology/Wikidata:',
         'http://dbpedia.org/class/yago/',
         "http://rdfs.org/ns/void#",
         'http://www.w3.org/ns/dcat',
         'http://www.w3.org/2001/vcard-rdf/',
         'http://www.ebusiness-unibw.org/ontologies/eclass',
         "http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset",
         'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/',
         'nodeID://']


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger("rdfmts")
logger.setLevel(logging.INFO)
fileHandler = logging.FileHandler("{0}/{1}.log".format('.', 'ontario-rdfmts-log'))
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


class DataSourceType(Enum):
    SPARQL_ENDPOINT = "SPARQL_Endpoint"
    MONGODB = "MongoDB"
    NEO4J = "Neo4j"
    MYSQL = "MySQL"
    SPARK_CSV = "SPARK_CSV"
    SPARK_TSV = "SPARK_TSV"
    SPARK_JSON = "SPARK_JSON"
    SPARK_XML = "SPARK_XML"
    HADOOP_CSV = "HADOOP_CSV"
    HADOOP_TSV = "HADOOP_TSV"
    HADOOP_JSON = "HADOOP_JSON"
    HADOOP_XML = "HADOOP_XML"
    REST_SERVICE = "REST_Service"
    LOCAL_CSV = "LOCAL_CSV"
    LOCAL_TSV = "LOCAL_TSV"
    LOCAL_JSON = "LOCAL_JSON"
    LOCAL_XML = "LOCAL_XML"


def read_config(filename):
    with open(filename, "r", encoding='utf8') as f:
        ds = json.load(f)

    conf = {
        "templates": [],
        "datasources": []

    }
    dsrdfmts = {}
    for d in ds:
        if d['type'] == 'SPARQL_Endpoint':
            rdfmts = get_typed_concepts(d['ID'], d['url'])
        else:
            mappings, rdfmts = ext_mappings(d['mappings'], d['ID'])
        # datasources[d['ID']] = DataSource(d['name'] if 'name' in d else d['ID'],
        #                                   d['ID'],
        #                                   d['url'],
        #                                   d['type'],
        #                                   d['params'],
        #                                   mappings)
        for rdfmt in rdfmts:
            rootType = rdfmt['rootType']
            if rootType not in dsrdfmts:
                dsrdfmts[rootType] = rdfmt
            else:
                otherrdfmt = dsrdfmts[rootType]
                dss = {d['datasource']: d for d in otherrdfmt['datasources']}
                if rdfmt['datasources'][0]['datasource'] not in dss:
                    otherrdfmt['datasources'].extend(rdfmt['datasources'])
                else:
                    pps = rdfmt['datasources'][0]['predicates']
                    dss[rdfmt['datasources'][0]['datasource']]['predicates'].extend(pps)
                    dss[rdfmt['datasources'][0]['datasource']]['predicates'] = list(set(dss[rdfmt['datasources'][0]['datasource']]['predicates']))
                    otherrdfmt['datasources'] = list(dss.values())

                otherpreds = {p['predicate']: p for p in otherrdfmt['predicates']}
                thispreds = {p['predicate']: p for p in rdfmt['predicates']}
                sameps = set(otherpreds.keys()).intersection(thispreds.keys())
                if len(sameps) > 0:
                    for p in sameps:
                        if len(thispreds[p]['range']) > 0:
                            otherpreds[p]['range'].extend(thispreds[p]['range'])
                            otherpreds[p]['range'] = list(set(otherpreds[p]['range']))
                preds = [otherpreds[p] for p in otherpreds]
                otherrdfmt['predicates'] = preds

                otherrdfmt['linkedTo'] = list(set(rdfmt['linkedTo'] + otherrdfmt['linkedTo']))

    conf['templates'] = list(dsrdfmts.values())
    conf['datasources'] = ds

    return conf


def ext_mappings(mappingslist, ds):
    mappings = {}
    rdfmts = []
    for m in mappingslist:
        mapping, rdfmt = read_mapping_file(m, ds)
        mappings.update(mapping)
        rdfmts.extend(rdfmt)

    return mappings, rdfmts


def read_mapping_file(m, ds):
    mappings, rdfmts = _query_mappings(m, ds)
    return mappings, rdfmts


def _query_mappings(filename, ds):
    g = rdflib.Graph()
    g.load(filename, format='n3')

    res = g.query(mapping_query)
    if res is None:
        return {}

    results = {}
    rdfmts = {}
    for row in res:

        rdfmt = row['rdfmt'].n3()[1:-1]

        predicate = row['predicate'].n3()[1:-1]
        if rdfmt not in rdfmts:
            rdfmts[rdfmt] = {}
        if predicate not in rdfmts[rdfmt]:
            rdfmts[rdfmt][predicate] = []

        objrdfclass = row['pomobjmaprdfmt'].n3()[1:-1] if row['pomobjmaprdfmt'] is not None else None

        if objrdfclass is not None:
            rdfmts[rdfmt][predicate].append(objrdfclass)

    molecules = []
    for m in rdfmts:
        rdfmt = {
            "rootType": m,
            "predicates": [],
            "linkedTo": [],
            "datasources": []
        }

        predicates = rdfmts[m]

        for p in predicates:
            rdfmt['predicates'].append({
                "predicate": p,
                "range": predicates[p]
            })
            rdfmt['linkedTo'].extend(predicates[p])

        preds = list(predicates.keys())
        rdfmt['datasources'].append({
            'datasource': ds,
            'predicates': preds

        })
        molecules.append(rdfmt)

    return results, molecules


dsquery = " ?tm rml:logicalSource ?ls . " \
            " ?ls rml:source ?sourceFile .  "

lsquery = dsquery + """                        
        OPTIONAL { ?ls rml:referenceFormulation ?refForm . }  
        OPTIONAL { ?ls rml:iterator ?iterator } 
"""
smquery = """
    OPTIONAL {
              ?tm rr:subject ?subject .
              }
    OPTIONAL {                      
              ?tm rr:subjectMap ?sm . 
              ?sm rr:class ?rdfmt . 
              OPTIONAL { ?sm rr:template ?smtemplate .} 
              OPTIONAL { ?sm rr:constant ?constsubject .}
              OPTIONAL { ?sm rml:reference ?smreference .}
            }
    OPTIONAL {
              ?tm rr:predicateObjectMap ?pom . 
              ?pom  rr:predicate ?predicate . 
              OPTIONAL { ?pom  rr:object ?objconst .}                             
              OPTIONAL {
                       ?pom rr:objectMap ?pomobjmap .            
                       OPTIONAL { ?pomobjmap rml:reference ?pomomapreference .}                                        
                       OPTIONAL { ?pomobjmap rr:constant ?constobject . }
                       OPTIONAL { ?pomobjmap rr:template ?predobjmaptemplate .}
                       OPTIONAL { ?pomobjmap rr:datatype ?pomobjmapdatatype.}
                       OPTIONAL { ?pomobjmap rr:class ?pomobjmaprdfmt . }      
                       OPTIONAL { ?pomobjmap rr:parentTriplesMap ?parentTPM . 	
                                  OPTIONAL{		
                                         ?pomobjmap rr:joinCondition ?jc .		
                                         ?jc rr:child ?jcchild .		
                                         ?jc rr:parent ?jcparent .		
                                         }		
                                 }	                              
                       }
           }
"""
proj = "?tm ?ls ?sourceFile ?sm ?pom ?pompm ?pomobjmap ?refForm ?iterator " \
       " ?rdfmt ?subject ?constsubject ?smtemplate ?smreference " \
       " ?predicate  ?pomomapreference ?pomobjmapdatatype ?constobject ?objconst " \
       " ?predobjmaptemplate ?parentTPM ?jcchild ?jcparent ?pomobjmaprdfmt "

mapping_query = prefixes + \
        " SELECT DISTINCT " + proj +" \n " + \
        " WHERE {\n\t\t" + \
        lsquery + \
        smquery + \
        " }"


def contactRDFSource(query, endpoint, format="application/sparql-results+json"):
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
                        reslist.append(x)
                    # reslist = res['results']['bindings']
                    return reslist, len(reslist)
                else:

                    return res['boolean'], 1

        else:
            print("Endpoint->", endpoint, resp.reason, resp.status_code, query)

    except Exception as e:
        print("Exception during query execution to", endpoint, ': ', e)

    return None, -2


def get_typed_concepts(ds, endpoint, limit=-1, types=[]):
    """
    Entry point for extracting RDF-MTs of an endpoint.
    Extracts list of rdf:Class concepts and predicates of an endpoint
    :param endpoint:
    :param limit:
    :return:
    """
    referer = endpoint
    reslist = []
    if len(types) == 0:
        query = "SELECT DISTINCT ?t  WHERE{  ?s a ?t.   }"
        if limit == -1:
            limit = 100
            offset = 0
            numrequ = 0
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    if limit < 1:
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                if card < limit:
                    break
                offset += limit
                time.sleep(5)
        else:
            reslist, card = contactRDFSource(query, referer)

        toremove = []
        # [toremove.append(r) for v in metas for r in reslist if v in r['t']]
        for r in reslist:
            for m in metas:
                if m in str(r['t']):
                    toremove.append(r)

        for r in toremove:
            reslist.remove(r)
    else:
        reslist = [{'t': t} for t in types]

    logger.info(endpoint)
    pprint(reslist)

    molecules = []
    for r in reslist:
        t = r['t']
        if "^^" in t:
            continue
        print(t)
        print("---------------------------------------")
        logger.info(t)

        rdfpropteries = []
        # Get predicates of the molecule t
        preds = get_predicates(referer, t)
        predicates = []
        linkedto = []
        for p in preds:
            pred = p['p']
            predicates.append(pred)

            # Get range of this predicate from this RDF-MT t
            ranges = get_rdfs_ranges(referer, pred)
            if len(ranges) == 0:
                rr = find_instance_range(referer, t, pred)
                mtranges = list(set(ranges + rr))
            else:
                mtranges = ranges
            ranges = []

            for mr in mtranges:
                if '^^' in mr:
                    continue
                if xsd not in mr:
                    ranges.append(mr)

            linkedto.extend(ranges)
            logger.info(pred + str(ranges))
            rdfpropteries.append({
                "predicate": pred,
                "range": ranges
            })

        rdfmt = {
            "rootType": t,
            "predicates": rdfpropteries,
            "linkedTo": linkedto,
            "datasources": [{
                'datasource': ds,
                'predicates': predicates

            }]
        }
        molecules.append(rdfmt)

    logger.info("=================================")

    return molecules


def get_rdfs_ranges(referer, p, limit=-1):

    RDFS_RANGES = " SELECT DISTINCT ?range" \
                  "  WHERE{ <" + p + "> <http://www.w3.org/2000/01/rdf-schema#range> ?range. " \
                                     "} "
    #
    # " " \

    reslist = []
    if limit == -1:
        limit = 100
        offset = 0
        numrequ = 0
        while True:
            query_copy = RDFS_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, referer)
            numrequ += 1
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                # print "setting limit to: ", limit
                if limit < 1:
                    break
                continue
            if card > 1:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(2)
    else:
        reslist, card = contactRDFSource(RDFS_RANGES, referer)

    ranges = []

    for r in reslist:
        skip = False
        for m in metas:
            if m in r['range']:
                skip = True
                break
        if not skip:
            ranges.append(r['range'])

    return ranges


def find_instance_range(referer, t, p, limit=-1):

    INSTANCE_RANGES = " SELECT DISTINCT ?r WHERE{ ?s a <" + t + ">. " \
                        " ?s <" + p + "> ?pt. " \
                        " ?pt a ?r . } "
    #
    #
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = INSTANCE_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, referer)
            numrequ += 1
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                # print "setting limit to: ", limit
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(2)
    else:
        reslist, card = contactRDFSource(INSTANCE_RANGES, referer)

    ranges = []

    for r in reslist:
        skip = False
        for m in metas:
            if m in r['r']:
                skip = True
                break
        if not skip:
            ranges.append(r['r'])

    return ranges


def get_predicates(referer, t, limit=-1):
    """
    Get list of predicates of a class t

    :param referer: endpoint
    :param server: server address of an endpoint
    :param path:  path in an endpoint (after server url)
    :param t: RDF class Concept extracted from an endpoint
    :param limit:
    :return:
    """
    #
    query = " SELECT DISTINCT ?p WHERE{ ?s a <" + t + ">. ?s ?p ?pt.  } "
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        print(t)
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, referer)
            numrequ += 1
            # print "predicates card:", card
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                # print "setting limit to: ", limit
                if limit < 1:
                    print("giving up on " + query)
                    print("trying instances .....")
                    rand_inst_res = get_preds_of_random_instances(referer, t)
                    existingpreds = [r['p'] for r in reslist]
                    for r in rand_inst_res:
                        if r not in existingpreds:
                            reslist.append({'p': r})
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(2)
    else:
        reslist, card = contactRDFSource(query, referer)

    return reslist


def get_preds_of_random_instances(referer, t, limit=-1):

    """
    get a union of predicated from 'randomly' selected 10 entities from the first 100 subjects returned

    :param referer: endpoint
    :param server:  server name
    :param path: path
    :param t: rdf class concept of and endpoint
    :param limit:
    :return:
    """
    query = " SELECT DISTINCT ?s WHERE{ ?s a <" + t + ">. } "
    reslist = []
    if limit == -1:
        limit = 50
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, referer)
            numrequ += 1
            # print "rand predicates card:", card
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                # print "setting limit to: ", limit
                if limit < 1:
                    break
                continue
            if numrequ == 100:
                break
            if card > 0:
                import random
                rand = random.randint(0, card - 1)
                inst = res[rand]
                inst_res = get_preds_of_instance(referer, inst['s'])
                inst_res = [r['p'] for r in inst_res]
                reslist.extend(inst_res)
                reslist = list(set(reslist))
            if card < limit:
                break
            offset += limit
            time.sleep(5)
    else:
        reslist, card = contactRDFSource(query, referer)

    return reslist


def get_preds_of_instance(referer, inst, limit=-1):
    query = " SELECT DISTINCT ?p WHERE{ <" + inst + "> ?p ?pt. } "
    reslist = []
    if limit == -1:
        limit = 1000
        offset = 0
        numrequ = 0
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, referer)
            numrequ += 1
            # print "inst predicates card:", card
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                # print "setting limit to: ", limit
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(2)
    else:
        reslist, card = contactRDFSource(query, referer)

    return reslist


def get_options(argv):
    try:
        opts, args = getopt.getopt(argv, "h:s:o:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    '''
    Supported output formats:
        - json (default)
        - nt
        - SPARQL-UPDATE (directly store to sparql endpoint)
    '''

    source = None
    output = 'config-output.json'
    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-s":
            source = arg
        elif opt == "-o":
            output = arg

    if not source:
        usage()
        sys.exit(1)

    return source, output


def usage():
    usage_str = ("Usage: {program} \n"
                 "-s <path/to/datasources.json> \n"
                 "-o <path/to/config-output.json> \n"
                 "where \n"                                  
                 "\t<path/to/datasources.json> - path to dataset info file  \n"
                 "\t<path/to/config-output.json> - name of output file  \n")

    print(usage_str.format(program=sys.argv[0]),)


if __name__ == "__main__":
    source, output = get_options(sys.argv[1:])
    # source = "../configurations/ds_config.json"
    # output = 'config-output.json'
    conf = read_config(source)
    pprint(conf)
    json.dump(conf, open(output, 'w+'))