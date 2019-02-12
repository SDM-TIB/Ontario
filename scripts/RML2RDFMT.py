#!/usr/bin/env python3.5

import getopt, sys
from pprint import pprint
import rdflib
from enum import Enum
import json

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


class TermType(Enum):
    TEMPLATE = "template"
    CONSTANT = "constant"
    REFERENCE = "reference"
    TRIPLEMAP = "triplemap"


def read_config(filename):
    with open(filename, "r", encoding='utf8') as f:
        ds = json.load(f)

    datasources = {}
    conf = {
        "templates": [],
        "datasources": []

    }
    dsrdfmts = {}
    for d in ds:
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
        # for m in mapping:
        #     for rdfmt in mapping[m]:
        #         mappings.setdefault().setdefault(rdfmt, []).append(mapping[m][rdfmt])
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
            "datasources": []
        }

        predicates = rdfmts[m]

        for p in predicates:
            rdfmt['predicates'].append({
                "predicate": p,
                "range": predicates[p]
            })
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
    conf = read_config("../configurations/ds_config.json")
    pprint(conf)
    json.dump(conf, open(output, 'w+'))