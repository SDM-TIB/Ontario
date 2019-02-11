#!/usr/bin/env python3.5

import hashlib
import json
import pprint as pp
import pprint
import random
from multiprocessing import Queue, Process
from multiprocessing.queues import Empty
import logging
import time
from pprint import pprint
import rdflib
from ontario.config.model import *

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
        datasources[d['ID']] = DataSource(d['name'] if 'name' in d else d['ID'],
                                          d['ID'],
                                          d['url'],
                                          d['type'],
                                          d['params'],
                                          mappings)
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
    pprint(conf)
    json.dump(conf, open('temp-conf.json', 'w+'))
    return datasources


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

        tm = row['tm'].n3()[1:-1]
        rdfmt = row['rdfmt'].n3()[1:-1]

        source = row['sourceFile'].n3()[1:-1]
        iterators = row['iterator'].n3()[1:-1] if row['iterator'] is not None else "*"
        subjectType = TermType.CONSTANT if row['subject'] is not None or \
                                            row['constsubject'] is not None else \
                        TermType.TEMPLATE if row['smtemplate'] is not None else \
                        TermType.REFERENCE if row['smreference'] is not None else None

        subject = row['subject'] if row['subject'] is not None \
                                               else row['smtemplate'].n3()[1:-1] if row['smtemplate'] is not None \
                                               else row['constsubject'].n3()[1:-1] if row['constsubject'] is not None \
                                               else row['smreference'].n3()[1:-1] if row['smreference'] is not None \
                                               else None
        if subjectType is None or subject is None or len(source.strip()) == 0:
            continue

        mapping = RMLMapping(source, subject, iterators, subjectType)

        if tm not in results:
            results[tm] = {}
            results[tm][rdfmt] = mapping
        else:
            if rdfmt not in results[tm]:
                results[tm][rdfmt] = mapping
            else:
                mapping = results[tm][rdfmt]

        predicate = row['predicate'].n3()[1:-1]
        if rdfmt not in rdfmts:
            rdfmts[rdfmt] = {}
        if predicate not in rdfmts[rdfmt]:
            rdfmts[rdfmt][predicate] = []

        objdtype = row['pomobjmapdatatype'].n3() if row['pomobjmapdatatype'] is not None else None
        objrdfclass = row['pomobjmaprdfmt'].n3()[1:-1] if row['pomobjmaprdfmt'] is not None else None

        if objrdfclass is not None:
            rdfmts[rdfmt][predicate].append(objrdfclass)

        objtype = TermType.CONSTANT if row['objconst'] is not None or row['constobject'] is not None \
                                               else TermType.TEMPLATE if row['predobjmaptemplate'] is not None \
                                               else TermType.REFERENCE if row['pomomapreference'] is not None \
                                               else TermType.TRIPLEMAP if row['parentTPM'] is not None \
                                               else None
        object = row['objconst'].n3()[1:-1] if row['objconst'] is not None \
                                else row['constobject'].n3()[1:-1] if row['constobject'] is not None \
                                else row['predobjmaptemplate'].n3()[1:-1] if row['predobjmaptemplate'] is not None \
                                else row['pomomapreference'].n3()[1:-1] if row['pomomapreference'] is not None \
                                else row['parentTPM'].n3()[1:-1] if row['parentTPM'] is not None \
                                else None
        jchild = row['jcchild'].n3()[1:-1] if row['jcchild'] is not None else None
        jparent = row['jcparent'].n3()[1:-1] if row['jcparent'] is not None else None

        mapping.predicateObjMap[predicate] = RDFMPredicateObjMap(predicate,
                                                                object,
                                                                objtype,
                                                                objdtype,
                                                                objrdfclass,
                                                                jchild,
                                                                jparent)

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


if __name__ == "__main__":
    read_config("/home/kemele/git/SemanticDataLake/Ontario/configurations/ds_config.json")