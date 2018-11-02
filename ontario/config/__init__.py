
__author__ = 'kemele'
import abc
import json
import logging
import rdflib
from ontario.config.model import *

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("{0}.log".format('ontario'))
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)


class OntarioConfiguration(object):

    def __init__(self, filename):
        self.filename = filename
        self.metadata = {}
        self.datasources = {}
        self.predicateMTindex = {}
        self.read_config()

    def read_config(self):
        # try:
        with open(self.filename, "r", encoding='utf8') as f:
            confdata = json.load(f)
            # read data sources first and templates next
            self.ext_datasources(confdata)
            self.ext_templates(confdata)
        # except Exception as ex:
        #     logger.error("cannot process configurations file. Please check if the file is properly formated!")
        #     logger.error(ex)
        #     traceback.print_exc()
        #     pass

    def ext_templates(self, confdata):
        if 'templates' in confdata:
            mts = confdata['templates']
            self.metadata = self.ext_templates_json(mts)
        else:
            self.metadata = {}

    def ext_datasources(self, confdata):
        if 'datasources' in confdata:
            ds = confdata['datasources']
            self.datasources = self.ext_datasources_json(ds)
        else:
            self.datasources = {}

    def ext_datasources_json(self, ds):
        datasources = {}
        for d in ds:
            mappings = self.ext_mappings(d['mappings'])
            datasources[d['ID']] = DataSource(d['name'] if 'name' in d else d['ID'],
                                              d['ID'],
                                              d['url'],
                                              d['type'],
                                              d['params'],
                                              mappings)

        return datasources

    # def check_triplemap_refs(self, mappings):
    #     for tm in mappings:
    #         for rdfmt in mappings[tm]:
    #             for pred in mappings[tm][rdfmt].predicateObjMap:
    #                 predobj = mappings[tm][rdfmt].predicateObjMap[pred]
    #                 if predobj.objectType == TermType.TRIPLEMAP:
    #                     rml = mappings[predobj.object]
    #                     rml = rml[list(rml.keys)[0]]
    #                     predobj.object = rml.subject

    def ext_mappings(self, mappingslist):
        mappings = {}
        for m in mappingslist:
            mapping = self.read_mapping_file(m)
            mappings.update(mapping)
            # for m in mapping:
            #     for rdfmt in mapping[m]:
            #         mappings.setdefault().setdefault(rdfmt, []).append(mapping[m][rdfmt])
        return mappings

    def ext_templates_json(self, mts):
        meta = {}
        for m in mts:
            rootType = m['rootType']
            predicates = m['predicates']
            linkedTo = []
            preds = {}
            for p in predicates:
                self.predicateMTindex.setdefault(p['predicate'], set()).add(rootType)
                preds[p['predicate']] = MTPredicate(p['predicate'], p['range'])
                linkedTo.extend(p['range'])
            datasources = m['datasources']
            wrappers = {}
            for w in datasources:
                wrappers[w['datasource']] = w['predicates']
            mt = RDFMT(rootType, linkedTo, preds, wrappers)
            meta[rootType] = mt

        return meta

    def read_mapping_file(self, m):
        mappings = self._query_mappings(m)
        return mappings

    def _query_mappings(self, filename):
        g = rdflib.Graph()
        g.load(filename, format='n3')

        res = g.query(mapping_query)
        if res is None:
            return {}

        results = {}

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

            objdtype = row['pomobjmapdatatype'].n3() if row['pomobjmapdatatype'] is not None else None
            objrdfclass = row['pomobjmaprdfmt'].n3()[1:-1] if row['pomobjmaprdfmt'] is not None else None

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

        return results

    def find_rdfmt_by_preds(self, preds):
        res = []
        for p in preds:
            if p in self.predicateMTindex:
                res.append(self.predicateMTindex[p])
        for r in res[1:]:
            res[0] = res[0].intersection(r)
        results = {}
        if len(res) > 0:
            mols = list(res[0])
            for m in mols:
                results[m] = self.metadata[m]
        return results

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
    mapping = OntarioConfiguration("/home/kemele/git/SDM/Ontario/configurations/biomed-configuration.json")
    mapping.read_config()
    import pprint
    pprint.pprint(mapping.filename)
    pprint.pprint(mapping.metadata)
    pprint.pprint(mapping.datasources)