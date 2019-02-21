
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
        if len(mappingslist) > 0:
            return self.read_mapping_file(mappingslist)
        # for m in mappingslist:
        #     mapping = self.read_mapping_file(m)
        #     mappings.update(mapping)
        #     # for m in mapping:
        #     #     for rdfmt in mapping[m]:
        #     #         mappings.setdefault().setdefault(rdfmt, []).append(mapping[m][rdfmt])
        return {}

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

    def read_mapping_file(self, mappingslist):
        mappings = self._query_mappings(mappingslist)
        return mappings

    def _query_mappingsX(self, mappingslist):
        g = rdflib.Graph()
        for filename in mappingslist:
            g.load(filename, format='n3')

        ls_query = "?tm rml:logicalSource ?ls . \n\t\t" \
                   "?ls rml:source ?source .\n\t\t" \
                   "OPTIONAL {?source ?p ?o }\n\t\t" \
                   "OPTIONAL {?ls rml:iterator ?iterator . }\n\t\t" \
                   "OPTIONAL {?ls rr:sqlVersion ?sqlVersion. }\n\t\t" \
                   "OPTIONAL {?ls rml:query ?lcQuery .} \n\t\t" \
                   "OPTIONAL {?ls rr:tableName ?tableName. }\n\t\t" \
                   "OPTIONAL {?ls rml:referenceFormulation ?refForm . }\n\t\t"
        ls_query = prefixes + '\n' + " SELECT DISTINCT * \n WHERE {\n\t\t" + ls_query + " }"

        res = g.query(ls_query)
        triplemaps = {}
        results = {}
        for row in res:
            tm = row['tm'].n3()[1:-1]
            source = row['source'].n3() if row['p'] is not None else row['source'].n3()[1:-1]

            if tm not in triplemaps:
                triplemaps[tm] = {}
                ls = row['ls'].n3()[1:-1]
                iterator = row['iterator'].n3()[1:-1] if row['iterator'] is not None else None
                sqlVersion = row['sqlVersion'].n3()[1:-1] if row['sqlVersion'] is not None else None
                lcQuery = row['lcQuery'].n3()[1:-1] if row['lcQuery'] is not None else None
                tableName = row['tableName'].n3()[1:-1] if row['tableName'] is not None else None
                refForm = row['refForm'].n3()[1:-1] if row['refForm'] is not None else None
                triplemaps[tm]['ls'] = ls
                triplemaps[tm]['source'] = {source: {}}
                triplemaps[tm]['iterator'] = iterator
                triplemaps[tm]['sqlVersion'] = sqlVersion
                triplemaps[tm]['lcQuery'] = lcQuery
                triplemaps[tm]['tableName'] = tableName
                triplemaps[tm]['refForm'] = refForm

            p = row['p'].n3()[1:-1] if row['p'] is not None else None
            o = row['o'].n3()[1:-1] if row['o'] is not None else None
            if p is not None:
                triplemaps[tm]['source'][source][p] = o

        for tm in triplemaps:
            source = list(triplemaps[tm]['source'].keys())[0]
            ls = triplemaps[tm]['ls']
            iterator = triplemaps[tm]['iterator']
            refForm = triplemaps[tm]['refForm']

            #datasource = DataSource(source, source, sourceType)
            #logicalSource = LogicalSource(ls, datasource, iterator, refForm)

            # subjectMap, predicate_object_map = self.query_rml_subj_pred_obj(g, tm)

            subject, rdfmts, subjtype = self.query_rml_subj(g, tm)
            mapping = RMLMapping(source, subject, iterator, subjtype)
            if len(rdfmts) == 0:
                continue
            rdfmt = rdfmts[0]
            if tm not in results:
                results[tm] = {}
                results[tm][rdfmt] = mapping
            else:
                if rdfmt not in results[tm]:
                    results[tm][rdfmt] = mapping
                else:
                    mapping = results[tm][rdfmt]

            results[tm][rdfmt] = self.query_rml_pred_obj(g, tm, mapping)

            # tmap = TripleMap(tm, logicalSource, subjectMap, predicate_object_map)
            # print(tmap)
        return results

    def query_rml_subj(self, g, tm):

        subj_query = "<" + tm + "> rml:logicalSource ?ls ." \
                      "OPTIONAL { <" + tm + "> rr:subject ?subject . }" \
                      "OPTIONAL { <" + tm + "> rr:subjectMap ?sm . " \
                        "          OPTIONAL { ?sm rr:class ?rdfmt .} " \
                        "          OPTIONAL { ?sm rr:termType ?smtype . }" \
                        "          OPTIONAL { ?sm rr:template ?smtemplate .}" \
                        "          OPTIONAL { ?sm rr:constant ?constsubject .}" \
                        "          OPTIONAL { ?sm rml:reference ?smreference .}" \
                        "}"
        subj_query = prefixes + '\n' + " SELECT DISTINCT * \n WHERE {\n\t\t" + subj_query + " }"
        res = g.query(subj_query)
        qresults = {}
        subject = None
        sm = None
        subjtype = None
        for row in res:
            sm = row['sm'].n3()[1:-1] if row['sm'] is not None else None

            rdfmt = row['rdfmt'].n3()[1:-1] if row['rdfmt'] is not None else tm
            if len(qresults) == 0:
                qresults.setdefault('rdfmt', []).append(rdfmt)
            else:
                qresults.setdefault('rdfmt', []).append(rdfmt)
                qresults['rdfmt'] = list(set(qresults['rdfmt']))
                continue

            if row['subject'] is not None:
                subject = row['subject'].n3()[1:-1]
                subjtype = TermType.CONSTANT
            elif row['constsubject'] is not None:
                subject = row['constsubject'].n3()[1:-1]
                subjtype = TermType.CONSTANT
            elif row['smtemplate'] is not None:
                subject = row['smtemplate'].n3()[1:-1]
                subjtype = TermType.TEMPLATE
            elif row['smreference'] is not None:
                subject = row['smreference'].n3()[1:-1]
                subjtype = TermType.REFERENCE
            else:
                subject = None
        if sm is None:
            return None, None, None

        return subject, qresults['rdfmt'], subjtype

    def query_rml_pred_obj(self, g, tm, mapping):

        pred_query = " <" + tm + "> rr:predicateObjectMap ?pom . " \
                     "OPTIONAL { ?pom  rr:predicate ?predicate .}" \
                     "OPTIONAL { ?pom rr:predicateMap ?pm . " \
                     "          OPTIONAL { ?pm rr:template ?pmtemplate .}" \
                     "          OPTIONAL { ?pm rr:constant ?constpredicate .}" \
                     "          OPTIONAL { ?pm rml:reference ?pmreference .}" \
                     "} "
        obj_query = " OPTIONAL {?pom  rr:object ?object }" \
                    "OPTIONAL {?pom   rr:objectMap ?pomobjmap . " \
                    "     OPTIONAL { ?pomobjmap rml:reference ?pomomapreference .}" \
                    "     OPTIONAL { ?pomobjmap rr:constant ?constobject . }" \
                    "     OPTIONAL { ?pomobjmap rr:template ?predobjmaptemplate .}" \
                    "     OPTIONAL { ?pomobjmap rr:datatype ?pomobjmapdatatype.}" \
                    "     OPTIONAL { ?pomobjmap rr:language  ?pomobjmaplangtag.}" \
                    "     OPTIONAL { ?pomobjmap rr:class ?pomobjmaprdfmt . } " \
                    "     OPTIONAL { ?pomobjmap rr:termType ?pomobjtmtype . } " \
                    "     OPTIONAL { ?pomobjmap rr:parentTriplesMap ?parentTPM . " \
                    "                OPTIONAL{?pomobjmap rr:joinCondition ?jc ." \
                    "                         ?jc rr:child ?jcchild ." \
                    "                         ?jc rr:parent ?jcparent ." \
                    "                        }" \
                    "             }" \
                    " } "

        pred_query = prefixes + '\n' + " SELECT DISTINCT * \n WHERE {\n\t\t" + pred_query + obj_query + " }"
        res = g.query(pred_query)
        pom = None

        for row in res:
            if pom is None:
                pom = row['pom'].n3()[1:-1]
            if row['predicate'] is not None:
                predicate = row['predicate'].n3()[1:-1]
            elif row['constpredicate'] is not None:
                predicate = row['constpredicate'].n3()[1:-1]
            else:
                predicate = None

            pmtemplate = row['pmtemplate'].n3()[1:-1] if row['pmtemplate'] is not None else None
            pmreference = row['pmreference'].n3()[1:-1] if row['pmreference'] is not None else None

            if pmtemplate is not None:
                # predicateTerm = TermMap(pmtemplate, TripleMapType.TEMPLATE, TermType.IRI)
                predicate = pmtemplate
            elif pmreference is not None:
                # predicateTerm = TermMap(pmreference, TripleMapType.REFERENCE, TermType.IRI)
                predicate = pmreference
            else:
                predicateTerm = None

            # pred = PredicateMap(predicate, predicateTerm)

            # pomobjtmtype = row['pomobjtmtype'].n3()[1:-1] if row['pomobjtmtype'] is not None else None

            if row['object'] is not None:
                pobject = row['object'].n3()[1:-1]
                objtype = TermType.CONSTANT
            elif row['constobject'] is not None:
                pobject = row['constobject'].n3()[1:-1]
                objtype = TermType.CONSTANT
            elif row['predobjmaptemplate'] is not None:
                pobject = row['predobjmaptemplate'].n3()[1:-1]
                objtype = TermType.TEMPLATE
            elif row['pomomapreference'] is not None:
                pobject = row['pomomapreference'].n3()[1:-1]
                objtype = TermType.REFERENCE
            else:
                pobject = None
                objtype = None

            parentTPM = row['parentTPM'].n3()[1:-1] if row['parentTPM'] is not None else None
            jchild = row['jcchild'].n3()[1:-1] if row['jcchild'] is not None else None
            jparent = row['jcparent'].n3()[1:-1] if row['jcparent'] is not None else None

            pomobjmaplangtag = row['pomobjmaplangtag'].n3()[1:-1] if row['pomobjmaplangtag'] is not None else None
            pomobjmapdatatype = row['pomobjmapdatatype'].n3()[1:-1] if row['pomobjmapdatatype'] is not None else None
            pomobjmaprdfmt = row['pomobjmaprdfmt'].n3()[1:-1] if row['pomobjmaprdfmt'] is not None else None

            # obj = ObjectMap(objTerm.value, objTerm, pomobjmapdatatype, pomobjmaplangtag, pomobjmaprdfmt)
            # predicate_object_map[pred.ID] = (pred, obj)

            mapping.predicateObjMap[predicate] = RDFMPredicateObjMap(predicate,
                                                                     pobject,
                                                                     objtype,
                                                                     pomobjmapdatatype,
                                                                     pomobjmaprdfmt,
                                                                     jchild,
                                                                     jparent)
        return mapping

    def _query_mappings(self, mappingslist):
        g = rdflib.Graph()
        results = {}
        try:
            for filename in mappingslist:
                g.load(filename, format='n3')

            res = g.query(mapping_query)
            if res is None:
                return {}

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
                tm = tm.replace('http://tib.de/ontario/mapping#', '')
                if tm + source not in results:
                    results[tm+source] = {}
                    results[tm+source][rdfmt] = mapping
                else:
                    if rdfmt not in results[tm+source]:
                        results[tm+source][rdfmt] = mapping
                    else:
                        mapping = results[tm+source][rdfmt]

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
        except Exception as e:
            print("Exception reading mapping", mappingslist, e)

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
    mapping = OntarioConfiguration("/home/kemele/git/SemanticDataLake/Ontario/scripts/polyweb-polystore.json")
    mapping.read_config()
    import pprint
    pprint.pprint(mapping.filename)
    pprint.pprint(mapping.metadata)
    pprint.pprint(mapping.datasources)