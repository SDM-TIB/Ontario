
__author__ = 'Kemele M. Endris'

import json
import rdflib
from rdflib.term import BNode

from ontario.model.rml_model import *
from ontario.model.rdfmt_model import RDFMT, MTPredicate, DataSource, DataSourceType


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

            mappings = self.load_mappings(d['mappings'])

            datasources[d['ID']] = DataSource(d['name'] if 'name' in d else d['ID'],
                                              d['ID'],
                                              d['url'],
                                              d['type'],
                                              d['params'],
                                              d['mappings'],
                                              mappings
                                              )
        return datasources

    def load_mappings(self, mappingslist, rdfmts=[]):
        if len(mappingslist) > 0:
            return self.read_mapping_files(mappingslist, rdfmts)
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

    def read_mapping_files(self, mappingslist, rdfmts=[]):
        mappings = query_rml(mappingslist, rdfmts)
        return mappings

    def find_rdfmt_by_preds(self, preds):
        res = []
        if len(preds) == 0:
            return self.metadata
        for p in preds:
            if p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                continue
            if p in self.predicateMTindex:
                res.append(self.predicateMTindex[p])

        for r in res[1:]:
            res[0] = res[0].intersection(r)
        results = {}
        if len(res) > 0:
            mols = list(set(res[0]))
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


def query_rml(filenames, rdfmts=[]):
    templates = {}
    graph = rdflib.Graph()
    for filename in filenames:
        graph.load(filename, format='n3')

    ls_query = "?tm rml:logicalSource ?ls . \n\t\t" \
               "?ls rml:source ?source " \
               "OPTIONAL {?source ?p ?o . }\n\t\t" \
               "OPTIONAL {?ls rml:iterator ?iterator . }\n\t\t" \
               "OPTIONAL {?ls rr:sqlVersion ?sqlVersion. }\n\t\t" \
               "OPTIONAL {?ls rml:query ?lcQuery .} \n\t\t" \
               "OPTIONAL {?ls rr:tableName ?tableName. }\n\t\t" \
               "OPTIONAL {?ls rml:referenceFormulation ?refForm . }\n\t\t"
    ls_query = prefixes + '\n' + " SELECT DISTINCT ?tm ?ls ?source  ?p ?o ?iterator ?sqlVersion ?lcQuery ?tableName ?refForm WHERE {\n\t\t" + ls_query + " }"

    res = graph.query(ls_query)
    triplemaps = {}
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

    # print('_________________________________________________________')
    for tm in triplemaps:
        sourceType = DataSourceType.LOCAL_CSV

        source = list(triplemaps[tm]['source'].keys())[0]
        sourceDesc = triplemaps[tm]['source'][source]
        ls = triplemaps[tm]['ls']
        iterator = triplemaps[tm]['iterator']
        refForm = triplemaps[tm]['refForm']
        sqlVersion = triplemaps[tm]['sqlVersion']
        tableName = triplemaps[tm]['tableName']
        lcQuery = triplemaps[tm]['lcQuery']

        datasource = RMLDataSource(source, source, sourceType)
        logicalSource = LogicalSource(ls, datasource, iterator, refForm)

        if sqlVersion is not None or refForm is not None and 'SQL2008' in refForm:

            logicalSource.table_name = tableName
            logicalSource.sqlVersion = sqlVersion
            logicalSource.query = lcQuery

            jdbcd = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDriver']
            if 'mysql' in jdbcd:
                sourceType = DataSourceType.MYSQL
            elif 'postgresql' in jdbcd:
                sourceType = DataSourceType.POSTGRESQL
            elif 'microsoft' in jdbcd:
                sourceType = DataSourceType.SQLSERVER

            datasource.name = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN']
            datasource.ID = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN']
            datasource.ds_desc['dbName'] = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN']
            datasource.ds_desc['username'] = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#username']
            datasource.ds_desc['password'] = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#password']
            datasource.ds_desc['jdbcDriver'] = sourceDesc['http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDriver']
            datasource.ds_desc['sqlVersion'] = sqlVersion

            datasource.ds_type = sourceType

        elif '.json' in source or refForm is not None and 'JSON' in refForm:
            sourceType = DataSourceType.LOCAL_JSON
        elif '.xml' in source or refForm is not None and 'XPath' in refForm:
            sourceType = DataSourceType.LOCAL_XML
        elif '.tsv' in source or refForm is not None and 'TSV' in refForm:
            sourceType = DataSourceType.LOCAL_TSV
        elif '.csv' in source or refForm is not None and 'CSV' in refForm:
            sourceType = DataSourceType.LOCAL_CSV
        elif refForm is not None and 'Mongo' in refForm:
            sourceType = DataSourceType.MONGODB

            datasource.name = sourceDesc['http://ontario.tib.eu/mapping/database']
            datasource.ID = sourceDesc['http://ontario.tib.eu/mapping/database']
            datasource.ds_desc['dbName'] = sourceDesc['http://ontario.tib.eu/mapping/database']
            datasource.ds_desc['username'] = sourceDesc['http://ontario.tib.eu/mapping/username']
            datasource.ds_desc['password'] = sourceDesc['http://ontario.tib.eu/mapping/password']
            datasource.ds_desc['mongoVersion'] = sqlVersion
        elif refForm is not None and 'Cypher' in refForm:
            sourceType = DataSourceType.NEO4J
            datasource.name = sourceDesc['http://ontario.tib.eu/mapping/database']
            datasource.ID = sourceDesc['http://ontario.tib.eu/mapping/database']
            datasource.ds_desc['dbName'] = sourceDesc['http://ontario.tib.eu/mapping/database']

            datasource.ds_desc['username'] = sourceDesc['http://ontario.tib.eu/mapping/username']
            datasource.ds_desc['password'] = sourceDesc['http://ontario.tib.eu/mapping/password']
            datasource.ds_desc['neo4jVersion'] = sqlVersion

        datasource.ds_type = sourceType
        logicalSource.data_source = datasource

        subjectMap, predicate_object_map = query_rml_subj_pred_obj(graph, tm, rdfmts)
        if subjectMap is not None:
            tmap = TripleMap(tm, logicalSource, subjectMap, predicate_object_map)
            # print(tmap)
            templates[tm] = tmap

    return templates


def query_rml_subj_pred_obj(graph, tm, rdfmts=[]):
    subjectMap = query_rml_subj(graph, tm, rdfmts)
    predicate_object_map = query_rml_pred_obj(graph, tm)

    return subjectMap, predicate_object_map


def query_rml_subj(graph, tm, rdfmts=[]):

    subj_query = "<" + tm + "> rml:logicalSource ?ls ." \
                 "OPTIONAL { <" + tm + "> rr:subject ?subject . }" \
                 "OPTIONAL { <" + tm + "> rr:subjectMap ?sm . " \
                 "          " \
                 "          OPTIONAL { ?sm rr:termType ?smtype . }" \
                 "          OPTIONAL { ?sm rr:template ?smtemplate .}" \
                 "          OPTIONAL { ?sm rr:constant ?constsubject .}" \
                 "          OPTIONAL { ?sm rml:reference ?smreference .}" \
                 ""
    if len(rdfmts) > 0:
        rdfmts = ["?rdfmt = <" + mt + ">" for mt in rdfmts]
        filter_str = "FILTER (" + " || ".join(rdfmts) + ")"
        subj_query += "?sm rr:class ?rdfmt .}"
        subj_query += filter_str
    else:
        subj_query += " OPTIONAL { ?sm rr:class ?rdfmt .} }"

    subj_query = prefixes + '\n' + " SELECT DISTINCT * \n WHERE {\n\t\t" + subj_query + " }"
    res = graph.query(subj_query)
    qresults = {}
    subjTerm = None
    sm = None
    for row in res:
        sm = row['sm'].n3()[1:-1] if row['sm'] is not None else None

        rdfmt = row['rdfmt'].n3()[1:-1] if row['rdfmt'] is not None else tm
        if len(qresults) == 0:
            qresults.setdefault('rdfmt', []).append(rdfmt)
        else:
            qresults.setdefault('rdfmt', []).append(rdfmt)
            qresults['rdfmt'] = list(set(qresults['rdfmt']))
            continue

        smtype = row['smtype'].n3()[1:-1] if row['smtype'] is not None else TermType.IRI

        if row['subject'] is not None:
            subject = row['subject'].n3()[1:-1]
            subjTerm = TermMap(subject, TripleMapType.CONSTANT, smtype)
        elif row['constsubject'] is not None:
            subject = row['constsubject'].n3()[1:-1]
            subjTerm = TermMap(subject, TripleMapType.CONSTANT, smtype)
        elif row['smtemplate'] is not None:
            smtemplate = row['smtemplate'].n3()[1:-1]
            subjTerm = TermMap(smtemplate, TripleMapType.TEMPLATE, smtype)
        elif row['smreference'] is not None:
            smreference = row['smreference'].n3()[1:-1]
            subjTerm = TermMap(smreference, TripleMapType.REFERENCE, smtype)
        else:
            subjTerm = None
    if sm is None:
        return

    subj = SubjectMap(sm, subjTerm, qresults['rdfmt'])
    return subj


def query_rml_pred_obj(graph, tm):
    predicate_object_map = {}

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
    res = graph.query(pred_query)
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

        if predicate is not None:
            predicateTerm = TermMap(predicate, TripleMapType.CONSTANT, TermType.IRI)
        elif pmtemplate is not None:
            predicateTerm = TermMap(pmtemplate, TripleMapType.TEMPLATE, TermType.IRI)
            predicate = pmtemplate
        elif pmreference is not None:
            predicateTerm = TermMap(pmreference, TripleMapType.REFERENCE, TermType.IRI)
            predicate = pmreference
        else:
            predicateTerm = None

        pred = PredicateMap(predicate, predicateTerm)

        pomobjtmtype = row['pomobjtmtype'].n3()[1:-1] if row['pomobjtmtype'] is not None else None

        if row['object'] is not None:
            pobject = row['object'].n3()[1:-1]
            if isinstance(row['object'], BNode):
                pomobjtmtype = TermType.BNode
            elif isinstance(row['object'], rdflib.Literal):
                pomobjtmtype = TermType.Literal
            else:
                pomobjtmtype = TermType.IRI
            objTerm = TermMap(pobject, TripleMapType.CONSTANT, pomobjtmtype)
        elif row['constobject'] is not None:
            pobject = row['constobject'].n3()[1:-1]
            if isinstance(row['constobject'], BNode):
                pomobjtmtype = TermType.BNode
            elif isinstance(row['constobject'], rdflib.Literal):
                pomobjtmtype = TermType.Literal
            else:
                pomobjtmtype = TermType.IRI
            objTerm = TermMap(pobject, TripleMapType.CONSTANT, pomobjtmtype)
        elif row['predobjmaptemplate'] is not None:
            predobjmaptemplate = row['predobjmaptemplate'].n3()[1:-1]
            if pomobjtmtype is None:
                objTerm = TermMap(predobjmaptemplate, TripleMapType.TEMPLATE, TermType.IRI)
            else:
                objTerm = TermMap(predobjmaptemplate, TripleMapType.TEMPLATE, pomobjtmtype)
        elif row['pomomapreference'] is not None:
            pomomapreference = row['pomomapreference'].n3()[1:-1]
            if pomobjtmtype is None:
                objTerm = TermMap(pomomapreference, TripleMapType.REFERENCE, TermType.Literal)
            else:
                objTerm = TermMap(pomomapreference, TripleMapType.REFERENCE, pomobjtmtype)

        else:
            objTerm = None

        parentTPM = row['parentTPM'].n3()[1:-1] if row['parentTPM'] is not None else None
        jcchild = row['jcchild'].n3()[1:-1] if row['jcchild'] is not None else None
        jcparent = row['jcparent'].n3()[1:-1] if row['jcparent'] is not None else None
        if parentTPM is not None:
            objTerm = ObjectReferenceMap(parentTPM, jcchild, jcparent)

        pomobjmaplangtag = row['pomobjmaplangtag'].n3()[1:-1] if row['pomobjmaplangtag'] is not None else None
        pomobjmapdatatype = row['pomobjmapdatatype'].n3()[1:-1] if row['pomobjmapdatatype'] is not None else None
        pomobjmaprdfmt = row['pomobjmaprdfmt'].n3()[1:-1] if row['pomobjmaprdfmt'] is not None else None

        obj = ObjectMap(objTerm.value, objTerm, pomobjmapdatatype, pomobjmaplangtag, pomobjmaprdfmt)
        predicate_object_map[pred.ID] = (pred, obj)

    return predicate_object_map
