import os, sys
from pprint import pprint
import rdflib
from rdflib.term import BNode
from tests.rml.rml_model import *


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
    g = rdflib.Graph()
    for filename in filenames:
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

        elif '.json' in source or refForm is not None and 'JSON' in refForm :
            sourceType = DataSourceType.LOCAL_JSON
        elif '.xml' in source or refForm is not None and 'XPath' in refForm:
            sourceType = DataSourceType.LOCAL_XML
        elif '.tsv' in source or refForm is not None and 'TSV' in refForm:
            sourceType = DataSourceType.LOCAL_TSV
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

        subjectMap, predicate_object_map = query_rml_subj_pred_obj(g, tm, rdfmts)
        if subjectMap is not None:
            tmap = TripleMap(tm, logicalSource, subjectMap, predicate_object_map)
            # print(tmap)
            templates[tm] = tmap

    return templates


def query_rml_subj_pred_obj(g, tm, rdfmts=[]):
    subjectMap = query_rml_subj(g, tm, rdfmts)
    predicate_object_map = query_rml_pred_obj(g, tm)

    return subjectMap, predicate_object_map


def query_rml_subj(g, tm, rdfmts=[]):

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
    res = g.query(subj_query)
    qresults= {}
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
        elif row['constsubject'] is not None :
            subject = row['constsubject'].n3()[1:-1]
            subjTerm = TermMap(subject, TripleMapType.CONSTANT, smtype)
        elif row['smtemplate'] is not None :
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


def query_rml_pred_obj(g, tm):
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


if __name__ == '__main__':
    import time

    current_milli_time = lambda: int(round(time.time() * 1000))

    s = current_milli_time()
    tcfolder = '/home/kemele/git/rml-test-cases/test-cases'
    fn = 'RMLTC0009b'
    print("CSV")
    filename = os.path.join(tcfolder, fn + '-CSV/mapping.ttl')
    templates = query_rml(filename)
    # print("JSON")
    # filename = os.path.join(tcfolder, fn + '-JSON/mapping.ttl')
    # query_rml(filename)
    print("MySQL")
    filename = os.path.join(tcfolder, fn + '-MySQL/mapping.ttl')
    query_rml(filename)
    # print("PostgreSQL")
    # filename = os.path.join(tcfolder, fn + '-PostgreSQL/mapping.ttl')
    # query_rml(filename)
    # print("SQLServer")
    # filename = os.path.join(tcfolder, fn + '-SQLServer/mapping.ttl')
    # query_rml(filename)
    # print("XML")
    # filename = os.path.join(tcfolder, fn + '-XML/mapping.ttl')
    # query_rml(filename)
    #chebi = '/home/kemele/git/SemanticDataLake/Ontario/mappings/LSLOD/LSLOD_RML_Mappings_MySQL/http-==bio2rdf.org=ns=chebi#Compound.ttl'

    print("Total time:", (current_milli_time()-s), 'ms')
    print(templates)