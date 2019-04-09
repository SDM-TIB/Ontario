
__author__ = "Kemele M. Endris"

from enum import Enum
from ontario.model import DataSourceType


class DataSource(object):

    def __init__(self, name, ID, url, dstype, params, mappingfiles, mappings={}):
        self.name = name
        self.ID = ID
        self.url = url
        self.params = params
        self.mappingfiles = mappingfiles
        self.mappings = mappings
        self.tripleMaps = None
        self.dstype = dstype
        if 'SPARQL_Endpoint' in dstype:
            self.dstype = DataSourceType.SPARQL_ENDPOINT
        elif 'MongoDB' in dstype:
            self.dstype = DataSourceType.MONGODB
        elif "Neo4j" in dstype:
            self.dstype = DataSourceType.NEO4J
        elif "HADOOP_CSV" in dstype:
            self.dstype = DataSourceType.HADOOP_CSV
        elif "HADOOP_XML" in dstype:
            self.dstype = DataSourceType.HADOOP_XML
        elif "HADOOP_JSON" in dstype:
            self.dstype = DataSourceType.HADOOP_JSON
        elif "HADOOP_TSV" in dstype:
            self.dstype = DataSourceType.HADOOP_TSV
        elif "REST" in dstype:
            self.dstype = DataSourceType.REST_SERVICE
        elif "LOCAL_CSV" in dstype:
            self.dstype = DataSourceType.LOCAL_CSV
        elif "LOCAL_TSV" in dstype:
            self.dstype = DataSourceType.LOCAL_TSV
        elif "LOCAL_JSON" in dstype:
            self.dstype = DataSourceType.LOCAL_JSON
        elif "LOCAL_XML" in dstype:
            self.dstype = DataSourceType.LOCAL_XML
        elif "SPARK_CSV" in dstype:
            self.dstype = DataSourceType.SPARK_CSV
        elif "SPARK_TSV" in dstype:
            self.dstype = DataSourceType.SPARK_TSV
        elif "SPARK_JSON" in dstype:
            self.dstype = DataSourceType.SPARK_JSON
        elif "MySQL" in dstype or 'mysql' in dstype.lower():
            self.dstype = DataSourceType.MYSQL
        else:
            self.dstype = DataSourceType.SPARQL_ENDPOINT

    def __repr__(self):
        return str({
            "name": self.name,
            "ID": self.ID,
            "url": self.url,
            "dstype": self.dstype.value,
            "params": self.params,
            "mappings": self.mappingfiles
        })


class RDFMT(object):

    def __init__(self, rootType, linkedTo, predicates, datasources, mttype=0):
        self.uri = rootType
        self.mttype = mttype
        self.linkedTo = linkedTo
        self.predicates = predicates
        self.datasources = datasources

    def __repr__(self):
        return str({
            "uri": self.uri,
            "mttype": self.mttype,
            "linkedTo": self.linkedTo,
            "predicates": self.predicates,
            "datasources": self.datasources
        })


class MTPredicate(object):

    def __init__(self, predicate, ranges, cardinality=-1):
        self.predicate = predicate
        self.ranges = ranges
        self.cardinality = cardinality

    def __repr__(self):
        return str({
            "predicate": self.predicate,
            "range": self.ranges
        })


class TermType(Enum):
    TEMPLATE = "template"
    CONSTANT = "constant"
    REFERENCE = "reference"
    TRIPLEMAP = "triplemap"


class RMLMapping(object):

    def __init__(self, source, subject, rmliterator="*", subjectType=TermType.TEMPLATE):
        self.source = source
        self.subject = subject
        self.iterator = rmliterator
        self.subjectType = subjectType
        self.subjectCols = []
        self.predicateObjMap = {}
        if self.subjectType == TermType.TEMPLATE or self.subjectType == TermType.REFERENCE:
            if len(self.subject.split('{')) == 2:
                self.subjectCols.append(self.subject[self.subject.find("{") + 1:self.subject.find("}")])
            else:
                splits = []
                for sp in self.subject.split('{'):
                    if "}" in sp:
                        splits.append(sp[:sp.rfind("}")])
                    else:
                        # if subject type is reference there will be only one value in the list
                        splits.append(sp)

                self.subjectCols = splits if len(self.subject.split('{')) <= 2 else splits[1:]


class RDFMPredicateObjMap(object):

    def __init__(self, predicate, object, objectType=TermType.REFERENCE,
                 objectDatatype="xsd:string", objectRDFClass=None, jchild=None, jparent=None):
        self.predicate = predicate
        self.object = object
        self.objectType = objectType
        self.objectDatatype = objectDatatype
        self.objectRDFClass = objectRDFClass
        self.objectCols = []
        if self.objectType == TermType.TEMPLATE or self.objectType == TermType.REFERENCE:
            if len(self.object.split('{')) == 2:
                self.objectCols.append(self.object[self.object.find("{") + 1:self.object.find("}")])
            else:
                splits = []
                for sp in self.object.split('{'):
                    if "}" in sp:
                        splits.append(sp[:sp.rfind("}")])
                self.objectCols = splits

        self.joinChild = jchild
        self.joinParent = jparent
