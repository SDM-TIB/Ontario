
__author__ = 'Kemele M. Endris'

from enum import Enum


class TripleMap(object):

    def __init__(self, ID, logicalSource, subjectMap, predicate_object_map=None):
        self.ID = ID
        self.logical_source = logicalSource
        self.subject_map = subjectMap
        self.predicate_object_map = predicate_object_map

    def __repr__(self):
        pos = [str(self.predicate_object_map[s][0]).strip() + '=>' + str(self.predicate_object_map[s][1]).strip()
               for s in self.predicate_object_map]
        return self.ID + "=> " \
               + "\nLogical Source:" + str(self.logical_source) \
               + "\nSubjectMap: " + str(self.subject_map) \
               + "\nPredicateObjectMap:\n\t" + "\n\t".join(pos)


class RMLDataSource(object):
    def __init__(self, ID, name, dsType=None):
        self.ID = ID
        self.ds_type = dsType
        self.ds_desc = {}
        self.name = name

    def __repr__(self):
        desc = [k + ':' + v for k, v in self.ds_desc.items()]
        return (self.ID if self.ID is not None else "") + \
               ("\t(" + self.ds_type.value + ') ' if self.ds_type is not None else "") + \
               (("\n\t\t" + "\n".join(desc)) if self.ds_desc is not None else '')


class LogicalSource(object):
    def __init__(self, ID, source, iterator=None, ref_form=None):
        self.ID = ID
        self.iterator = iterator
        self.ref_formulation = ref_form
        self.data_source = source

        self.sqlVersion = None
        self.query = None
        self.table_name = None

    def __repr__(self):
        return '\n\tSource: ' + str(self.data_source) + \
                ("\titerator: " + self.iterator if self.iterator is not None else "") + \
                ("\n\t\treferenceFormulation: " + self.ref_formulation if self.ref_formulation is not None else "") + \
               ("\n\t\tSQLVersion: " + self.sqlVersion if self.sqlVersion is not None else "") + \
               ("\n\t\tTableName: " + self.table_name if self.table_name is not None else "") + \
               ("\n\t\tQuery: " + self.query if self.query is not None else "")


class TermType(Enum):
    IRI = "http://www.w3.org/ns/r2rml#IRI"
    BNode = "http://www.w3.org/ns/r2rml#BlankNode"
    Literal = "http://www.w3.org/ns/r2rml#Literal"


class TripleMapType(Enum):
    TEMPLATE = "Template"
    CONSTANT = "Constant"
    REFERENCE = "Reference"
    TRIPLEMAP = "TripleMap"


class SubjectMap(object):
    def __init__(self, ID, subject, rdfTypes):
        self.ID = ID
        self.subject = subject
        self.rdf_types = rdfTypes

    def __repr__(self):
        return (str(self.subject) if self.subject is not None else "") \
              + "\t" + ("RDFTypes:" + str(self.rdf_types) if self.rdf_types is not None else "")


class PredicateMap(object):
    def __init__(self, ID, predicate):
        self.ID = ID
        self.predicate = predicate

    def __repr__(self):
        return str(self.predicate)


class ObjectMap(object):
    def __init__(self, ID, theobject, datatype=None, language=None, objRdfType=None):
        self.ID = ID
        self.objectt = theobject
        self.data_type = datatype
        self.language = language
        self.obj_rdf_type = objRdfType

    def __repr__(self):
        return str(self.objectt) + " \n" \
               + ' \t' + (self.data_type if self.data_type is not None else "") \
               + '\t' + (self.obj_rdf_type if self.obj_rdf_type is not None else "") \
               + "\t" + (self.language if self.language is not None else "")


class TermMap(object):
    def __init__(self, value, resource_type, term_type):
        self.value = value
        self.resource_type = resource_type
        self.term_type = term_type

    def __repr__(self):
        return self.value  # + " " + self.term_map_type

    def __str__(self):
        return self.value  # + " " + self.term_map_type.value


class ObjectReferenceMap(object):
    def __init__(self, parentmap, child_column=None, parent_column=None):
        self.parent_map = parentmap
        self.child_column = child_column
        self.parent_column = parent_column
        self.term_type = TermType.IRI
        self.value = self.parent_map

    def __repr__(self):
        return str(self.value) + '(' \
               + (self.child_column if self.child_column is not None else "") + '->' \
               + (self.parent_column if self.parent_column is not None else "") + ')'
