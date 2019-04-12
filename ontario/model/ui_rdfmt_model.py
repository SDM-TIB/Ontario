
__author__ = 'Kemele M. Endris'

from ontario.rdfmt.utils import contactRDFSource
from . import DataSourceType
import urllib.parse as urlparse
import datetime

mtonto = "http://tib.eu/dsdl/ontario/ontology/"
mtresource = "http://tib.eu/dsdl/ontario/resource/"
xsd = "http://www.w3.org/2001/XMLSchema#"


class RDFMT(object):

    def __init__(self, rid, name, mt_type=0, sources=[], subClassOf=[], properties=[], desc=''):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.sources = sources
        self.subClassOf = subClassOf
        self.properties = properties
        if desc is not None and "^" in desc:
            desc = desc[:desc.find("^^")]
        if name is not None and "^" in name:
            name = name[:name.find("^^")]
        self.name = name
        self.desc = desc
        self.mt_type = mt_type

    def to_rdf(self):
        data = []
        data.append("<" + self.rid + "> a <" + mtonto + "RDFMT> ")
        if self.mt_type == 0:
            data.append("<" + self.rid + "> a <" + mtonto + "TypedRDFMT> ")
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append("<" + self.rid + "> <" + mtonto + "source> <" + s.rid + ">")
        for s in self.subClassOf:
            data.append("<" + self.rid + "> <" + mtonto + "subClassOf> <" + s + "> ")
        if self.desc is not None and self.desc != '':
            self.desc = self.desc.replace('"', "'").replace("\n", " ")
            data.append("<" + self.rid + "> <" + mtonto + "desc> \"" + self.desc + "\" ")
        if self.name is not None and self.name != "":
            self.name = self.name.replace('"', "'").replace("\n", " ")
            data.append("<" + self.rid + "> <" + mtonto + "name> \"" + self.name + "\" ")
        else:
            data.append("<" + self.rid + "> <" + mtonto + "name> \"" + self.rid + "\" ")

        for r in self.properties:
            data.extend(r.to_rdf())
            data.append("<" + self.rid + "> <" + mtonto + "hasProperty> <" + r.rid + "> ")

        today = str(datetime.datetime.now())
        data.append("<" + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append("<" + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')

        return data


class MTProperty(object):

    def __init__(self, rid, predicate, sources, cardinality=-1, ranges=[], policies=[], label=''):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.predicate = urlparse.quote(predicate, safe="/:#-")
        self.sources = sources
        self.cardinality = cardinality
        self.ranges = ranges
        self.policies = policies

        if label is not None and "^" in label:
            label = label[:label.find("^^")]
        self.label = label

    def to_rdf(self):
        data = []
        data.append("<" + self.rid + "> a <" + mtonto + "MTProperty> ")
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append("<" + self.rid + "> <" + mtonto + "cardinality> " + str(self.cardinality))
        for s in self.sources:
            data.extend(s.to_rdf())
            data.append("<" + self.rid + "> <" + mtonto + "propSource> <" + s.rid + "> ")

        if self.label is not None and self.label != '':
            self.label = self.label.replace('"', "'").replace("\n", " ")
            data.append("<" + self.rid + "> <" + mtonto + "label> \"" + self.label + "\" ")
        if self.predicate is not None:
            data.append("<" + self.rid + "> <" + mtonto + "predicate> <" + self.predicate + "> ")
        for r in self.ranges:
            data.extend(r.to_rdf())
            data.append("<" + self.rid + "> <" + mtonto + "linkedTo> <" + r.rid + "> ")

        for p in self.policies:
            data.extend(p.to_rdf())
            data.append("<" + self.rid + "> <" + mtonto + "policies> <" + p.rid + "> ")

        return data


class PropRange(object):

    def __init__(self, rid, prange, source, range_type=0, cardinality=-1):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.prange = prange
        self.source = source
        self.cardinality = cardinality
        self.range_type = range_type

    def to_rdf(self):
        data = []
        data.append("<" + self.rid + "> a <" + mtonto + "PropRange> ")
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append("<" + self.rid + "> <" + mtonto + "cardinality> " + str(self.cardinality))
        data.append("<" + self.rid + "> <" + mtonto + "datasource> <" + self.source.rid + "> ")
        if self.range_type == 0:
            data.append("<" + self.rid + "> <" + mtonto + "rdfmt> <" + self.prange + "> ")
        else:
            data.append("<" + self.rid + "> <" + mtonto + "xsdtype> <" + self.prange + "> ")

        data.append("<" + self.rid + "> <" + mtonto + "name> <" + self.prange + "> ")
        return data


class Source(object):

    def __init__(self, rid, source, cardinality=-1):
        self.rid = urlparse.quote(rid, safe="/:#-")
        self.source = source
        self.cardinality = cardinality

    def to_rdf(self):
        data = []
        data.append("<" + self.rid + "> a <" + mtonto + "Source> ")
        if self.cardinality != '' and int(self.cardinality) >= 0:
            data.append("<" + self.rid + "> <" + mtonto + "cardinality> " + str(self.cardinality))
        data.append("<" + self.rid + "> <" + mtonto + "datasource> <" + self.source.rid + "> ")

        return data


class DataSource(object):

    def __init__(self, rid, url, dstype, name=None, desc="", params={}, keywords="", homepage="", version="",
                 organization="", ontology_graph=None, triples=-1):

        self.rid = urlparse.quote(rid, safe="/:#-")
        self.url = url
        if isinstance(dstype, DataSourceType):
            self.dstype = dstype
        else:
            if 'SPARQL_Endpoint' in dstype:
                self.dstype = DataSourceType.SPARQL_ENDPOINT
            elif 'MongoDB' in dstype:
                self.dstype = DataSourceType.MONGODB
            elif "Neo4j" in dstype:
                self.dstype = DataSourceType.NEO4J

            elif "SPARK_CSV" in dstype:
                self.dstype = DataSourceType.SPARK_CSV
            elif "SPARK_XML" in dstype:
                self.dstype = DataSourceType.SPARK_XML
            elif "SPARK_JSON" in dstype:
                self.dstype = DataSourceType.SPARK_JSON
            elif "SPARK_TSV" in dstype:
                self.dstype = DataSourceType.SPARK_TSV

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

            elif "MySQL" in dstype:
                self.dstype = DataSourceType.MYSQL
            else:
                self.dstype = DataSourceType.SPARQL_ENDPOINT

        if name is None:
            self.name = self.url
        else:
            self.name = name.replace('"', "'")

        self.desc = desc
        self.params = params
        self.keywords = keywords
        self.homepage = homepage
        self.version = version
        self.organization = organization
        self.triples = triples
        self.ontology_graph = ontology_graph

    def isAccessible(self):
        ask = "ASK {?s ?p ?o}"
        e = self.url
        referer = e
        if self.dstype == DataSourceType.SPARQL_ENDPOINT:
            print("checking endpoint accessibility", e)
            val, c = contactRDFSource(ask, referer)
            if c == -2:
                print(e, '-> is not accessible. Hence, will not be included in the federation!')
            if val:
                return True
            else:
                print(e, "-> is returning empty results. Hence, will not be included in the federation!")

        return False

    def to_rdf(self):

        data = []
        data.append("<" + self.rid + "> a <" + mtonto + "DataSource> ")
        data.append("<" + self.rid + "> <" + mtonto + "dataSourceType> <" + mtresource + 'DatasourceType/' + str(
            self.dstype.value) + "> ")
        data.append("<" + self.rid + "> <" + mtonto + "url> \"" + urlparse.quote(self.url, safe="/:") + "\" ")
        if self.name is not None and self.name != "":
            self.name = self.name.replace('"', "'").replace("\n", " ")
            data.append("<" + self.rid + "> <" + mtonto + "name> \"" + self.name + "\" ")
        if self.version is not None and self.version != "":
            data.append("<" + self.rid + "> <" + mtonto + "version> \"" + self.version + "\" ")
        if self.keywords is not None and self.keywords != "":
            data.append("<" + self.rid + "> <" + mtonto + "keywords> \"" + self.keywords + "\" ")
        if self.organization is not None and self.organization != "":
            data.append("<" + self.rid + "> <" + mtonto + "organization> \"" + self.organization + "\" ")
        if self.homepage is not None and self.homepage != "":
            data.append("<" + self.rid + "> <" + mtonto + "homepage> \"" + self.homepage + "\" ")
        if self.params is not None and len(self.params) > 0:
            data.append("<" + self.rid + "> <" + mtonto + "params> \"" + str(self.params) + "\" ")
        if self.desc is not None and self.desc != "":
            data.append("<" + self.rid + "> <" + mtonto + "desc> \"" + self.desc.replace('"', "'").replace('`', "'") + "\" ")
        if self.triples != '' and int(self.triples) >= 0:
            data.append("<" + self.rid + "> <" + mtonto + "triples> " + self.triples)

        today = str(datetime.datetime.now())
        data.append("<" + self.rid + '>  <http://purl.org/dc/terms/created> "' + today + '"')
        data.append("<" + self.rid + '>  <http://purl.org/dc/terms/modified> "' + today + '"')
        return data

    def __repr__(self):
        return "{" + \
                "\trid: " + self.rid +\
                ",\turl: " + self.url +\
                ",\tdstype: " + str(self.dstype) + \
                ",\tparams: " + str(self.params) + \
                "}"


class ACPolicy(object):

    def __init__(self, authorizedBy, operations, authorizedTo, validFrom=None, validUntil=None, desc=''):
        self.authorizedBy = authorizedBy
        self.authorizedTo = authorizedTo
        self.operations = operations
        self.validFrom = validFrom
        self.validUntil = validUntil
        self.desc = desc


class ACOperation(object):

    def __init__(self, name, desc=''):
        self.name = name
        self.desc = desc


class AppUser(object):

    def __init__(self, name, username, passwd=None, params={}):
        self.name = name
        self.username = username
        self.passwd = passwd
        self.params = params
