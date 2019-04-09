
__author__ = 'Kemele M. Endris'

from enum import Enum


class DataSourceType(Enum):
    # SPARQL_ENDPOINT = "SPARQL_Endpoint"
    #
    # MONGODB = "MongoDB"
    # NEO4J = "Neo4j"
    #
    # MYSQL = "MySQL"
    # POSTGRESQL = "PostgreSQL"
    # SQLSERVER = "SQLServer"
    #
    # HADOOP_CSV = "HADOOP_CSV"
    # HADOOP_TSV = "HADOOP_TSV"
    # HADOOP_JSON = "HADOOP_JSON"
    # HADOOP_XML = "HADOOP_XML"
    #
    # REST_SERVICE = "REST_Service"
    #
    # LOCAL_CSV = "LOCAL_CSV"
    # LOCAL_TSV = "LOCAL_TSV"
    # LOCAL_JSON = "LOCAL_JSON"
    # LOCAL_XML = "LOCAL_XML"
    #
    # SPARK_CSV = "SPARK_CSV"
    # SPARK_TSV = "SPARK_TSV"
    # SPARK_JSON = "SPARK_JSON"
    # SPARK_XML = "SPARK_XML"

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
