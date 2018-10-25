import hashlib
import json
from pyspark.sql import SparkSession
from ontario.sparql.parser import queryParser as qp
from mysql import connector
from mysql.connector import errorcode
from multiprocessing import Process, Queue
from ontario.config.model import DataSourceType
from ontario.wrappers.spark.utils import *
from json import load
from ontario.wrappers.hadoop import SparkHDFSClient
from pprint import pprint


class MySQLWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.mysql = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        username = None
        password = None
        if datasource.params is not None and len(datasource.params) > 0:
            if isinstance(datasource.params, dict):
                username = datasource.params['username'] if 'username' in datasource.params else None
                password = datasource.params['password'] if 'password' in datasource.params else None
            else:
                maps = datasource.params.split(';')
                for m in maps:
                    params = m.split(':')
                    if len(params) > 0:
                        if 'username' == params[0]:
                            username = params[1]
                        if 'password' == params[0]:
                            password = params[1]
        if ':' in self.url:
            host, port = self.url.split(':')
        else:
            host = self.url
            port = '3306'
        try:
            if username is None:
                self.mysql = connector.connect(user='root', host=self.url)
            else:
                self.mysql = connector.connect(user=username, password=password, host=host, port=port)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        self.mappings = {tm: self.datasource.mappings[tm] for tm in self.datasource.mappings \
                         for rdfmt in self.rdfmts if rdfmt in self.datasource.mappings[tm]}

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):
        """
        Entry point for query execution on csv files
        :param querystr: string query
        :return:
        """
        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []
        querytxt = query
        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)

        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset
        ds = self.star['datasource']

        # sqlquery, coltotemplates, projvartocols, tablename, filename, exploded = self.translate()
        cursor = self.mysql.cursor()

        queue.put("EOF")
