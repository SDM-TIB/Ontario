
__author__ = 'Kemele M. Endris'

from ontario.sparql.parser import queryParser as qp
from multiprocessing import Queue
from ontario.wrappers.mysql.utils import *
from ontario.sparql.parser.services import Filter, Expression, Argument, unaryFunctor, binaryFunctor
from ontario.model.rml_model import TripleMapType
from pyspark.sql import SparkSession
from ontario.wrappers.sparqltosql import SPARQL2SQL
import json
import hashlib
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('ontario.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class SPARKWrapper(object):

    def __init__(self, datasource, config, rdfmts, star):
        self.datasource = datasource
        self.rdfmts = rdfmts
        self.url = datasource.url
        self.params = datasource.params
        self.config = config
        self.spark = None
        self.df = None
        self.result = None
        self.star = star
        self.query = None
        self.prefixes = {}
        if ':' in self.url:
            self.host, self.port = self.url.split(':')
        else:
            self.host = self.url
            self.port = '7077'

        if len(self.datasource.mappings) == 0:
            self.mappings = self.config.load_mappings(self.datasource.mappingfiles, []) # self.rdfmts
        else:
            # self.mappings = self.config.mappings
            self.mappings = self.datasource.mappings

    def executeQuery(self, query, queue=Queue(), limit=-1, offset=0):
        """
        Entry point for query execution on csv files
        :param querystr: string query
        :return:
        """
        from time import time
        if len(self.mappings) == 0:
            print("Empty Mapping")
            queue.put('EOF')
            return []
        # querytxt = query
        self.query = qp.parse(query)
        self.prefixes = getPrefs(self.query.prefs)

        # query_filters = [f for f in self.query.body.triples[0].triples if isinstance(f, Filter)]

        if limit > -1 or offset > -1:
            self.query.limit = limit
            self.query.offset = offset

        sparql2sql = SPARQL2SQL(query, self.mappings, self.datasource, self.rdfmts, self.star)

        sqlquery, projvartocols, coltotemplates, filenametablename = sparql2sql.translate()
        # sqlquery, projvartocols, coltotemplates, filenametablename = self.translate(query_filters)
        # print(sqlquery)
        if self.spark is None:
            # url = 'spark://node3.research.tib.eu:7077' # self.mapping['url']
            # params = {
            #     "spark.driver.cores": "4",
            #     "spark.executor.cores": "4",
            #     "spark.cores.max": "6",
            #     "spark.default.parallelism": "4",
            #     "spark.executor.memory": "6g",
            #     "spark.driver.memory": "6g",
            #     "spark.driver.maxResultSize": "6g",
            #     "spark.python.worker.memory": "4g",
            #     "spark.local.dir": "/tmp",
            #     "spark.debug.maxToStringFields": "500"
            # }
            params = self.params
            start = time()
            # self.config['params']
            self.spark = SparkSession.builder.master('local[*]') \
                .appName("OntarioSparkWrapper" + str(self.datasource.url) + query)
            for p in params:
                self.spark = self.spark.config(p, params[p])

            self.spark = self.spark.getOrCreate()
            logger.info("SPARK Initialization cost:" + str(time()-start))
        start = time()

        for filename, tablename in filenametablename.items():
            # schema = self.make_schema(schemadict[filename])
            # filename = "hdfs://node3.research.tib.eu:9000" + filename
            # filename = self.datasource.url + "/" + filename
            # filename = "/media/kemele/DataHD/LSLOD-flatfiles/" + filename
            # print(filename, tablename)
            if self.datasource.dstype == DataSourceType.LOCAL_JSON or \
                    self.datasource.dstype == DataSourceType.SPARK_JSON:
                df = self.spark.read.json(filename)
            elif self.datasource.dstype == DataSourceType.HADOOP_JSON:
                filename = "hdfs://node3.research.tib.eu:9000" + filename
                df = self.spark.read.json(filename)
            elif self.datasource.dstype == DataSourceType.HADOOP_TSV or \
                    self.datasource.dstype == DataSourceType.HADOOP_CSV:
                filename = "hdfs://node3.research.tib.eu:9000" + filename
                df = self.spark.read.csv(filename, inferSchema=True, sep='\t'
                                         if self.datasource.dstype == DataSourceType.HADOOP_TSV else ',',
                                         header=True)
            else:
                df = self.spark.read.csv(filename, inferSchema=True,
                                         sep='\t' if self.datasource.dstype == DataSourceType.LOCAL_TSV or
                                                     self.datasource.dstype == DataSourceType.SPARK_TSV else ',',
                                         header=True)

            df.createOrReplaceTempView(tablename)

        logger.info("time for reading file" + str(filenametablename) + str(time() - start))

        totalres = 0
        try:
            runstart = time()
            if isinstance(sqlquery, list):# and len(sqlquery) > 3:
                sqlquery = " UNION ".join(sqlquery)
            # print(sqlquery)
            if isinstance(sqlquery, list):
                logger.info(" UNION ".join(sqlquery))
                res_dict = []
                for sql in sqlquery:
                    cardinality = self.process_result(sql, queue, projvartocols, coltotemplates, res_dict)
                    totalres += cardinality
            else:
                logger.info(sqlquery)
                cardinality = self.process_result(sqlquery, queue, projvartocols, coltotemplates)
                totalres += cardinality
            logger.info("Exec in SPARK took:" + str(time() - runstart))
        except Exception as e:
            print("Exception ", e)
            pass
        # print('SPARK End:', time(), "Total results:", totalres)
        # print("SPARK finished after: ", (time()-start))
        queue.put("EOF")
        self.spark.stop()

    def process_result(self, sql, queue, projvartocols, coltotemplates, res_dict=None):
        c = 0
        result = self.spark.sql(sql).toJSON()
        # print('count-----',result.count())
        for row in result.collect():
            c += 1
            row = json.loads(row)
            # if res_dict is not None:
            #     rowtxt = ",".join(list(row.values()))
            #     if rowtxt in res_dict:
            #         continue
            #     else:
            #         res_dict.append(rowtxt)

            res = {}
            skip = False
            for r in row:
                if row[r] == 'null':
                    skip = True
                    break
                if not isinstance(row[r], str):
                    row[r] = str(row[r])
                if '_' in r and r[:r.find("_")] in projvartocols:
                    s = r[:r.find("_")]
                    if s in res:
                        val = res[s]
                        if 'http://' in row[r]:
                            res[s] = row[r]
                        else:
                            res[s] = val.replace('{' + r[r.find("_") + 1:] + '}', row[r].replace(" ", '_'))
                    else:
                        if 'http://' in r:
                            res[s] = r
                        else:
                            res[s] = coltotemplates[s].replace('{' + r[r.find("_") + 1:] + '}',
                                                               row[r].replace(" ", '_'))
                elif r in projvartocols and r in coltotemplates:
                    if 'http://' in row[r]:
                        res[r] = row[r]
                    else:
                        res[r] = coltotemplates[r].replace('{' + projvartocols[r] + '}', row[r].replace(" ", '_'))
                else:
                    res[r] = row[r]

            if not skip:
                queue.put(res)
                # if 'keggCompoundId' in res:
                #     print(res['keggCompoundId'])
        return c
