
__author__ = 'Kemele M. Endris'

from mysql import connector
from mysql.connector import errorcode


class MySQLClient(object):
    def __init__(self, url=None, username=None, passwd=None):
        self.url = url
        self.username = username
        self.password = passwd

        if self.url is None:
            self.url = '127.0.0.1'

        if ':' in self.url:
            host, port = self.url.split(':')
        else:
            host = self.url
            port = '3306'

        try:
            if self.username is None:
                self.client = connector.connect(user='root', host=self.url)
            else:
                self.client = connector.connect(user=username, password=passwd, host=host, port=port)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def list_tables(self):
        results = []
        cursor = self.client.cursor()
        cursor.execute("show databases")
        databases = []
        for d in cursor:
            if d[0] != 'information_schema' and d[0] != 'mysql' and d[0] != "sys" and d[0] != 'performance_schema':
                databases.append(d[0])
        for d in databases:
            cursor.execute("use " + d)
            cursor.execute("show tables")

            tables = [t[0] for t in cursor]

            counter = self.client.cursor()
            counter.execute('use ' + d)
            for t in tables:
                counter.execute('SELECT COUNT(*) FROM ' + t)
                count = [c[0] for c in counter][0]
                row = {
                    "db": d,
                    "document": t,
                    "count": count
                }
                results.append(row)
        return results

    def get_samples(self, dbname, tablename, limit=10):
        cursor = self.client.cursor()
        cursor.execute("use " + dbname)
        cursor.execute("select * from " + tablename + " LIMIT " + str(limit))
        header = [h[0] for h in cursor._description]
        results = [{header[i]: str(line[i]) for i in range(len(line))} for line in cursor]
        return results, len(results)


if __name__ == "__main__":
    neo = MySQLClient("node3.research.tib.eu:13308", "root", "1234")
    results = neo.list_tables()
    from pprint import pprint
    # pprint(results)
    for r in results:
        print("===================")
        pprint(r)
        res, card = neo.get_samples(r["db"], r['document'])
        print("samples ... ")
        pprint(res)
        break

