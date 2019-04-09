
__author__ = 'Kemele M. Endris'

from hdfs import Config
from ontario.wrappers.spark.utils import *
from json import load
import os


class SparkHDFSClient(object):
    def __init__(self, datasource):
        self.datasource = datasource
        self.client = Config().get_client("dev")

    def get_file_list(self, folder):
        files = self.client.list(folder.strip())
        files = [folder + '/' + file for file in files]
        return files

    def list_collections(self):
        results = []
        status = self.client.status(self.datasource.url, strict=False)
        print(status, self.datasource.url)
        if status is not None:
            if status['type'] == "DIRECTORY":
                files = self.get_file_list(self.datasource.url)
                while len(files) > 0:
                    file = files.pop()
                    status = self.client.status(os.path.join(self.datasource.url, file), strict=False)
                    if status is None:
                        continue
                    if status['type'] == "DIRECTORY":
                        subfiles = self.get_file_list(os.path.join(self.datasource.url, file))
                        files.extend(subfiles)
                        continue
                    else:
                        if self.datasource.dstype == DataSourceType.SPARK_CSV and file[-2:] != 'sv' \
                                or self.datasource.dstype == DataSourceType.SPARK_TSV and file[-2:] != 'sv'\
                                or self.datasource.dstype == DataSourceType.SPARK_XML and file[-3:] != 'xml'\
                                or self.datasource.dstype == DataSourceType.SPARK_JSON and file[-4:] != 'json':
                            continue
                        row = {
                            "db": file[:file.rfind('/')] if '/' in file else self.datasource.url,
                            "document": file[file.rfind('/')+1:] if '/' in file else file,
                            "count": -1
                        }
                        results.append(row)

                return results
            else:
                return [{"db": self.datasource.url, "document": self.datasource.url, "count": -1}]
        else:
            return results

    def get_documents(self, filename, limit=10):
        results = []
        delimiter = "\n"
        header = None
        rows = 0
        if self.datasource.dstype == DataSourceType.SPARK_CSV or \
                self.datasource.dstype == DataSourceType.SPARK_TSV:
            delimiter = "\n"
            with self.client.read(filename, encoding='utf-8', delimiter=delimiter) as reader:
                for line in reader:
                    if len(line.strip()) == 0 or line[0] == '#':
                        continue
                    if filename[-3:] == "csv":
                        line = line.split(',')
                    else:
                        line = line.split('\t')

                    if header is None:
                        header = line
                        continue
                    res = {header[i]: line[i] for i in range(len(line)) if i < len(header)}
                    results.append(res)
                    rows += 1
                    if rows > limit + 1:
                        break
        elif self.datasource.dstype == DataSourceType.SPARK_XML:
            with self.client.read(filename, encoding='utf-8', chunk_size=2048) as reader:
                header = ['content']
                for chunk in reader:
                    res = {'content': str(chunk)}
                    results.append(res)
                    print(results)
                    break
        elif self.datasource.dstype == DataSourceType.SPARK_JSON:
            with self.client.read(filename, encoding='utf-8') as reader:
                model = load(reader)
                if isinstance(model, list):
                    model = [{p: str(list(md[p][0].keys())) if isinstance(md[p], list) and isinstance(md[p][0], dict)
                        else str(model[p])
                        if isinstance(md[p], list) else str(list(md[p].keys())) if isinstance(md[p], dict) else md[p] for p in md} for md in model]
                    results.extend(model)
                else:
                    model = {p: str(list(model[p][0].keys())) if isinstance(model[p], list) and isinstance(model[p][0], dict)
                        else model[p] if isinstance(model[p], list) else str(list(model[p].keys())) if isinstance(model[p], dict) else model[p] for p in model}
                    results.append(model)

        return results[:limit], limit
