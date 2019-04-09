
__author__ = 'Kemele M. Endris'

import csv
import os
import xml.etree.ElementTree as ET
from pprint import pprint


class LocalFlatFileClient(object):
    def __init__(self, datasource):
        self.datasource = datasource

    def get_file_list(self, folder):
        files = os.listdir(folder)
        files = [folder + '/' + file for file in files]
        return files

    def list_collections(self):
        results = []
        if os.path.exists(self.datasource.url):
            if os.path.isdir(self.datasource.url):
                files = self.get_file_list(self.datasource.url)
                while len(files) > 0:
                    file = files.pop()
                    if os.path.isdir(os.path.join(self.datasource.url, file)):
                        subfiles = self.get_file_list(os.path.join(self.datasource.url, file))
                        files.extend(subfiles)
                        continue
                    else:
                        if self.datasource.dstype == DataSourceType.LOCAL_CSV and file[-3:] != 'csv' \
                                or self.datasource.dstype == DataSourceType.LOCAL_TSV and file[-3:] != 'tsv'\
                                or self.datasource.dstype == DataSourceType.LOCAL_XML and file[-3:] != 'xml'\
                                or self.datasource.dstype == DataSourceType.LOCAL_JSON and file[-4:] != 'json':
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


class CSVTSVFileClient(object):

    def __init__(self, filename, delimiter=','):
        self.filename = filename
        self.delimiter = delimiter

    def get_documents(self, limit=15):
        results = []
        fp = open(self.filename)
        reader = csv.reader(fp, delimiter=self.delimiter)
        header = None
        rows = 0
        for r in reader:
            if header is None:
                header = r
                continue
            res = {header[i]: r[i] for i in range(len(r))}
            print(res)
            results.append(res)
            rows += 1
            if rows > limit+1:
                break

        return results, limit


class XMLFileClient(object):

    def __init__(self, filename, rootTag='*'):
        self.filename = filename
        self.rootTag = rootTag

    def get_documents(self, limit=10):
        results = []
        tree = ET.parse(self.filename)
        root = tree.getroot()
        db_attr = root.attrib
        l = 0

        parsed = dict()

        for doc in root.iter():
            if doc.text:
                print(doc.text)
            doc_dict = db_attr.copy()
            doc_dict.update(doc.attrib)
            doc_dict['data'] = doc.text
            pprint(doc.attrib)
            pprint(doc_dict)
            for child in list(doc):
                print(child)
            print('---------', doc.tag, '-------------')
            l += 1
            if l >= limit:
                break
        l = 0
        for i, element in enumerate(root):
            print(element)
            for key in element.keys():
                parsed[key] = element.attrib.get(key)

            pprint(parsed)
            l += 1
            if l >= limit:
                break

        return results, limit


if __name__ == "__main__":
    from ontario.config.model import *
    datasource = DataSource("http://drugbank",
                            "/media/kemele/DataHD/Datasets/Ontario-experiment/DrugBank",
                            DataSourceType.LOCAL_XML,
                            name="COSMIC")

    tc = XMLFileClient('/media/kemele/DataHD/Datasets/pubmed18n0001.xml', "*")
    tc.get_documents()
