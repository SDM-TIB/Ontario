import logging
import rdflib
import json

from tests.rml import query_rml
from ontario.model.rdfmt_model import RDFMT, MTPredicate, DataSource


class OntarioConfiguration(object):

    def __init__(self, filename):
        self.filename = filename
        self.metadata = {}
        self.datasources = {}
        self.predicateMTindex = {}
        self.read_config()

    def read_config(self):
        # try:
        with open(self.filename, "r", encoding='utf8') as f:
            confdata = json.load(f)
            # read data sources first and templates next
            self.ext_datasources(confdata)
            self.ext_templates(confdata)

    def ext_templates(self, confdata):
        if 'templates' in confdata:
            mts = confdata['templates']
            self.metadata = self.ext_templates_json(mts)
        else:
            self.metadata = {}

    def ext_datasources(self, confdata):
        if 'datasources' in confdata:
            ds = confdata['datasources']
            self.datasources = self.ext_datasources_json(ds)
        else:
            self.datasources = {}

    def ext_datasources_json(self, ds):
        datasources = {}
        for d in ds:
            # mappings = self.load_mappings(d['mappings'])
            datasources[d['ID']] = DataSource(d['name'] if 'name' in d else d['ID'],
                                              d['ID'],
                                              d['url'],
                                              d['type'],
                                              d['params'],
                                              d['mappings']
                                              #, mappings
                                              )

        return datasources

    def load_mappings(self, mappingslist, rdfmts=[]):
        if len(mappingslist) > 0:
            return self.read_mapping_files(mappingslist, rdfmts)
        return {}

    def ext_templates_json(self, mts):
        meta = {}
        for m in mts:
            rootType = m['rootType']
            predicates = m['predicates']
            linkedTo = []
            preds = {}
            for p in predicates:
                self.predicateMTindex.setdefault(p['predicate'], set()).add(rootType)
                preds[p['predicate']] = MTPredicate(p['predicate'], p['range'])
                linkedTo.extend(p['range'])
            datasources = m['datasources']
            wrappers = {}
            for w in datasources:
                wrappers[w['datasource']] = w['predicates']
            mt = RDFMT(rootType, linkedTo, preds, wrappers)
            meta[rootType] = mt

        return meta

    def read_mapping_files(self, mappingslist, rdfmts=[]):
        mappings = query_rml(mappingslist, rdfmts)
        return mappings

    def find_rdfmt_by_preds(self, preds):
        res = []
        for p in preds:
            if p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                continue
            if p in self.predicateMTindex:
                res.append(self.predicateMTindex[p])
        for r in res[1:]:
            res[0] = res[0].intersection(r)
        results = {}
        if len(res) > 0:
            mols = list(set(res[0]))
            for m in mols:
                results[m] = self.metadata[m]
        return results
