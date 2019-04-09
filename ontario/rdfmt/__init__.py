#!/usr/bin/env python3

__author__ = 'Kemele M. Endris'

import hashlib
import pprint as pp
import pprint
from multiprocessing import Queue, Process
from multiprocessing.queues import Empty
import logging
import time
from pprint import pprint
import datetime

from ontario.model.rdfmt_model import *
from ontario.rdfmt.utils import contactRDFSource, updateRDFSource


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger("rdfmts")
logger.setLevel(logging.INFO)
fileHandler = logging.FileHandler("{0}/{1}.log".format('.', 'ontario-rdfmts-log'))
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

xsd = "http://www.w3.org/2001/XMLSchema#"
owl = ""
rdf = ""
rdfs = "http://www.w3.org/2000/01/rdf-schema#"
mtonto = "http://tib.eu/dsdl/ontario/ontology/"
mtresource = "http://tib.eu/dsdl/ontario/resource/"

metas = ['http://www.w3.org/ns/sparql-service-description',
         'http://www.openlinksw.com/schemas/virtrdf#',
         'http://www.w3.org/2000/01/rdf-schema#',
         'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
         'http://purl.org/dc/terms/Dataset',
         'http://bio2rdf.org/dataset_vocabulary:Endpoint',
         'http://www.w3.org/2002/07/owl#',
         "http://purl.org/goodrelations/",
         'http://www.ontologydesignpatterns.org/ont/d0.owl#',
         'http://www.wikidata.org/',
         'http://dbpedia.org/ontology/Wikidata:',
         'http://dbpedia.org/class/yago/',
         "http://rdfs.org/ns/void#",
         'http://www.w3.org/ns/dcat',
         'http://www.w3.org/2001/vcard-rdf/',
         'http://www.ebusiness-unibw.org/ontologies/eclass',
         "http://bio2rdf.org/bio2rdf.dataset_vocabulary:Dataset",
         'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/',
         'nodeID://']


class RDFMTMgr(object):

    def __init__(self, queryurl, updateurl, user, passwd, graph):
        self.graph = graph
        self.queryendpoint = queryurl
        self.updateendpoint = updateurl
        self.user = user
        self.passwd = passwd

    def create(self, ds, outqueue=Queue(), types=[], isupdate=False):

        endpoint = ds.url
        logger.info('----------------------' + endpoint + '-------------------------------------')

        if not isupdate:
            # Get #triples of a dataset
            triples = self.get_cardinality(endpoint)
            if isinstance(triples, str) and '^' in triples:
                triples = int(triples[:triples.find('^^')])
            ds.triples = triples

            data = ds.to_rdf()
            self.updateGraph(data)
        else:
            today = str(datetime.datetime.now())
            data = ["<" + ds.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
            self.updateGraph(data)

        results = self.get_rdfmts(ds, types)
        # self.create_inter_ds_links(datasource=ds)
        outqueue.put('EOF')

        return results

    def get_rdfmts(self, ds, types=[]):
        results = self.extractMTLs(ds, types)

        return results

    def extractMTLs(self, datasource, types=[]):
        rdfmolecules = {}
        endpoint = datasource.url

        if datasource.ontology_graph is None:
            results = self.get_typed_concepts(datasource, -1, types)
        else:
            results = self.get_mts_from_owl(datasource, datasource.ontology_graph, -1, types)

        rdfmolecules[endpoint] = results

        pp.pprint(results)
        logger.info("*****" + endpoint + " ***********")
        logger.info("*************finished *********************")

        return rdfmolecules

    def get_typed_concepts(self, e, limit=-1, types=[]):
        """
        Entry point for extracting RDF-MTs of an endpoint.
        Extracts list of rdf:Class concepts and predicates of an endpoint
        :param endpoint:
        :param limit:
        :return:
        """
        endpoint = e.url
        referer = endpoint
        reslist = []
        if len(types) == 0:
            #  "Optional {?t  <" + rdfs + "label> ?label} .  "Optional {?t  <" + rdfs + "comment> ?desc} .
            query = "SELECT DISTINCT ?t ?label " \
                    " WHERE{  ?s a ?t.  " \
                    "Optional {?t  <" + rdfs + "label> ?label FILTER langMatches( lang(?label), \"EN\" ) } }"
                    # filter (regex(str(?t), 'http://dbpedia.org/ontology', 'i'))
                    #
            if limit == -1:
                limit = 100
                offset = 0
                numrequ = 0
                while True:
                    query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                    res, card = contactRDFSource(query_copy, referer)
                    numrequ += 1
                    if card == -2:
                        limit = limit // 2
                        limit = int(limit)
                        if limit < 1:
                            break
                        continue
                    if card > 0:
                        reslist.extend(res)
                    if card < limit:
                        break
                    offset += limit
                    time.sleep(5)
            else:
                reslist, card = contactRDFSource(query, referer)

            toremove = []
            # [toremove.append(r) for v in metas for r in reslist if v in r['t']]
            for r in reslist:
                for m in metas:
                    if m in str(r['t']):
                        toremove.append(r)

            for r in toremove:
                reslist.remove(r)
        else:
            reslist = [{'t': t} for t in types]

        logger.info(endpoint)
        logger.info(reslist)
        pprint(reslist)

        results = []
        alreadyprocessed = []
        for r in reslist:
            t = r['t']
            if "^^" in t:
                continue
            if t in alreadyprocessed:
                continue
            print(t)
            print("---------------------------------------")
            alreadyprocessed.append(t)
            card = self.get_cardinality(endpoint, t)
            if isinstance(card, str) and '^' in card:
                card = int(card[:card.find('^^')])

            # molecules[m]['wrappers'][0]['cardinality'] = card
            if isinstance(card, str) and "^^" in card:
                mcard = card[:card.find("^^")]
            else:
                mcard = str(card)

            sourceURI = mtresource + str(hashlib.md5(str(endpoint + t).encode()).hexdigest())
            source = Source(sourceURI, e, mcard)
            # GEt subclasses:
            subc = self.get_subclasses(endpoint, t)
            subclasses = []
            if subc is not None:
                subclasses = [r['subc'] for r in subc]

            rdfpropteries = []
            # Get predicates of the molecule t
            preds = self.get_predicates(referer, t)
            propertiesprocessed = []
            for p in preds:
                rn = {"t": t, "cardinality": mcard, "subclasses": subclasses}
                pred = p['p']
                if pred in propertiesprocessed:
                    continue

                propertiesprocessed.append(pred)

                mtpredicateURI = mtresource + str(hashlib.md5(str(t + pred).encode()).hexdigest())
                propsourceURI = mtresource + str(hashlib.md5(str(endpoint + t + pred).encode()).hexdigest())
                # Get cardinality of this predicate from this RDF-MT
                predcard = self.get_cardinality(endpoint, t, prop=pred)
                if isinstance(predcard, str) and "^^" in predcard:
                    predcard = predcard[:predcard.find("^^")]
                else:
                    predcard = str(predcard)

                print(pred, predcard)
                rn['p'] = pred
                rn['predcard'] = predcard

                # Get range of this predicate from this RDF-MT t
                rn['range'] = self.get_rdfs_ranges(referer, pred)
                if len(rn['range']) == 0:
                    rn['r'] = self.find_instance_range(referer, t, pred)
                    mtranges = list(set(rn['range'] + rn['r']))
                else:
                    mtranges = rn['range']
                ranges = []

                for mr in mtranges:
                    if '^^' in mr:
                        continue
                    mrpid = mtresource + str(hashlib.md5(str(endpoint + t + pred + mr).encode()).hexdigest())
                    rcard = -1
                    if xsd not in mr:
                        rcard = self.get_cardinality(endpoint, t, prop=pred, mr=mr)
                        rtype = 0
                    else:
                        rcard = self.get_cardinality(endpoint, t, prop=pred, mr=mr, mrtype=mr)
                        rtype = 1

                    if isinstance(rcard, str) and "^^" in rcard:
                        rcard = rcard[:rcard.find("^^")]
                    else:
                        rcard = str(rcard)
                    ran = PropRange(mrpid, mr, e, range_type=rtype, cardinality=rcard)
                    ranges.append(ran)
                if 'label' in p:
                    plab = p['label']
                else:
                    plab = ''

                predsouce = Source(propsourceURI, e, predcard)
                mtprop = MTProperty(mtpredicateURI, pred, [predsouce], ranges=ranges, label=plab)
                rdfpropteries.append(mtprop)

                results.append(rn)

            name = r['label'] if 'label' in r else t
            desc = r['desc'] if "desc" in r else None

            mt = RDFMT(t, name, properties=rdfpropteries, desc=desc, sources=[source], subClassOf=subclasses)
            data = mt.to_rdf()
            self.updateGraph(data)

        return results

    def get_rdfs_ranges(self, referer, p, limit=-1):

        RDFS_RANGES = " SELECT DISTINCT ?range" \
                      "  WHERE{ <" + p + "> <http://www.w3.org/2000/01/rdf-schema#range> ?range. " \
                                         "} "
        #
        # " " \

        reslist = []
        if limit == -1:
            limit = 100
            offset = 0
            numrequ = 0
            while True:
                query_copy = RDFS_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    # print "setting limit to: ", limit
                    if limit < 1:
                        break
                    continue
                if card > 1:
                    reslist.extend(res)
                if card < limit:
                    break
                offset += limit
                time.sleep(2)
        else:
            reslist, card = contactRDFSource(RDFS_RANGES, referer)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if m in r['range']:
                    skip = True
                    break
            if not skip:
                ranges.append(r['range'])

        return ranges

    def find_instance_range(self, referer, t, p, limit=-1):

        INSTANCE_RANGES = " SELECT DISTINCT ?r WHERE{ ?s a <" + t + ">. " \
                            " ?s <" + p + "> ?pt. " \
                            " ?pt a ?r . } "
        #
        #
        reslist = []
        if limit == -1:
            limit = 50
            offset = 0
            numrequ = 0
            while True:
                query_copy = INSTANCE_RANGES + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    # print "setting limit to: ", limit
                    if limit < 1:
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                if card < limit:
                    break
                offset += limit
                time.sleep(2)
        else:
            reslist, card = contactRDFSource(INSTANCE_RANGES, referer)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if m in r['r']:
                    skip = True
                    break
            if not skip:
                ranges.append(r['r'])

        return ranges

    def get_predicates(self, referer, t, limit=-1):
        """
        Get list of predicates of a class t

        :param referer: endpoint
        :param server: server address of an endpoint
        :param path:  path in an endpoint (after server url)
        :param t: RDF class Concept extracted from an endpoint
        :param limit:
        :return:
        """
        #
        query = " SELECT DISTINCT ?p ?label WHERE{ ?s a <" + t + ">. ?s ?p ?pt.  " \
                " Optional {?p  <" + rdfs + "label> ?label FILTER langMatches( lang(?label), \"EN\" )}} "
        reslist = []
        if limit == -1:
            limit = 50
            offset = 0
            numrequ = 0
            print(t)
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                # print "predicates card:", card
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    # print "setting limit to: ", limit
                    if limit < 1:
                        print("giving up on " + query)
                        print("trying instances .....")
                        rand_inst_res = self.get_preds_of_random_instances(referer, t)
                        existingpreds = [r['p'] for r in reslist]
                        for r in rand_inst_res:
                            if r not in existingpreds:
                                reslist.append({'p': r})
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                if card < limit:
                    break
                offset += limit
                time.sleep(2)
        else:
            reslist, card = contactRDFSource(query, referer)

        return reslist

    def get_preds_of_random_instances(self, referer, t, limit=-1):

        """
        get a union of predicated from 'randomly' selected 10 entities from the first 100 subjects returned

        :param referer: endpoint
        :param server:  server name
        :param path: path
        :param t: rdf class concept of and endpoint
        :param limit:
        :return:
        """
        query = " SELECT DISTINCT ?s WHERE{ ?s a <" + t + ">. } "
        reslist = []
        if limit == -1:
            limit = 50
            offset = 0
            numrequ = 0
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                # print "rand predicates card:", card
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    # print "setting limit to: ", limit
                    if limit < 1:
                        break
                    continue
                if numrequ == 100:
                    break
                if card > 0:
                    import random
                    rand = random.randint(0, card - 1)
                    inst = res[rand]
                    inst_res = self.get_preds_of_instance(referer, inst['s'])
                    inst_res = [r['p'] for r in inst_res]
                    reslist.extend(inst_res)
                    reslist = list(set(reslist))
                if card < limit:
                    break
                offset += limit
                time.sleep(5)
        else:
            reslist, card = contactRDFSource(query, referer)

        return reslist

    def get_preds_of_instance(self, referer, inst, limit=-1):
        query = " SELECT DISTINCT ?p ?label WHERE{ <" + inst + "> ?p ?pt." \
                " Optional {?p  <" + rdfs + "label> ?label FILTER langMatches( lang(?label), \"EN\" )} } "
        reslist = []
        if limit == -1:
            limit = 1000
            offset = 0
            numrequ = 0
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                # print "inst predicates card:", card
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    # print "setting limit to: ", limit
                    if limit < 1:
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                if card < limit:
                    break
                offset += limit
                time.sleep(2)
        else:
            reslist, card = contactRDFSource(query, referer)

        return reslist

    def get_mts_from_owl(self, e, graph, limit=-1, types=[]):
        endpoint = e.url
        referer = endpoint
        reslist = []
        query = "SELECT DISTINCT ?t ?p ?range ?plabel ?tlabel " \
                " WHERE{ graph <" + graph + ">{ ?p  <" + rdfs + "domain> ?t." \
                        "Optional {?p <"+rdfs+"range> ?range}" \
                        "Optional {?p <" + rdfs + "label> ?plabel. filter langMatches(?plabel, 'EN')}" \
                        "Optional {?t <" + rdfs + "label> ?tlabel. filter langMatches(?tlabel, 'EN')}" \
                "}}"  # filter (regex(str(?t), 'http://dbpedia.org/ontology', 'i'))
        #
        if limit == -1:
            limit = 50
            offset = 0
            numrequ = 0
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, referer)
                numrequ += 1
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    if limit < 1:
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                if card < limit:
                    break
                print(limit, offset)
                offset += limit
                time.sleep(5)
        else:
            reslist, card = contactRDFSource(query, referer)

        toremove = []
        # [toremove.append(r) for v in metas for r in reslist if v in r['t']]
        for r in reslist:
            for m in metas:
                if m in str(r['t']):
                    toremove.append(r)

        for r in toremove:
            reslist.remove(r)

        logger.info(endpoint)
        # logger.info(reslist)
        results = []
        alreadyprocessed = {}
        mts = {}
        for r in reslist:
            t = r['t']
            if "^^" in t:
                continue

            subclasses = []
            if t not in alreadyprocessed:
                # card = self.get_cardinality(endpoint, t)
                # if isinstance(card, str) and '^' in card:
                #     card = int(card[:card.find('^^')])
                #
                # # molecules[m]['wrappers'][0]['cardinality'] = card
                # if isinstance(card, str) and "^^" in card:
                #     mcard = card[:card.find("^^")]
                # else:
                #     mcard = str(card)
                mcard = -1
                print(t)
                sourceURI = mtresource + str(hashlib.md5(str(endpoint + t).encode()).hexdigest())
                source = Source(sourceURI, e, mcard)
                alreadyprocessed[t] = mcard
                subc = self.get_subclasses(endpoint, t)
                subclasses = [r['subc'] for r in subc]
                name = r['tlabel'] if 'tlabel' in r else t
                desc = r['tdesc'] if "tdesc" in r else None
                mts[t] = {"name": name, "properties": [], "desc": desc, "sources": [source], "subClassOf": subclasses}

            else:
                mcard = alreadyprocessed[t]

            pred = r['p']
            print(pred)
            rn = {"t": t, "cardinality": mcard, "subclasses": subclasses}
            mtpredicateURI = mtresource + str(hashlib.md5(str(t + pred).encode()).hexdigest())
            propsourceURI = mtresource + str(hashlib.md5(str(endpoint + t + pred).encode()).hexdigest())
            # Get cardinality of this predicate from this RDF-MT
            predcard = -1# self.get_cardinality(endpoint, t, prop=pred)
            if isinstance(predcard, str) and "^^" in predcard:
                predcard = predcard[:predcard.find("^^")]
            else:
                predcard = str(predcard)

            #print(pred, predcard)
            rn['p'] = pred
            rn['predcard'] = predcard

            # Get range of this predicate from this RDF-MT t
            rn['range'] = [] # self.get_rdfs_ranges(referer, pred)

            ranges = []
            if 'range' in r and xsd not in r['range']:
                print('range', r['range'])
                rn['range'].append(r['range'])
                mr = r['range']
                mrpid = mtresource + str(hashlib.md5(str(endpoint + t + pred + mr).encode()).hexdigest())

                if xsd not in mr:
                    rcard = -1#self.get_cardinality(endpoint, t, prop=pred, mr=mr)
                    rtype = 0
                else:
                    rcard = -1#self.get_cardinality(endpoint, t, prop=pred, mr=mr, mrtype=mr)
                    rtype = 1

                if isinstance(rcard, str) and "^^" in rcard:
                    rcard = rcard[:rcard.find("^^")]
                else:
                    rcard = str(rcard)
                ran = PropRange(mrpid, mr, e, range_type=rtype, cardinality=rcard)
                ranges.append(ran)
            if 'plabel' in r:
                plab = r['plabel']
            else:
                plab = ''

            predsouce = Source(propsourceURI, e, predcard)
            mtprop = MTProperty(mtpredicateURI, pred, [predsouce], ranges=ranges, label=plab)
            mts[t]['properties'].append(mtprop)
            # rdfpropteries.append(mtprop)

            results.append(rn)

        for t in mts:
            mt = RDFMT(t, mts[t]['name'], mts[t]['properties'], mts[t]['desc'], mts[t]['sources'], mts[t]['subClassOf'])
            data = mt.to_rdf()
            self.updateGraph(data)

        return results

    def updateGraph(self, data):
        i = 0
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(data), 49):
            if i + 49 > len(data):
                updatequery = "INSERT DATA { GRAPH <" + self.graph + ">{ " + " . \n".join(data[i:]) + "} }"
            else:
                updatequery = "INSERT DATA { GRAPH <" + self.graph + ">{ " + " . \n".join(data[i:i + 49]) + "} }"
            # logger.info(updatequery)
            updateRDFSource(updatequery, self.updateendpoint)
        if i < len(data) + 49:
            updatequery = "INSERT DATA { GRAPH <" + self.graph + ">{ " + " . \n".join(data[i:]) + "} }"
            # logger.info(updatequery)
            updateRDFSource(updatequery, self.updateendpoint)

    def delete_insert_data(self, delete, insert, where=[]):
        i = 0
        updatequery = "WITH <" + self.graph + "> DELETE {"
        # Virtuoso supports only 49 triples at a time.
        for i in range(0, len(delete), 49):
            if i + 49 > len(delete):
                updatequery += " . \n".join(delete[i:]) + "} " \
                               "INSERT {" + " . \n".join(insert[i:]) + "} " \
                                "WHERE {" + " . \n".join(where[i:]) + "}"
            else:
                updatequery += " . \n".join(delete[i:i + 49]) + "} " \
                               "INSERT {" + " . \n".join(insert[i:i + 49]) + "} " \
                                "WHERE {" + " . \n".join(where[i:i + 49]) + "}"
            # logger.info(updatequery)
            updateRDFSource(updatequery, self.updateendpoint)
        updatequery = "WITH <" + self.graph + "> DELETE {"
        if i < len(delete) + 49:
            updatequery += " . \n".join(delete[i:]) + "} " \
                               "INSERT {" + " . \n".join(insert[i:]) + "} " \
                               "WHERE {" + " . \n".join(where[i:]) + "}"
            # logger.info(updatequery)
            updateRDFSource(updatequery, self.updateendpoint)

    def get_cardinality(self, endpoint, mt=None, prop=None, mr=None, mrtype=None):
        if mt is None:
            query = "SELECT (COUNT(*) as ?count) WHERE{?s ?p ?o }"
        elif prop is None:
            query = "SELECT (COUNT(?s) as ?count) WHERE{?s a <" + mt.replace(" ", "_") + "> }"
        else:
            if mr is None:
                query = "SELECT (COUNT(?s) as ?count) WHERE{?s a <" + mt.replace(" ", "_") + "> . ?s <" + prop + "> ?o}"
            else:
                if mrtype is None:
                    query = "SELECT (COUNT(?s) as ?count) WHERE{?s a <" + mt.replace(" ", "_") +\
                            "> . ?s <" + prop + "> ?o . ?o a <" + mr.replace(" ", "_") + ">}"
                else:
                    query = "SELECT (COUNT(?s) as ?count) WHERE{?s a <" + mt.replace(" ", "_") + \
                            "> . ?s <" + prop + "> ?o . filter((datatype(?o))=<" + mr.replace(" ", "_") + ">)}"

        res, card = contactRDFSource(query, endpoint)
        if res is not None and len(res) > 0 and len(res[0]['count']) > 0:
            return res[0]['count']

        return -1

    def get_subclasses(self, endpoint, root):
        referer = endpoint
        query = "SELECT DISTINCT ?subc WHERE{<" + root.replace(" ", "_") + "> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?subc }"
        res, card = contactRDFSource(query, referer)
        return res

    def get_sources(self):
        query = " SELECT distinct ?subject ?url" \
                "  WHERE { " \
                "        GRAPH <" + self.graph + "> { " \
                                            "  ?subject a <http://tib.eu/dsdl/ontario/ontology/DataSource> . " \
                                            "  ?subject <http://tib.eu/dsdl/ontario/ontology/url> ?url." \
                                            "} } "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(5)

        return reslist

    def get_source(self, dsid):
        query = " SELECT distinct * " \
                "  WHERE { " \
                "        GRAPH <" + self.graph + "> { " \
                                           "<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/url> ?url. " \
                                           "<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype. " \
                                           " optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/name> ?name} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/version> ?version} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/organization> ?organization} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/params> ?params} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/desc> ?desc} " \
                                           "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/triples> ?triples} " \
                                           "} } "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit

        return reslist

    def get_ds_rdfmts(self, datasource):
        query = " SELECT distinct ?subject ?card" \
                "  WHERE { " \
                "        GRAPH <" + self.graph + "> { " \
                        "  ?subject a <http://tib.eu/dsdl/ontario/ontology/RDFMT> . " \
                        "  ?subject <http://tib.eu/dsdl/ontario/ontology/source> ?source." \
                        " optional{?source <http://tib.eu/dsdl/ontario/ontology/cardinality> ?card.}" \
                        "  ?source <http://tib.eu/dsdl/ontario/ontology/datasource> <" + datasource + "> ." \
                        "} } "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
            time.sleep(5)

        return reslist

    def create_inter_ds_links(self, datasource=None, outputqueue=Queue()):

        sources = self.get_sources()
        rdfmts = {}
        if len(sources) == 0:
            outputqueue.put('EOF')
            return

        sourcemaps = {s['subject']: s for s in sources}
        print(sourcemaps)
        for s in sources:
            mts = self.get_ds_rdfmts(s['subject'])
            rdfmts[s['subject']] = {m['subject']: int(m['card']) if 'card' in m else -1 for m in mts}

        print(rdfmts.keys())
        if datasource is not None:
            self.update_links(rdfmts, sourcemaps, datasource)
        else:
            self.find_all_links(rdfmts, sourcemaps)

        outputqueue.put('EOF')

    def find_all_links(self, rdfmts, sourcemaps):
        queues = {}
        processes = {}

        for si in rdfmts:
            s = sourcemaps[si]
            print("Searching inter links from ", s['subject'])
            for ti in rdfmts:
                t = sourcemaps[ti]
                if si == ti:
                    continue
                queue = Queue()
                queues[ti] = queue
                p = Process(target=self.get_inter_ds_links_bn, args=(s, rdfmts[si], t, rdfmts[ti], queue,))
                p.start()
                processes[ti] = p
                # self.get_inter_ds_links_bn(s, rdfmts[s], t, rdfmts[t], graph)
                if len(queues) > 2:
                    toremove = []
                    while len(queues) > 0:
                        for endpoint in queues:
                            try:
                                queue = queues[endpoint]
                                r = queue.get(False)
                                if r == 'EOF':
                                    toremove.append(endpoint)
                            except Empty:
                                pass
                        for r in toremove:
                            if r in queues:
                                del queues[r]
                            if r in processes and processes[r].is_alive():
                                print('terminating: ', r)
                                processes[r].terminate()
                                del processes[r]

    def update_links(self, rdfmts, sourcemaps, datasource):

        queues = {}
        processes = {}
        if isinstance(datasource, DataSource):
            did = datasource.rid
            ds = sourcemaps[datasource.rid]
        else:
            ds = datasource
            datasource = self.get_source(datasource)
            if len(datasource) > 0:
                datasource = datasource[0]
                datasource = DataSource(ds,
                                        datasource['url'],
                                        datasource['dstype'],
                                        name=datasource['name'],
                                        desc=datasource['desc'] if "desc" in datasource else "",
                                        params=datasource['params'] if "params" in datasource else {},
                                        keywords=datasource['keywords'] if 'keywords' in datasource else "",
                                        version=datasource['version'] if 'version' in datasource else "",
                                        homepage=datasource['homepage'] if 'homepage' in datasource else "",
                                        organization=datasource['organization'] if 'organization' in datasource else "",
                                        ontology_graph=datasource[
                                            'ontology_graph'] if 'ontology_graph' in datasource else None
                                        )
                ds = sourcemaps[datasource.rid]
                did = datasource.rid
            else:
                did = datasource

        today = str(datetime.datetime.now())
        data = ["<" + datasource.rid + '> <http://purl.org/dc/terms/modified> "' + today + '"']
        delete = ["<" + datasource.rid + '> <http://purl.org/dc/terms/modified> ?modified ']
        self.delete_insert_data(delete, data, delete)
        print(did)
        for si in sourcemaps:
            s = sourcemaps[si]
            if si == did:
                continue
            print(si)
            queue1 = Queue()
            queues[did] = queue1
            print("Linking between ", ds['subject'], len(rdfmts[did]), " and (to) ", s['subject'], len(rdfmts[si]))
            p = Process(target=self.get_inter_ds_links_bn, args=(ds, rdfmts[did], s, rdfmts[si], queue1,))
            p.start()
            processes[did] = p
            queue2 = Queue()
            queues[si] = queue2
            print("Linking between ", s['subject'], len(rdfmts[si]), " and (to) ", ds['subject'], len(rdfmts[did]))
            p2 = Process(target=self.get_inter_ds_links_bn, args=(s, rdfmts[si], ds, rdfmts[did], queue2,))
            p2.start()
            processes[si] = p2
            if len(queues) >= 2:

                while len(queues) > 0:
                    toremove = []
                    for endpoint in queues:
                        try:
                            queue = queues[endpoint]
                            r = queue.get(False)
                            print(r)
                            if r == 'EOF':
                                toremove.append(endpoint)
                        except Empty:
                            pass
                    for r in toremove:
                        if r in queues:
                            del queues[r]
                        print(r, queues.keys())
                        if r in processes and processes[r].is_alive():
                            print('terminating: ', r)
                            processes[r].terminate()
                        if r in processes:
                            del processes[r]
        print("linking DONE!", did)

    def get_inter_ds_links_bn(self, s, srdfmts, t, trdfmts, queue=Queue()):
        endpoint1 = s['url']
        endpoint2 = t['url']
        # print("Linking between ", s['subject'], " and (to) ", t['subject'])
        for m1 in srdfmts:
            query = "SELECT DISTINCT ?p ?t WHERE {?s a <" + m1 + ">. ?s ?p ?t . FILTER (isURI(?t)) }"
            limit = 100
            offset = 0
            reslist = []
            while True:
                query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
                res, card = contactRDFSource(query_copy, endpoint1)
                if card == -2:
                    limit = limit // 2
                    limit = int(limit)
                    if limit < 1:
                        break
                    continue
                if card > 0:
                    reslist.extend(res)
                    if len(reslist) >= 1000:
                        break
                if card < limit:
                    break

                if offset + limit >= srdfmts[m1]:
                    offset += limit
                else:
                    offset += limit

                time.sleep(5)
            for m2 in trdfmts:
                print(m1, " --- Vs --- ", m2, "in [", endpoint2, "]")
                results = self.get_links_bn_ds(reslist, m2, endpoint2)
                data = []
                print(len(results), 'links found')
                try:
                    for link in results:
                        val = str(endpoint2 + m1 + link + m2).encode()
                        mrpid = mtresource + str(hashlib.md5(val).hexdigest())

                        card = results[link]
                        rs = DataSource(t['subject'], t['url'], DataSourceType.SPARQL_ENDPOINT)
                        ran = PropRange(mrpid, m2, rs, range_type=0, cardinality=card)
                        data.extend(ran.to_rdf())
                        mtpid = mtresource + str(hashlib.md5(str(m1 + link).encode()).hexdigest())
                        data.append("<" + mtpid + "> <" + mtonto + "linkedTo> <" + mrpid + "> ")
                    if len(data) > 0:
                        self.updateGraph(data)
                except Exception as e:
                    print("Exception : ", e)
                    logger.error("Exception while collecting data" + str(e))
                    logger.error(m1 + " --- Vs --- " + m2 + "in [" + endpoint2 + "]")
                    logger.error(results)
                    logger.error(data)
        print('get_inter_ds_links_bn Done!')
        queue.put("EOF")

    def get_links_bn_ds(self, reslist, m2, e2):

        resdict = {}
        results = {}
        prefixes = {}
        for r in reslist:
            if r['p'] in resdict:
                resdict[r['p']].append(r['t'])
            else:
                resdict[r['p']] = [r['t']]

            obj = r['t']
            if r['p'] in prefixes:
                prefixes[r['p']].append(obj[:obj.rfind("/")])
            else:
                prefixes[r['p']] = [obj[:obj.rfind("/")]]
        print('linking properties:', resdict.keys())
        for p in resdict:
            prefs = list(set(prefixes[p]))
            reslist = self.get_if_prefix_matches(m2, prefs, e2)
            e1res = resdict[p]
            matching = list(set(reslist).intersection(set(e1res)))
            if len(matching) > 0:
                results[p] = len(matching)
                print(len(matching), " links out of 10000 subject found for", p, m2, 'in', e2)
            else:
                print(p, 'no matching found')
        return results

    def get_if_prefix_matches(self, m2, prefixes, e):
        reslist = []
        i = 0
        j = 0
        for i in range(10, len(prefixes), 10):
            prefs = [" regex(str(?t), '"+urlparse.quote(p, safe="/:")+"', 'i') " for p in prefixes[j:i]]
            #for p in prefixes:
            #    prefs.append(" regex(str(?t), '"+p+"', 'i') ")
            # Check if there are subjects with prefixes matching
            tquery = "SELECT DISTINCT * WHERE {?t a <" + urlparse.quote(m2, safe="/:") + ">. FILTER (" + " || ".join(prefs) + ") }"
            reslist.extend(self.get_results(tquery, e))
            j += 10

        if i < len(prefixes):
            prefs = [" regex(str(?t), '" + urlparse.quote(p, safe="/:") + "', 'i') " for p in prefixes[i:]]
            tquery = "SELECT DISTINCT * WHERE {?t a <" + urlparse.quote(m2, safe="/:") + ">. FILTER (" + " || ".join(prefs) + ") }"
            reslist.extend(self.get_results(tquery, e))

        return reslist

    def get_results(self, query, endpoint):
        reslist = []
        limit = 1000
        offset = 0

        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, endpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend([r['t'] for r in res])
            if card < limit:
                break
            offset += limit
            time.sleep(5)

        return reslist

    def create_from_mapping(self, ds, outqueue=Queue(), types=[], isupdate=False):
        endpoint = ds.url
        logger.info('----------------------' + endpoint + '-------------------------------------')

        results = self.get_rdfmts(ds, types)
        # self.create_inter_ds_links(datasource=ds)
        outqueue.put('EOF')

        return results

    def get_rdfmts_from_mapping(self, ds, types=None):
        if types is None:
            types = []
        prefix = "prefix rr: <http://www.w3.org/ns/r2rml#> " \
                 "prefix rml: <http://semweb.mmlab.be/ns/rml#>"
        mtquery = prefix + " SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <" + self.graph + "> { " \
                           "?tm rml:logicalSource ?ls . " \
                           "?ls rml:source <" + ds.rid + "> . " \
                           "?tm rr:subjectMap  ?sm. ?sm rr:class ?t . " \
                           "?tm rr:predicateObjectMap ?pom. " \
                           "?pom rr:predicate ?p . " \
                           "?pom rr:objectMap ?om. " \
                           "OPTIONAL { ?om rr:class ?r ." \
                               "?pt rr:subjectMap ?ptsm. " \
                               "?ptsm rr:class ?r . " \
                               "?pt rml:logicalSource ?ptls. " \
                               "?ptls rml:source ?rds .}" \
                           "OPTIONAL { " \
                               "?om rr:parentTriplesMap ?pt . " \
                               "?pt rr:subjectMap ?ptsm. " \
                               "?ptsm rr:class ?r . " \
                               "?pt rml:logicalSource ?ptls. " \
                               "?ptls rml:source ?rds ." \
                               "} " \
                           "}} "
        res, card = contactRDFSource(mtquery, self.queryendpoint)
        results = []
        data = []
        if card > 0:
            reslist = {}
            for r in res:
                t = r['t']
                p = r['p']
                if t in reslist:
                    if p in reslist[t]:
                        if 'r' in r:
                            if r['r'] in reslist[t][p]:
                                if r['rds'] in reslist[t][p][r['r']]:
                                    continue
                                else:
                                    reslist[t][p][r['r']].append(r['rds'])
                            else:
                                reslist[t][p][r['r']] = [r['rds']]
                        else:
                            continue
                    else:
                        reslist[t][p] = {}
                        if 'r' in r:
                            reslist[t][p][r['r']] = [r['rds']]
                        else:
                            continue
                else:
                    reslist[t] = {}
                    reslist[t][p] = {}
                    if 'r' in r:
                        reslist[t][p][r['r']] = [r['rds']]
                    else:
                        continue

            for t in reslist:
                sourceURI = mtresource + str(hashlib.md5(str(ds.url + t).encode()).hexdigest())
                source = Source(sourceURI, ds)

                rdfpropteries = []
                preds = reslist[t]

                for p in preds:
                    rn = {"t": t, "cardinality": -1, "subclasses": [], 'p': p, 'predcard': -1, 'range': preds[p].keys()}
                    mtpredicateURI = mtresource + str(hashlib.md5(str(t + p).encode()).hexdigest())
                    propsourceURI = mtresource + str(hashlib.md5(str(ds.url + t + p).encode()).hexdigest())
                    ranges = []
                    if len(preds[p]) > 0:
                        for mr in preds[p]:
                            mrpid = mtresource + str(hashlib.md5(str(ds.url + t + p + mr).encode()).hexdigest())
                            rtype = 0
                            for mrr in preds[p][mr]:
                                print(p, mr, mrr)
                                rds = DataSource(mrr, ds.url, DataSourceType.MONGODB) # Only mrr is important here for the range
                                ran = PropRange(mrpid, mr, rds, range_type=rtype, cardinality=-1)
                                ranges.append(ran)

                    predsouce = Source(propsourceURI, ds, -1)
                    mtprop = MTProperty(mtpredicateURI, p, [predsouce], ranges=ranges, label=p)
                    rdfpropteries.append(mtprop)

                    results.append(rn)
                # add rdf:type property
                p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                mtpredicateURI = mtresource + str(hashlib.md5(str(t + p).encode()).hexdigest())
                propsourceURI = mtresource + str(hashlib.md5(str(ds.url + t + p).encode()).hexdigest())

                predsouce = Source(propsourceURI, ds, -1)
                mtprop = MTProperty(mtpredicateURI, p, [predsouce], ranges=[], label="RDF type")
                rdfpropteries.append(mtprop)

                name = t
                desc = None
                mt = RDFMT(t, name, properties=rdfpropteries, desc=desc, sources=[source], subClassOf=[])
                mtd = mt.to_rdf()
                data.extend(mtd)
                data = list(set(data))
                #pprint.pprint(data)

        if len(data) > 0:
            self.updateGraph(data)

        return results


class MTManager(object):
    """
    Used in Config to access RDF-MTs in the data lake
    """
    def __init__(self, queryurl, user, passwd, graph):
        self.graph = graph
        self.queryendpoint = queryurl
        self.user = user
        self.passwd = passwd

    def get_data_sources(self):
        query = " SELECT distinct ?rid ?endpoint" \
                "  WHERE { " \
                "        GRAPH <" + self.graph + "> { " \
                         "  ?rid a <http://tib.eu/dsdl/ontario/ontology/DataSource> . " \
                         "  ?rid <http://tib.eu/dsdl/ontario/ontology/url> ?endpoint." \
                         "} } "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit

        return reslist

    def get_rdfmts(self):

        query = "SELECT distinct ?rid ?datasource ?card ?pred ?predcard ?mtr ?mtrange " \
                " WHERE { " \
                " GRAPH <" + self.graph + "> {" \
                "    ?rid a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ." \
                "    ?rid <http://tib.eu/dsdl/ontario/ontology/source> ?source. " \
                "    ?rid <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp . " \
                "    ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred . " \
                "    ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?datasource. " \
                "    optional {" \
                "          ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange ." \
                "          ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?mtr . " \
                "      }  }  }  "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
        results = {}
        for r in reslist:
            if r['rid'] not in results:
                results[r['rid']] = {
                            'rootType': r['rid'],
                            'linkedTo': [r['mtr']] if 'mtr' in r else [],
                            'wrappers': [
                                {
                                    'url': r['datasource'],
                                    "predicates": [
                                        r['pred']
                                         ],
                                    "urlparam": "",
                                    "wrapperType": "SPARQLEndpoint"
                                }
                            ],
                            'predicates': [
                                {'predicate': r['pred'],
                                 "range":[r['mtr']] if 'mtr' in r else []}
                                ],
                            'subclass': []
                            }
            else:
                if 'mtr' in r:
                    results[r['rid']]['linkedTo'].append(r['mtr'])
                    results[r['rid']]['linkedTo'] = list(set(results[r['rid']]['linkedTo']))
                pfound = False
                for p in results[r['rid']]['predicates']:
                    if p['predicate'] == r['pred']:
                        if 'mtr' in r:
                            p['range'].append(r['mtr'])
                        pfound = True

                if not pfound:
                    results[r['rid']]['predicates'].append({
                        'predicate': r['pred'],
                        "range": [r['mtr']] if 'mtr' in r else []
                    })
                wfound = False
                for w in results[r['rid']]['wrappers']:
                    if w['url'] == r['datasource']:
                        wfound = True
                        w['predicates'].append(r['pred'])
                        w['predicates'] = list(set(w['predicates']))
                if not wfound:
                    results[r['rid']]['wrappers'].append({
                        'url': r['datasource'],
                        "predicates": [
                            r['pred']
                            ],
                        "urlparam": "",
                        "wrapperType": "SPARQLEndpoint"
                    })

        return results

    def get_rdfmt(self, rdfclass):

        query = "SELECT distinct ?datasource  ?pred ?mtr " \
                " WHERE { " \
                " GRAPH <" + self.graph + "> {" \
                "    <" + rdfclass + "> <http://tib.eu/dsdl/ontario/ontology/source> ?source. " \
                "    <" + rdfclass + "> <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp . " \
                "    ?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred . "\
                " ?source <http://tib.eu/dsdl/ontario/ontology/datasource> ?datasource. "\
                " optional {" \
                                     "          ?mtp <http://tib.eu/dsdl/ontario/ontology/linkedTo> ?mtrange ." \
                "          ?mtrange <http://tib.eu/dsdl/ontario/ontology/rdfmt> ?mtr . " \
                "      }  }  }  "
        limit = 1000
        offset = 0
        reslist = []
        print("getting rdfmt ...")
        print(query)
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
        results = {}
        for r in reslist:
            r['rid'] = rdfclass
            if r['rid'] not in results:
                results[r['rid']] = {
                            'rootType': r['rid'],
                            'linkedTo': [r['mtr']] if 'mtr' in r else [],
                            'wrappers': [
                                {
                                    'url': r['datasource'],
                                    "predicates": [
                                        r['pred']
                                         ],
                                    "urlparam": "",
                                    "wrapperType": "SPARQLEndpoint"
                                }
                            ],
                            'predicates': [
                                {'predicate': r['pred'],
                                 "range":[r['mtr']] if 'mtr' in r else []}
                                ],
                            'subclass': []
                            }
            else:
                if 'mtr' in r:
                    results[r['rid']]['linkedTo'].append(r['mtr'])
                    results[r['rid']]['linkedTo'] = list(set(results[r['rid']]['linkedTo']))
                pfound = False
                for p in results[r['rid']]['predicates']:
                    if p['predicate'] == r['pred']:
                        if 'mtr' in r:
                            p['range'].append(r['mtr'])
                        pfound = True

                if not pfound:
                    results[r['rid']]['predicates'].append({
                        'predicate': r['pred'],
                        "range": [r['mtr']] if 'mtr' in r else []
                    })
                wfound = False
                for w in results[r['rid']]['wrappers']:
                    if w['url'] == r['datasource']:
                        wfound = True
                        w['predicates'].append(r['pred'])
                        w['predicates'] = list(set(w['predicates']))
                if not wfound:
                    results[r['rid']]['wrappers'].append({
                        'url': r['datasource'],
                        "predicates": [
                            r['pred']
                            ],
                        "urlparam": "",
                        "wrapperType": "SPARQLEndpoint"
                    })

        return results[rdfclass] if rdfclass in results else {}

    def get_data_source(self, dsid):
        query = " SELECT distinct * " \
                "  WHERE { " \
                "        GRAPH <" + self.graph + "> { " \
                         "<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/url> ?url. " \
                         "<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/dataSourceType> ?dstype. " \
                         " optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/name> ?name} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/version> ?version} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/keywords> ?keywords} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/organization> ?organization} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/homepage> ?homepage} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/params> ?params} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/desc> ?desc} " \
                         "optional{<" + dsid + "> <http://tib.eu/dsdl/ontario/ontology/triples> ?triples} " \
                         "} } "
        limit = 1000
        offset = 0
        reslist = []
        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contactRDFSource(query_copy, self.queryendpoint)
            if card == -2:
                limit = limit // 2
                limit = int(limit)
                if limit < 1:
                    break
                continue
            if card > 0:
                reslist.extend(res)
            if card < limit:
                break
            offset += limit
        if len(reslist) > 0:
            e = reslist[0]
            ds = DataSource(dsid,
                            e['url'],
                            e['dstype'],
                            name=e['name'],
                            desc=e['desc'] if "desc" in e else "",
                            params=e['params'] if "params" in e else {},
                            keywords=e['keywords'] if 'keywords' in e else "",
                            version=e['version'] if 'version' in e else "",
                            homepage=e['homepage'] if 'homepage' in e else "",
                            organization=e['organization'] if 'organization' in e else "",
                            triples=e['triples'] if 'triples' in e else -1,
                            ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
                            )
            return ds
        else:
            return None

    def get_mappings(self, dsid):
        prefix = "prefix rr: <http://www.w3.org/ns/r2rml#> " \
                 "prefix rml: <http://semweb.mmlab.be/ns/rml#>"
        mtquery = prefix + " SELECT DISTINCT ?t ?p ?r ?rds WHERE { GRAPH <" + self.graph + "> { " \
                           "?tm rml:logicalSource ?ls . " \
                           "?ls rml:source <" + dsid + "> . " \
                           "?tm rr:subjectMap  ?sm. " \
                           "?sm rr:class ?t . " \
                           "?tm rr:predicateObjectMap ?pom. " \
                           "?pom rr:predicate ?p . " \
                           "?pom rr:objectMap ?om. " \
                           "OPTIONAL { " \
                           "          ?om rr:parentTriplesMap ?pt . " \
                           "          ?pt rr:subjectMap ?ptsm. " \
                           "          ?ptsm rr:class ?r . " \
                           "          ?pt rml:logicalSource ?ptls. " \
                           "          ?ptls rml:source ?rds ." \
                           "     } " \
                           "  }" \
                           "} "
        print(mtquery)
        res, card = contactRDFSource(mtquery, self.queryendpoint)
        results = []
        return res

    def get_rdfmts_by_preds(self, preds):
        query = "SELECT distinct ?rid WHERE { GRAPH <" + self.graph + "> {"
        query += "    ?rid a <http://tib.eu/dsdl/ontario/ontology/RDFMT> ."
        i = 0
        for p in preds:
            query += "?rid <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp" + str(i) + \
                     " . ?mtp" + str(i) + " <http://tib.eu/dsdl/ontario/ontology/predicate> <" + p + "> ."
            i += 1

        query += " } }"
        reslist = query_endpoint(self.queryendpoint, query)

        results = {}
        for r in reslist:
            res = self.get_rdfmt(r['rid'])
            if len(res) > 0:
                results[r['rid']] = res

        return results

    def get_preds_mt(self):
        query = "SELECT distinct ?rid ?pred WHERE { GRAPH <" + self.graph + "> {"
        query += "    ?rid a <http://tib.eu/dsdl/ontario/ontology/RDFMT> . " \
                 "?rid <http://tib.eu/dsdl/ontario/ontology/hasProperty> ?mtp . " \
                 "?mtp <http://tib.eu/dsdl/ontario/ontology/predicate> ?pred . "
        query += " } }"
        reslist = query_endpoint(self.queryendpoint, query)

        results = {}
        for r in reslist:
            results.setdefault(r['pred'], []).append(r['rid'])
        results = {r: list(set(results[r])) for r in results}
        return results


def query_endpoint(endpoint, query):
    limit = 1000
    offset = 0
    reslist = []
    while True:
        query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
        res, card = contactRDFSource(query_copy, endpoint)
        if card == -2:
            limit = limit // 2
            limit = int(limit)
            if limit < 1:
                break
            continue
        if card > 0:
            reslist.extend(res)
        if card < limit:
            break
        offset += limit

    return reslist


if __name__ == "__main__":

    # with open("public-dbpedia-endpoint.json") as f: # pubmed-single-endpoint.json bio2rdf-public-single-endpoint.json
    #     endps = json.load(f)
    datasets = []
    # for e in endps:
    #     ds = DataSource(e['id'],
    #                     e['url'],
    #                     DataSourceType.SPARQL_ENDPOINT,
    #                     name=e['name'],
    #                     desc=e['desc'] if "desc" in e else "",
    #                     keywords=e['keywords'] if 'keywords' in e else "",
    #                     version=e['version'] if 'version' in e else "",
    #                     homepage=e['homepage'] if 'homepage' in e else "",
    #                     organization=e['organization'] if 'organization' in e else "",
    #                     ontology_graph=e['ontology_graph'] if 'ontology_graph' in e else None
    #                     )
    #     datasets.append(ds)

    mgraph = "http://ontario.tib.eu/federation/g/ChEBI-csv-mapping-test"
    print("******* ****************************")

    mgmgr = RDFMTMgr("http://node2.research.tib.eu:1300/sparql",
                     "http://node2.research.tib.eu:1300/sparql",
                     "dba",
                     "dba", mgraph)
    mgr = MTManager("http://node2.research.tib.eu:1300/sparql",
                    "dba",
                    "dba", mgraph)
    ds = mgr.get_data_source("http://ontario.tib.eu/ChEBI-csv-mapping-test/datasource/DrugBank-xml")
    # ds = DataSource("http://tib.eu/dsdl/ontario/resource/ReactomeNeo4j",
    #                 'bolt://node3.research.tib.eu:7687',
    #                 DataSourceType.NEO4J)
    mgmgr.get_rdfmts_from_mapping(ds, [])
    # print(ds)
    # # pprint.pprint(mgmgr.get_rdfmts())
    # mgmgr.create(ds)
    print("DONE!!")
    # print("LINKING ........")
    # mgmgr.create_inter_ds_links()
