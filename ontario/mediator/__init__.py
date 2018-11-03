from ontario.config import OntarioConfiguration
from ontario.mediator.Decomposer import LakeCatalyst
from ontario.mediator.Decomposer import MetaCatalyst
from ontario.mediator.Planner import LakePlanner
import pprint
from ontario.config.model import DataSourceType
import ontario.sparql.utilities as utils


class Catalyst(object):

    def __init__(self, query, config):
        self.query = query
        self.config = config

    def catalyze(self, decomposed_query):
        meta_queries = {}
        if len(decomposed_query) > 0:
            decomposed_query = decomposed_query[0]
            for s in decomposed_query:
                meta = MetaCatalyst(decomposed_query[s], self.config)
                prefixes = utils.getPrefs(self.query.prefs)
                meta_queries[s] = meta.decompose(prefixes)

            for n in meta_queries:
                m = meta_queries[n]
                for s in m:

                    ds = self.config.datasources[s]

                    m[s]['triples'] = list(set(m[s]['triples']))
                    m[s]['datasource'] = ds

        # Check if some stars can be combined as one subquery, based on the source type (e.g., SPARQL_Endpoint)
        combinations = {}
        startriples = {}
        combined= {}
        for star in meta_queries:
            for source in meta_queries[star]:
                variables = self.get_vars(meta_queries[star][source]['triples'])
                for star2 in meta_queries:
                    if star == star2:
                        continue
                    ds = meta_queries[star][source]['datasource']
                    if ds.dstype == DataSourceType.NEO4J or ds.dstype == DataSourceType.SPARK_XML:
                        continue
                    if source in meta_queries[star2] and \
                        len(set(meta_queries[star]).intersection(set(meta_queries[star2]))) == len(set(meta_queries[star])) == len(set(meta_queries[star2])):
                        varibales2 = self.get_vars(meta_queries[star2][source]['triples'])
                        if len(variables.intersection(varibales2)) > 0:
                            combinations.setdefault(source, []).extend(meta_queries[star][source]['triples'])
                            startriples.setdefault(star, {})['triples'] = meta_queries[star][source]['triples']
                            startriples.setdefault(star2, {})['triples'] = meta_queries[star2][source]['triples']
                            combinations[source].extend(meta_queries[star2][source]['triples'])
                            combined.setdefault(star, {}).setdefault(star2, []).append(source)
        # PUSH-DOWN joins
        remove = []
        for c in combined:
            if c in remove:
                continue
            for c2 in combined[c]:
                for s in combined[c][c2]:
                    meta_queries[c][s]['triples'] = list(set(combinations[s]))
                    meta_queries[c][s]['predicates'].extend(meta_queries[c2][s]['predicates'])
                    meta_queries[c][s]['predicates'] = list(set(meta_queries[c][s]['predicates']))
                    meta_queries[c][s]['rdfmts'].extend(meta_queries[c2][s]['rdfmts'])
                    meta_queries[c][s]['rdfmts'] = list(set(meta_queries[c][s]['rdfmts']))
                    startriples[c]['rdfmts'] = meta_queries[c][s]['rdfmts']
                    startriples[c2]['rdfmts'] = meta_queries[c2][s]['rdfmts']

                    startriples[c]['predicates'] = meta_queries[c][s]['predicates']
                    startriples[c2]['predicates'] = meta_queries[c2][s]['predicates']

                    meta_queries[c][s]['startriples'] = startriples
                remove.append(c2)
        for r in remove:
            del meta_queries[r]

        return meta_queries

    def get_vars(self, tps):
        variables = set()
        for t in tps:
            if not t.subject.constant:
                variables.add(t.subject.name)
            if not t.predicate.constant:
                variables.add(t.predicate.name)
            if not t.theobject.constant:
                variables.add(t.theobject.name)

        return variables


def finalize(processqueue):
    import os
    p = processqueue.get()

    while p != "EOF":
        try:
            os.kill(p, 9)
        except OSError as ex:
            pass
        p = processqueue.get()


if __name__ == "__main__":
    query= """
            prefix iasis: <http://project-iasis.eu/vocab/> 
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
            prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 

            SELECT DISTINCT * WHERE { 
                 ?s  <http://project-iasis.eu/vocab/total_cn> ?tcn .
                 ?s   <http://project-iasis.eu/vocab/gene> ?gene .
                 ?gene  a  <http://project-iasis.eu/vocab/Gene> .
                 ?gene  rdfs:label ?label .
            } limit 100
        """

    query = """
            prefix iasis: <http://project-iasis.eu/vocab/> 
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
             select distinct ?s ?id ?sample ?z where {
                 ?s a  iasis:CNV .
                 ?s iasis:id ?id .
                 ?s iasis:total_cn ?z .
                 ?s iasis:cnv_isLocatedIn_sample ?sample .
                 ?sample iasis:primaryTissue ?tissue .
                 ?sample rdfs:label ?samplename .
            } limit 100
        """
    query = """
                    prefix iasis: <http://project-iasis.eu/vocab/> 
                    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                     select distinct * where {
                         ?s a  iasis:Mutation .
                         ?s iasis:mutation_somatic_status ?somaticstatus .
                         ?s iasis:mutation_mentionedIn_publication ?publ .
                         ?s iasis:mutation_isLocatedIn_sample ?sample .
                         ?publ iasis:PubMedID ?pubmedid .     
                         ?sample iasis:primaryTissue ?tissue .
                        ?sample rdfs:label ?samplename .
                    } limit 100
                """
    query = """
                prefix iasis: <http://iasis-project.eu/vocab/> 
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                prefix biopax: <http://www.biopax.org/release/biopax-level3.owl#>
                 select distinct *
                 where {
                     ?s a  biopax:BioSource .
                     ?s biopax:name ?name .                     
                     ?s biopax:abbreviation  ?abbr .
                     ?s biopax:xref ?xref .
                     ?xref biopax:id ?xrefid .
                     ?xref biopax:db ?db .
                     ?xref rdfs:label ?reflabel.
                }
            """
    query = """
            prefix iasis: <http://project-iasis.eu/vocab/>  
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
             select distinct *
             where {
                 ?s a  iasis:Drug .
                 ?s iasis:drugName ?name .                     
                 ?s iasis:description  ?desc .
                 ?s iasis:unii ?unii .   
                 ?s iasis:drugClassification ?cla .
                 ?cla iasis:kingdom ?kd.
                 
            } limit 10
        """
    dquery = """
                            prefix lsdl: <http://project-iasis.eu/vocab/> 
                            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                             select distinct *
                             where {
                                 ?s a  lsdl:Drug .
                                 ?s lsdl:drugName ?name .   
                                 ?s lsdl:unii ?unii .          
                                 ?s lsdl:cas-number ?cla .        
                                 ?s lsdl:toxicity ?pst.                      
                            } limit 10
                        """
    query = """
                        prefix chebi: <http://tandem.tib.eu/ontology/> 
                        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                        prefix dc: <http://purl.org/dc/elements/1.1/>
                         select distinct *
                         where {
                             ?s a chebi:Compound.
                             ?s chebi:url ?url .
                             ?s chebi:definition ?definition.   
                             ?s chebi:synonym ?synonym.
                             ?s dc:title ?name.               
                        } LIMIT 10
                    """
    query = """
                            prefix chebi: <http://tandem.tib.eu/ontology/> 
                            prefix lsdl: <http://lsdl.tib.eu/vocab/>
                            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                            prefix dc: <http://purl.org/dc/elements/1.1/>
                             select distinct *
                             where {
                                 ?s a chebi:Compound.
                                 ?s chebi:url ?url .
                                 ?s chebi:definition ?definition. 
                                 ?s dc:title ?name.   
                                 ?a chebi:compound ?s.
                                 ?a chebi:accessionNumber ?accno. 
                                 ?d lsdl:primaryAccessionNo ?accno.
                                 ?d lsdl:synonym ?drugsyn.
                                 ?d rdfs:label ?drugname .
                                             
                            } 
                        """
    query = """
            prefix chebi: <http://tandem.tib.eu/ontology/> 
            prefix lsdl: <http://lsdl.tib.eu/vocab/>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
            prefix dc: <http://purl.org/dc/elements/1.1/>
            prefix owl: <http://www.w3.org/2002/07/owl#>
             select distinct *
             where {
                 ?s a chebi:Compound.
                 ?s chebi:url ?url .  
                 ?a chebi:compound ?s.
                 ?a chebi:accessionNumber ?accno. 
                 ?d lsdl:primaryAccessionNo ?accno.
                 ?d rdfs:label ?drugname .
                 ?d lsdl:chebiId ?s.   
                 ?d owl:sameAs  ?dbID. 
                 ?dbID <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/pubchemCompoundId> ?pubchemId   .
                 ?dbID ?p ?o
            } limit 100
        """
    # mgraph = "http://ontario.tib.eu/federation/g/drugbankxml"
    mgraph = "http://ontario.tib.eu/federation/g/ChEBI-csv-mapping-test"
    from time import time

    start = time()
    query = """
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT * WHERE {
                    ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label  .
                    ?s <http://project-iasis.eu/vocab/semanticType> ?semType  .
                    ?s <http://project-iasis.eu/vocab/snomedct_us> ?snomedct  .
                    ?s <http://project-iasis.eu/vocab/hpo> ?hpo  .
                    ?s <http://project-iasis.eu/vocab/drugbankId> ?drugbankId  .
                    ?s <http://project-iasis.eu/vocab/hgnc> ?hgnc  .
                    ?s <http://project-iasis.eu/vocab/causes> ?causes  .
                    ?s <http://project-iasis.eu/vocab/mentionedIn> ?publication  .
                    ?publication <http://project-iasis.eu/vocab/pmid> ?pmid .                    
                    ?publication <http://project-iasis.eu/vocab/title> ?title .
                    ?publication <http://project-iasis.eu/vocab/year> ?year .
                    ?publication <http://project-iasis.eu/vocab/journal> ?journal .
                  
              }
        """
    query = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT DISTINCT * WHERE {
                        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label  .
                        ?s <http://project-iasis.eu/vocab/semanticType> ?semType  .
                        ?s <http://project-iasis.eu/vocab/snomedct_us> ?snomedct  .
                        ?s <http://project-iasis.eu/vocab/causes> ?causes  .
                        ?s <http://project-iasis.eu/vocab/mentionedIn> ?publication  .
                        ?publication <http://project-iasis.eu/vocab/pmid> ?pmid .                    
                        ?publication <http://project-iasis.eu/vocab/title> ?title .
                        ?publication <http://project-iasis.eu/vocab/year> ?year .
                        ?publication <http://project-iasis.eu/vocab/journal> ?journal .

                  }
            """

    query = """
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT DISTINCT * WHERE {
                    ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label  .
                    ?s <http://www.ebi.ac.uk/chebi/status> ?status .
                    ?s <http://www.ebi.ac.uk/chebi/source> ?source .
                    ?s <http://www.ebi.ac.uk/chebi/url> ?url .
                    ?s <http://www.ebi.ac.uk/chebi/definition> ?definition. 
                    ?s <http://www.ebi.ac.uk/chebi/name> ?name .
                    ?s <http://www.ebi.ac.uk/chebi/accession> ?accession .
                    ?s <http://www.ebi.ac.uk/chebi/synonym> ?synonym .
                    ?s rdfs:comment ?comment.
              } limit 10
        """

    query = """
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            prefix owl: <http://www.w3.org/2002/07/owl#>
            SELECT DISTINCT * WHERE {
                    ?s owl:sameAs ?drugbank .
                    ?s <http://drugbank.ca/vocab/chebiId> ?chebiId .
                    ?s <http://drugbank.ca/vocab/description> ?description .
                    ?s <http://drugbank.ca/vocab/casNumber> ?casNumber .
                    ?s <http://drugbank.ca/vocab/unii> ?unii .
                    ?s <http://drugbank.ca/vocab/metabolism> ?metabolism .
                    ?s <http://drugbank.ca/vocab/state> ?state .
                    ?s <http://drugbank.ca/vocab/indication> ?indication .
              } limit 10
        """
    query = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT DISTINCT ?refn WHERE {
                        ?s <http://www.ebi.ac.uk/chebi/refDBName> ?refn .
                        ?s <http://www.ebi.ac.uk/chebi/refId> ?refId .
                        ?s <http://www.ebi.ac.uk/chebi/refName> ?refName .
                        ?s <http://www.ebi.ac.uk/chebi/compound> ?compound. 
                  }
                """
    query = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix owl: <http://www.w3.org/2002/07/owl#>
                SELECT DISTINCT * WHERE {
                        ?s owl:sameAs ?drugbank .
                        ?s <http://drugbank.ca/vocab/chebiId> ?chebiId .
                        ?s <http://drugbank.ca/vocab/description> ?description .
                        ?s <http://drugbank.ca/vocab/casNumber> ?casNumber .
                        ?s <http://drugbank.ca/vocab/name> ?drugname .
                        ?s2 <http://www.ebi.ac.uk/chebi/url> ?url .
                        ?s2 <http://www.ebi.ac.uk/chebi/name> ?compoundname. 
                        ?s2 <http://www.ebi.ac.uk/chebi/id> ?chebiId .
                  } limit 10
            """
    query = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT DISTINCT * WHERE {
                        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label  .
                        ?s <http://www.ebi.ac.uk/chebi/status> ?status .
                        ?s <http://www.ebi.ac.uk/chebi/source> ?source .
                        ?s <http://www.ebi.ac.uk/chebi/url> ?url .
                        ?s <http://www.ebi.ac.uk/chebi/definition> ?definition. 
                        ?s <http://www.ebi.ac.uk/chebi/name> ?name .
                        ?s <http://www.ebi.ac.uk/chebi/accession> ?accession .
                  } limit 10
            """
    configuration = OntarioConfiguration('../../configurations/chebi-mysql-config.json')
    print("reading config finished!", configuration.datasources)
    dc = Catalyst(query, configuration)
    # pprint.pprint(configuration.metadata)
    decomp = LakeCatalyst(dc.query, dc.config)
    dc.query = decomp.query
    decomposed_query = decomp.decompose()
    print("---------Decomposed query -------")
    pprint.pprint(decomposed_query)
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

    quers = dc.catalyze(decomposed_query)
    import pprint
    pprint.pprint(quers)
    if len(quers) == 0:
        print('No results')
        exit()

    pl = LakePlanner(dc.query, quers, configuration)
    tree = pl.make_tree()

    print("0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0")
    print(pl.query)
    print("777777777777777777---PLAN----777777777777777777777777777")

    plan = pl.make_plan()
    pprint.pprint(plan)

    from multiprocessing import Queue
    out = Queue()
    processqueue = Queue()
    plan.execute(out, processqueue)
    r = out.get()
    processqueue.put('EOF')
    i = 0

    while r != 'EOF':
        pprint.pprint(r)
        r = out.get()
        i += 1

    exetime = time() - start
    print("total: ", i)
    print("exe time:", exetime)
    finalize(processqueue)

