# Ontario
Ontario: A Federated SPARQL Query Processor over Semantic Data Lakes

# Using Ontario
Check the `demo` folder for dockerized examples.


## Mapping file
`chebi-tsv-mapping.ttl`

```text
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dc: <http://purl.org/dc/elements/1.1/> .
@prefix chebi: <http://www.ebi.ac.uk/chebi/> .
@prefix : <http://tib.de/ontario/mapping#> .

:chebi_compound
  	rml:logicalSource [
                rml:source "compounds.tsv";
                rml:referenceFormulation ql:TSV;
                rml:iterator "*"
  			 ];
  	rr:subjectMap [
        rr:template "http://www.ebi.ac.uk/chebi/{ID}";
        rr:class chebi:Compound
  	];  	
    rr:predicateObjectMap [
      rr:predicate chebi:accession;
      rr:objectMap [
        rml:reference "CHEBI_ACCESSION"
      ]
    ];
    rr:predicateObjectMap [
      rr:predicate rdfs:label;
      rr:objectMap [
        rml:reference "NAME"
      ]
    ].
 ```
 
## Configurations
To generate the RDF Molecule Templates, one should prepare a list of data sources with their mapping files (if any) as follows:

`datasources.json`

```json
[
      {
        "name": "ChEBI-TSV",
        "ID": "http://iasis.eu/datasource/chebi-tsv",
        "url": "/home/user/data/ChEBI-TSV",
        "params": {
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                "spark.cores.max": "6",
                "spark.default.parallelism": "4",
                "spark.executor.memory": "6g",
                "spark.driver.memory": "12g",
                "spark.driver.maxResultSize": "8g",
                "spark.python.worker.memory": "10g",
                "spark.local.dir": "/tmp"
        },
        "type": "LOCAL_TSV",
        "mappings": ["/home/user/git/Ontario/mappings/ChEBI/chebi-tsv-mapping.ttl"]
      }
  ]
```

Data Source `type` value can be one of the following:

```buildoutcfg
    SPARQL_Endpoint    
    MySQL
    LOCAL_CSV
    LOCAL_TSV
    LOCAL_JSON
    LOCAL_XML
    HADOOP_CSV
    HADOOP_TSV
    HADOOP_JSON
    HADOOP_XML
    MongoDB
    Neo4j
```

Then run the following:

```bash
    python3 scripts/create_rdfmts.py -s datasources.json -o config.json
    
```
Then the RDF-MTs will be generated either by contacting the data sources or from the RML mappings.
 The content of the `config.json` file contains the following information:

```json
{
  "templates": [ {  
        "rootType": "http://tib.eu/ontology/chebi/Compound",
        "datasources": [
                      {
                        "datasource": "http://iasis.eu/datasource/chebi-tsv",
                        "predicates": [
                          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                          "http://www.w3.org/2000/01/rdf-schema#label",
                          "http://tib.eu/ontology/chebi/accession"
                          ]
                      }
                    ],
        "predicates": [
                      {
                        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "range": []
                      },
                      {
                        "predicate": "http://tib.eu/ontology/chebi/accession",
                        "range": []
                      },
                      {
                        "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
                        "range": []
                      }
                    ]
     }
  ],
  "datasources": [
      {
        "name": "ChEBI-TSV",
        "ID": "http://iasis.eu/datasource/chebi-tsv",
        "url": "/home/user/data/ChEBI-TSV",
        "params": {
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                "spark.cores.max": "6",
                "spark.default.parallelism": "4",
                "spark.executor.memory": "6g",
                "spark.driver.memory": "12g",
                "spark.driver.maxResultSize": "8g",
                "spark.python.worker.memory": "10g",
                "spark.local.dir": "/tmp"
        },
        "type": "LOCAL_TSV",
        "mappings": ["/home/user/git/Ontario/mappings/ChEBI/chebi-tsv-mapping.ttl"]
  }
  ]
}
```


## Running Ontario

Ontario has been developed in python (3.x) and depends on some python packages to communicate with different databases and services.
To install the required packages run:

```bash
    pip3 install -r requirements.txt
```

Install Ontario:
```bash
    python3 setup.py install
```


To run queries:

```bash
    ./runExperiment.py -q path/to/sparqlquery.txt -c path/to/config.json -p False
```

If you want to just see the plans, set `-p True`. 

To run multiple queries in a folder:

```bash
    ./runOntarioExp.sh  /path/to/queriefolder/  path/to/config.json outputname.tsv  errorlog.txt False
```

If you want to just see the plans, set the last argument `True`

# Creating Docker image
```bash
docker build -t ontario:0.3 .
```

# Publication:
Kemele M. Endris, Philipp D. Rohde, Maria-Esther Vidal, and Sören Auer. "Ontario: Federated Query Processing against a Semantic Data Lake." DEXA 2019 - Database and Expert Systems Applications. Lecture Notes in Computer Science. Springer, Cham (2019).

# License
This work is licensed under [GNU/GPL v2](https://www.gnu.org/licenses/gpl-2.0.html).
