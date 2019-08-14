# Ontario Demo

To demonstrate Ontario SDL in action, we use the following setting:

### Data Sources
- DrugBank: `RDB-MySQL`
- KEGG: `RDF-Virtuoso`
- ChEBI: `TSV-LocalFile`

Demo folder contains:

- `./configureations/` - contains `datasources.json` and `config.json`. Note: `config.json` is created by the RDF-MT creation script. (see below)
- `./data.tar.gz` and `./data2.tar.gz`  - contains  sample datasets for `RDB`, `rdf` and `tsv` files.
- `./mappings` - contains sample mapping files for raw files in `./data`, i.e., for MySQL data and TSV files
- `./queries` - contains sample queries 
- `./docker-compose.yml` - file for creating three docker containers: `ontario`, `drugbankrdb`, and `keggrdf`

### Extract `./data.tar.gz`
```bash
tar -xvf data.tar.gz
tar -xvf data2.tar.gz
```

### Create the Semantic Data Lake
To create the containers, run the following:
```bash
 docker-compose up -d 
```

Check if the containers are started:
```bash
CONTAINER ID        IMAGE                      COMMAND                  CREATED             STATUS              PORTS                                             NAMES
76cc25df04f7        kemele/ontario:0.3         "/Ontario/start_spar…"   3 hours ago         3 hours ago         0.0.0.0:5001->5000/tcp                            ontario
1e5cd4b6cf47        mysql:5.7.16               "docker-entrypoint.s…"   3 hours ago         Up 3 hours          0.0.0.0:9000->3306/tcp                            drugbankrdb
2256c8ff7089        kemele/virtuoso:7-stable   "/bin/bash /virtuoso…"   3 hours ago         Up 3 hours          0.0.0.0:1116->1111/tcp, 0.0.0.0:11385->8890/tcp   keggrdf
```

Wait for some seconds until the data is completely loaded:
Check logs of virtuoso:
```bash
....
17:32:22 Checkpoint started
17:32:22 Checkpoint finished, log reused
17:32:22 HTTP/WebDAV server online at 8890
17:32:22 Server online at 1111 (pid 103)
```
Check logs of MySQl:

```bash
....
2019-08-14T17:32:48.816395Z 0 [Note] Event Scheduler: Loaded 0 events
2019-08-14T17:32:48.816616Z 0 [Note] mysqld: ready for connections.
Version: '5.7.16'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server (GPL)
```

### Create RDF Molecule Templates (RDF-MT) - `myconfig.json`
After datasets are loaded, run the following script to create configuration file:

```bash
 docker exec -t ontario /Ontario/scripts/create_rdfmts.py -s /configurations/datasources.json -o /configurations/myconfig.json 
```

The above command creates the RDF-MT based source descriptions stored in `/configurations/myconfig.json`. 
Make sure the file exists by running the following command:
```bash
docker exec -t ontario ls /configurations
```

The excerpt from `myconfig.json` looks like as follows:

```json
{
  "templates": [
    {
      "rootType": "http://bio2rdf.org/ns/kegg#Drug",
      "predicates": [
        {
          "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
          "range": []          
        },
        {
          "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
          "range": []
        },
        {
          "predicate": "http://www.w3.org/2002/07/owl#sameAs",
          "range": [
            "http://bio2rdf.org/ns/kegg#Drug"
          ]
        },
        ... 
      ],
      "linkedTo": [
        "http://bio2rdf.org/ns/kegg#Drug"
      ],
      "datasources": [
        {
          "datasource": "KEGG",
          "predicates": [
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.w3.org/2000/01/rdf-schema#label",
            "http://www.w3.org/2002/07/owl#sameAs",
            "http://bio2rdf.org/ns/bio2rdf#url",
            "http://purl.org/dc/elements/1.1/identifier",
            "http://purl.org/dc/elements/1.1/title",
            "http://bio2rdf.org/ns/bio2rdf#formula",
            "http://bio2rdf.org/ns/bio2rdf#mass",
            "http://bio2rdf.org/ns/bio2rdf#synonym",
            "http://bio2rdf.org/ns/bio2rdf#urlImage",
            "http://bio2rdf.org/ns/bio2rdf#xRef"
          ]
        }
      ]
    },
    {
      "rootType": "http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drug_interactions",
      "predicates": [
        {
          "predicate": "http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2",
          "range": []
        },
        {
          "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
          "range": []
        },
         ... 
      ],
      "linkedTo": [],
      "datasources": [
        {
          "datasource": "Drugbank",
          "predicates": [
            "http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2",
            "http://www.w3.org/2000/01/rdf-schema#label",
            "http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1",
            "http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text"
          ]
        }
      ]
    },
    {
      "rootType": "http://bio2rdf.org/ns/chebi#Compound",
      "predicates": [
        {
          "predicate": "http://www.w3.org/2000/01/rdf-schema#comment",
          "range": []
        },
        {
          "predicate": "http://bio2rdf.org/ns/chebi#is_substituent_group_from",
          "range": []
        },
        {
          "predicate": "http://bio2rdf.org/ns/chebi#has_parent_hydride",
          "range": []
        },
        {
          "predicate": "http://bio2rdf.org/ns/chebi#has_role",
          "range": []
        },
        {
          "predicate": "http://bio2rdf.org/ns/chebi#is_tautomer_of",
          "range": []
        },        
        ...
      ],
      "linkedTo": [],
      "datasources": [
        {
          "datasource": "http://tib.eu/chebi-tsv",
          "predicates": [
            "http://www.w3.org/2000/01/rdf-schema#comment",
            "http://bio2rdf.org/ns/chebi#is_substituent_group_from",
            "http://bio2rdf.org/ns/chebi#has_parent_hydride",
            "http://bio2rdf.org/ns/chebi#has_role",
            "http://bio2rdf.org/ns/chebi#is_tautomer_of",
            "http://bio2rdf.org/ns/bio2rdf#synonym",
            "http://bio2rdf.org/ns/chebi#is_conjugate_base_of",
            "http://bio2rdf.org/ns/bio2rdf#formula",
            "http://bio2rdf.org/ns/chebi#has_part",
            "http://bio2rdf.org/ns/chebi#iupacName",
            "http://bio2rdf.org/ns/chebi#is_a",
            "http://bio2rdf.org/ns/chebi#has_functional_parent",
            "http://bio2rdf.org/ns/bio2rdf#xRef",
            "http://bio2rdf.org/ns/chebi#is_conjugate_acid_of",
            "http://bio2rdf.org/ns/bio2rdf#url",
            "http://bio2rdf.org/ns/bio2rdf#inchi"
          ]
        }
      ]
    }
  ],
  "datasources": [
    {
      "name": "Drugbank",
      "ID": "Drugbank",
      "url": "drugbankrdb:3306",
      "params": {
        "username": "root",
        "password": "1234"
      },
      "type": "MySQL",
      "mappings": [
        "/mappings/mysql/drugbank/drug_interactions.ttl",
        "/mappings/mysql/drugbank/drugs.ttl",
        "/mappings/mysql/drugbank/enzymes.ttl",
        "/mappings/mysql/drugbank/references.ttl",
        "/mappings/mysql/drugbank/targets.ttl"
      ]
    },
    {
      "name": "KEGG",
      "ID": "KEGG",
      "url": "http://keggrdf:8890/sparql",
      "params": {},
      "type": "SPARQL_Endpoint",
      "mappings": []
    },
    {
      "name": "ChEBI-TSV",
      "ID": "http://tib.eu/chebi-tsv",
      "url": "/data/tsv",
      "params": {
        "spark.driver.cores": "4",
        "spark.executor.cores": "4",
        "spark.cores.max": "6",
        "spark.default.parallelism": "4",
        "spark.executor.memory": "6g",
        "spark.driver.memory": "6g",
        "spark.driver.maxResultSize": "6g",
        "spark.python.worker.memory": "4g",
        "spark.local.dir": "/tmp"
      },
      "type": "LOCAL_TSV",
      "mappings": [
        "/mappings/tsv/chebi/Compound.ttl"
      ]
    }
  ]
}
```

You might see the following warning (not an error!):
```bash
WARNING: Couldn't create 'parsetab'. [Errno 20] Not a directory: '/usr/local/lib/python3.6/dist-packages/ontario-0.3-py3.6.egg/ontario/sparql/parser/parsetab.py'
```
### Execute a queries - `command-line`

```bash
docker exec -t ontario /Ontario/scripts/runExperiment.py -c /configurations/myconfig.json -q /queries/simpleQueries/SQ2 -r True 
```
Where `-r` indicates whether to print results (rows) or not.
The following queries are available for testing:

- `/queries/simpleQueries/SQ2`
- `/queries/simpleQueries/SQ3`
- `/queries/simpleQueries/SQ4`
- `/queries/simpleQueries/SQ5`
- `/queries/complexqueries/CQ1`
- `/queries/complexqueries/CQ2`

Summary of execution (and raw results) will be printed on your terminal.
You can inspect `ontario.log` file as: `$ docker exec -t ontario less /Ontario/ontario.log` .


### Execute multiple queries
```bash
# docker exec -t ontario /Ontario/scripts/runOntarioExp.sh  [query_folder] [config_file] [result_file_name] [errors_file_name] [planonlyTrueorFalse] [printResultsTrueorFalse]
docker exec -it ontario /Ontario/scripts/runOntarioExp.sh /queries/simpleQueries /configurations/myconfig.json /results/result.tsv /results/error.txt False False
```
Summary of execution will be saved in `/results/result.tsv`. 
You can inspect it as: `$ docker exec -t ontario cat /results/result.tsv` ,
OR you can find the file in main directory of Ontario (since `\results`, `\configurations`, and others are bounded as volumes)