# Ontario
Ontario: A Federated SPARQL Query Processor over Semantic Data Lakes

## Configurations
```
{
  "templates": [ {  
        "rootType": "http://tib.eu/ontology/chebi/Compound",
        "datasources": [
                      {
                        "datasource": "http://tib.eu/dataset/chebi-csv",
                        "predicates": [
                          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                          "http://www.w3.org/2000/01/rdf-schema#label"
                          ]
                      }
                    ],
        "predicates": [
                      {
                        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
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
        "name": "ChEBI",
        "ID": "http://tib.eu/dataset/chebi-csv",
        "url": "http://localhost:18891/sparql",
        "params": {},
        "type": "SPARQL_Endpoint",
        "mappings": ["mappings/ChEBI/chebi-csv-mapping.ttl"]
      }
  ]

}
```