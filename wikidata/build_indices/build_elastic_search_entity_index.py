from elasticsearch import Elasticsearch 
import json 
from wikidata.config_path import WIKIDATA5M_ENTITIES_FILE

#DATA_DIR = '/export/share/amrita/efficientQA/'
#WIKIDATA5M_DIR = DATA_DIR+'/wikidata/wikidata5m'
#WIKIDATA5M_ENTITIES_FILE = WIKIDATA5M_DIR+'/wikidata5m_entity.txt'

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
indexsetting = {
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer_keyword": {
          "type": "custom",
          "tokenizer": "keyword",
          "filter": [
            "asciifolding",
            "lowercase"
          ]
        },
        "my_analyzer_shingle": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "asciifolding",
            "lowercase",
            "shingle"
          ]
        }
      }
    }
  },
  "mappings": {
    "wikidata_ent":{  
      "properties": {
        "entity_synonyms": {
          "type": "list",
          "analyzer": "my_analyzer_keyword",
          "search_analyzer": "my_analyzer_shingle"
        }
      }
    }
  }
}


def build_elastic_search_entity_index():
  es.indices.create(index='wikidata5m_entities', body=indexsetting, ignore=400)

  count = 0
  
  for line in open(WIKIDATA5M_ENTITIES_FILE).readlines():
      line = line.strip().split('\t')
      entity_id = line[0]
      entity_synonyms = line[1:]
      d = {}
      d['entity_id'] = entity_id
      d['entity_synonyms'] = entity_synonyms
      es.index(index='wikidata5m_entities', doc_type='wikidata_ent', id=count, body=d)
      count += 1
      if count%1000==0:
          print ('finished ', count)

if __name__=="__main__":
  
  # USAGE: python -m wikidata.build_indices.build_elastic_search_entity_index

  build_elastic_search_entity_index()



