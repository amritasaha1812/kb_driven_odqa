from elasticsearch import Elasticsearch 
import json 
from wikidata.config_path import WIKIPEDIA_CHUNKS_FILE

"""
Build an elastic search index for the wikipedia documents from WIKIPEDIA_CHUNKS_FILE (psgs_w100.tsv). 
Each elastic search entry has the wikipedia document i.e. title, list of 100word document chunks and the id (from psgs_w100.tsv) of the first and last document chunk
"""

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#DATA_DIR = '/export/share/amrita/efficientQA/'
#DPR_WIKIDATA_DIR = DATA_DIR+'/data/wikipedia_split'
#WIKIPEDIA_CHUNKS_FILE = DPR_WIKIDATA_DIR+'/psgs_w100.tsv'

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
        "document_chunks": {
          "type": "list",
          "analyzer": "my_analyzer_keyword",
          "search_analyzer": "my_analyzer_shingle"
        }
      }
    }
  }
}


def build_elastic_search_wikipedia_index(start_index=0, start_doc_id=0):

    es.indices.create(index='wikipedia', body=indexsetting, ignore=400)

    last_title = None
    docs = []
    ids = []
    if start_index>0:
        last_id = start_index-1
    else:
        last_id = 1
    count = start_doc_id
    lines = open(WIKIPEDIA_CHUNKS_FILE).readlines()[start_index:]
    for line in lines:
        line = line.strip().split('\t')
        if line[0]=='id':
            continue
        id = int(line[0])
        text = line[1]
        title = line[2]
        if last_title is None:
            last_title = title
        if title != last_title:
            es_title = last_title
            es_first_id = last_id
            es_last_id = ids[-1]
            d = {}
            d['title'] = es_title
            d['document_chunks'] = docs
            d['first_chunk_id'] = es_first_id
            d['last_chunk_id'] = es_last_id
            d['ids'] = ids
            es.index(index='wikipedia', doc_type='wiki', id=count, body=d)
            count+=1
            if count%1000==0:
                print ('added ', count , 'documents')
            last_id = id 
            last_title = title
            docs = []
            ids = []
        docs.append(text)
        ids.append(id)

if __name__=="__main__":

    # USAGE python -m wikidata.build_indices.build_elastic_search_wikipedia_index
    #start_index = 12302965
    #start_doc_id = 1575008
    #start_index = int(sys.argv[1])
    #start_doc_id = int(sys.argv[2])
    build_elastic_search_wikipedia_index(start_index, start_doc_id)

