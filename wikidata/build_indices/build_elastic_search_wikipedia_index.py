from elasticsearch import Elasticsearch 
import json 
from wikidata.config_path import WIKIPEDIA_CHUNKS_FILE

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#DATA_DIR = '/export/share/amrita/efficientQA/'
#DPR_WIKIDATA_DIR = DATA_DIR+'/data/wikipedia_split'
#WIKIPEDIA_CHUNKS_FILE = DPR_WIKIDATA_DIR+'/psgs_w100.tsv'



def build_elastic_search_wikipedia_index():
    last_title = None
    docs = []
    last_id = 0
    count = 0
    for line in open(WIKIPEDIA_CHUNKS_FILE).readlines():
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
            es_last_id = id - 1
            d = {}
            d['title'] = es_title
            d['document_chunks'] = docs
            d['first_chunk_id'] = es_first_id
            d['last_chunk_id'] = es_last_id
            es.index(index='wikipedia', doc_type='wiki', id=count, body=d)
            count+=1
            if count%100==0:
                print ('added ', count , 'documents')
            last_id = id 
            last_title = title
            docs = []
        docs.append(text)

if __name__=="__main__":

    # USAGE python -m wikidata.build_indices.build_elastic_search_wikipedia_index

    build_elastic_search_wikipedia_index()

