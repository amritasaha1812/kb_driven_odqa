import requests
import os 
from requests import utils

from elasticsearch import Elasticsearch 
import json 
import sys
from wikidata.config_path import WIKIDATA5M_DIR, WIKIDATA5M_DOCTITLE_ENTITY_DIR

#DATA_DIR = '/export/share/amrita/efficientQA/'
#WIKIDATA5M_DIR = DATA_DIR+'/wikidata/wikidata5m'
#WIKIDATA5M_DOCTITLE_ENTITY_DIR = WIKIDATA5M_DIR+'/wikidata5m_doctitle_entity'

index = 'wikipedia'
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
num_docs = 3232910

def get_wikipedia_url_from_wikidata_id(wiki_title, lang='en', debug=False):
    url = (
        'https://en.wikipedia.org/w/api.php'
        '?action=query'
        '&prop=pageprops'
        f'&titles={wiki_title}'
        '&format=json')
    return requests.get(url).json()


def link_wikititle_to_entity_id(start, end):
    if not os.path.exists(WIKIDATA5M_DOCTITLE_ENTITY_DIR):
        os.mkdir(WIKIDATA5M_DOCTITLE_ENTITY_DIR)
    d={}
    count = 0
    error = 0
    file_count = int(start / 1000)
    for i in range(start, end):
        try:
            d_title = es.get(index=index, doc_type="wiki", id=i)['_source']['title']
            if d_title.startswith('"') and d_title.endswith('"'):
                d_title = d_title[1:-1]
            wikidata_info=get_wikipedia_url_from_wikidata_id(d_title)
            if 'error' in wikidata_info:
                error+=1
                continue
            d[i] = {'title':d_title, 'wikidata_info':wikidata_info}
            count += 1
        except:
            print ('error in accessing document ', i)
        if count%100==0:
            print ('finished ', count , '( error ', error, ')')
            if count %1000==0:
                if len(d)>1:
                    json.dump(d, open(WIKIDATA5M_DOCTITLE_ENTITY_DIR+'/'+str(file_count)+'.json', 'w'), indent=4)
                    d={}
                    file_count+=1

if __name__=="__main__":

    # USAGE python -m wikidata.build_indices.link_wikititle_to_entity_id start_index end_index
    
    start = int(sys.argv[1])
    end = min(num_docs, int(sys.argv[2]))

    link_wikititle_to_entity_id(start, end)
    

