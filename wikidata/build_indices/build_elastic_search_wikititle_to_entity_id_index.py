from elasticsearch import Elasticsearch 
import os 
import json 
from wikidata.config_path import WIKIDATA5M_DOCTITLE_ENTITY_DIR

"""
Build elastic search index of the wikipedia titles corresponding to the titles in WIKIPEDIA_CHUNKS_FILE (psgs_100w.tsv) to the wikidata entities. 
Some of these wikidata entities may not be in wikidata5m. 
"""

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#DATA_DIR = '/export/share/amrita/efficientQA/'
#WIKIDATA5M_DIR = DATA_DIR+'/wikidata/wikidata5m'
#WIKIDATA5M_DOCTITLE_ENTITY_DIR = WIKIDATA5M_DIR+'/wikidata5m_doctitle_entity'



def get_wiki_entity_id_from_name(s):
    s=s.lower()
    ent_index="wikidata5m_entities"
    d = es.search(index=ent_index, body={"query":{"match":{"entity_synonyms":s}}})
    ent_ids = []
    for h in d['hits']['hits']:
        h = h['_source']
        if s in h['entity_synonyms']:
            eid = h['entity_id']
            ent_ids.append(eid)
    return ent_ids



def build_elastic_search_wikititle_to_entity_id_index():
    not_found = 0
    not_found_in_wikiapi = 0
    count = 0
    for f in os.listdir(WIKIDATA5M_DOCTITLE_ENTITY_DIR):
        f = dir+'/'+f
        data = json.load(open(f))
        for k,val in data.items():
            title = val['title']
            wiki_info = []
            if 'wikidata_info' not in val:
                continue
            if 'query' not in val['wikidata_info']:
                continue
            found_mapping=False
            for k,v in val['wikidata_info']['query']['pages'].items():
                if 'title' not in v and 'wikibase_item' not in v and 'wikibase-shortdesc' not in v:
                    print ('cannot find ', title)
                if 'title' in v:
                    wikibase_title = v['title']
                else:
                    wikibase_title = 'NONE'
                if 'pageprops' in v and 'wikibase_item' in v['pageprops']:
                    found_mapping=True
                    if 'wikibase-shortdesc' in v['pageprops']:
                        wikibase_shortdesc = v['pageprops']['wikibase-shortdesc']
                    else:
                        wikibase_shortdesc = 'NONE'
                    wikibase_item = v['pageprops']['wikibase_item']
                d={'wikibase_title': wikibase_title, 'wikibase_shortdesc': wikibase_shortdesc, 'wikibase_item': wikibase_item}
                wiki_info.append(d)
            if not found_mapping:
                wiki_entities = get_wiki_entity_id_from_name(title)
                if len(wiki_entities)>0:
                    found_mapping = True
                    wiki_info=[{'wikibase_title':wikibase_title, 'wikibase_item':e} for e in wiki_entities]
                else:
                    not_found +=1 
                not_found_in_wikiapi+=1
            if found_mapping:
                d={'title': title, 'wiki_info': wiki_info}
                es.index(index='wikipedia_to_entity_id', doc_type='wiki_to_eid', id=count, body=d)
            count+=1
            if count%1000==0:
                print ('added ', count , 'documents, not_found: ', not_found, ' not_found_in_api ', not_found_in_wikiapi)


if __name__=="__main__":
    
    # USAGE: python -m wikidata.build_indices.build_elastic_search_wikititle_to_entity_id_index
    
    build_elastic_search_wikititle_to_entity_id_index() 