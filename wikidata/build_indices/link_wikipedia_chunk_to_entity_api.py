import sqlite3
import re
import urllib.parse
from elasticsearch import Elasticsearch 
import os 
import sys 
import pickledb 
from wikidata.config_path import HYPERLINKED_WIKIPEDIA_DB, HYPERLINKED_WIKIPEDIA_DB_TITLE_TO_ROW_MAP
#HYPERLINKED_WIKIPEDIA_DIR = '/export/share/k.hashimoto/nq_project/nq_models/wiki_db/'
#HYPERLINKED_WIKIPEDIA_DB = HYPERLINKED_WIKIPEDIA_DIR+'/wiki_20181220_nq_hyper_linked.db'


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
wiki_index = "wikipedia_to_entity_id"

con = sqlite3.connect(HYPERLINKED_WIKIPEDIA_DB)
cursor = con.cursor()

wikidb_title_to_row_map = None
if os.path.exists(HYPERLINKED_WIKIPEDIA_DB_TITLE_TO_ROW_MAP):
    wikidb_title_to_row_map = pickledb.load(HYPERLINKED_WIKIPEDIA_DB_TITLE_TO_ROW_MAP, False)


def dump_db_rowid_to_doctitle_mapping():
    wikidb_title_to_row_map = pickledb.load(HYPERLINKED_WIKIPEDIA_DB_TITLE_TO_ROW_MAP, False)
    cursor.execute("select * from documents")
    count = 0
    for data in cursor:
        title = data[-1]
        wikidb_title_to_row_map.set(title, count)
        count += 1
    wikidb_title_to_row_map.dump()

def link_wikipedia_chunk_to_entity_api(title=None):
    if wikidb_title_to_row_map is None:
        dump_db_rowid_to_doctitle_mapping()
    if title is None:
        return None
    row_id = wikidb_title_to_row_map.get(title)
    cursor.execute("select * from documents LIMIT 1 OFFSET "+str(row_id))
    for data in cursor:
        data_id = data[0]
        data_text = data[1]
        data_title = data[3]

        data_text_firstsent_index = data_text.find('\n\n')
        if data_text_firstsent_index>0:
            data_text_firstsent = data_text[:data_text_firstsent_index]
            if data_text_firstsent.strip().lower()==data_title.strip().lower():
                data_text = data_text[data_text_firstsent_index+2:]
            else:
                print ('Title does not match with opening line of text')
                print ('title: ', data_title, 'text: ', data_text[:20])
        else:
            print ('no paragraph in text')
        data_text = data_text.replace("<a href=", "<a_href=")
        linked_entities = {}
        for m in re.compile('<a.*?>').finditer(data_text):
            start_index = m.start()
            start_word_index = len(data_text[:start_index].split(' '))
            ent_name = m.group()
            document_chunk_id = int(start_word_index/100)
            ent_name_processed = urllib.parse.unquote(ent_name.replace('<a href="','').replace('">',''))
            d = es.search(index=wiki_index, body={"query":{"match":{"title":ent_name_processed}}})
            ent_ids = []
            for h in d['hits']['hits']:
                h = h['_source']
                if h['title'].lower()==ent_name_processed.lower():
                    ent_ids.extend([di['wikibase_item'] for di in h['wiki_info'] if di['wikibase_item']!='NONE'])
            if document_chunk_id not in linked_entities:
                linked_entities[document_chunk_id] = []
            linked_entities[document_chunk_id].extend(ent_ids)
        break
    return linked_entities    
            
if __name__=="__main__":

    # USAGE: python -m wikidata.build_indices.link_wikipedia_chunk_to_entity_api 

    
