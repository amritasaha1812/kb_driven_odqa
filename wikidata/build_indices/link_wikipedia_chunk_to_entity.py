import sqlite3
import re
import urllib.parse
from elasticsearch import Elasticsearch 
import os 
import sys 
from wikidata.config_path import HYPERLINKED_WIKIPEDIA_DB
#HYPERLINKED_WIKIPEDIA_DIR = '/export/share/k.hashimoto/nq_project/nq_models/wiki_db/'
#HYPERLINKED_WIKIPEDIA_DB = HYPERLINKED_WIKIPEDIA_DIR+'/wiki_20181220_nq_hyper_linked.db'


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
wiki_index = "wikipedia_to_entity_id"

con = sqlite3.connect(HYPERLINKED_WIKIPEDIA_DB)
cursor = con.cursor()

def link_wikipedia_chunk_to_entity(start_index, end_index):
    num_docs = end_index - start_index
    cursor.execute("select * from documents limit "+str(num_docs)+" offset "+int(start_index))
    count = 0

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
                
            document_chunk_start_index = start_word_index % 100
            if document_chunk_id not in linked_entities:
                linked_entities[document_chunk_id] = []
            linked_entities[document_chunk_id].append({"entity_ids":ent_ids, "chunk_start_word_index": document_chunk_start_index, "global_start_word_index": start_word_index, "global_start_index":start_index})
        ds = []
        for k,v in linked_entities.items():
            entities = v
            doc_chunk = k 
            d={"document_chunk": doc_chunk, "linked_entities": entities}
            ds.append(d)
        final_data = {'title': data_title, 'linked_data':ds}
        es.index(index='wikidoc_to_entity_id', doc_type='wikidoc_to_eid', id=count, body=final_data)
        count += 1
        if count % 100==0:
            print ('finished ', count)
            
if __name__=="__main__":

    # USAGE: python -m wikidata.build_indices.link_wikipedia_chunk_to_entity start_index end_index

    start_index = int(sys.argv[1])  
    end_index = int(sys.argv[2])
    link_wikipedia_chunk_to_entity(start_index, end_index)