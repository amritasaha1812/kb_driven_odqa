import sqlite3
import re
import urllib.parse
from elasticsearch import Elasticsearch 
import unicodedata
import os 

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
wiki_index = "wikipedia"
wiki_hyperlinked_db = '/export/share/k.hashimoto/nq_project/nq_models/wiki_db/wiki_20181220_nq_hyper_linked.db'

def get_document_chunks_from_title(title):
    document_chunks = []
    for h in es.search(index=wiki_index, body={"query":{"match":{"title":title}}})['hits']['hits']:
        h = h['_source']
        if h['title']==title:
            document_chunks = h['document_chunks']
            break 
    document_chunks = ' '.join(document_chunks).replace('"','').replace('\t',' ').lower().strip()
    document_chunks = re.sub(' +', ' ', unicodedata.normalize("NFKD", document_chunks))
    #if document_chunks.startswith('"') and document_chunks.endswith('"'):
    #    document_chunks = document_chunks[1:-1]
    return document_chunks

con = sqlite3.connect(wiki_hyperlinked_db)
cursor = con.cursor()
cursor.execute("select * from documents")

count = 0

num_not_match=0
num_docs = 0
for data in cursor:
    num_docs+=1
    data_id = data[0]
    data_text = data[1]
    data_title = data[3]

    document_chunks = get_document_chunks_from_title(data_title)
    if len(document_chunks)==0:
        continue
    cleaned_data_text = re.sub('<a href=.*?>|</a>', '', data_text).replace('</a>', '')
    cleaned_data_text = cleaned_data_text.replace('\n\n',' ').replace('\n', ' ').replace('"','').replace('\t',' ').lower().strip()
    cleaned_data_text = re.sub(' +', ' ', unicodedata.normalize("NFKD", cleaned_data_text))
    if len(cleaned_data_text)==0 and len(document_chunks)==0:
        continue
    if cleaned_data_text not in document_chunks:
        '''print ('A: ', document_chunks)
        print ('\n')
        print ('B: ', cleaned_data_text)
        print ('Document dont match')
        print ('\n\n')'''
        #print ('Document dont match')
        cleaned_data_text_last_line = cleaned_data_text.split('. ')[-1]
        last_index_in_document_chunks = document_chunks.find(cleaned_data_text_last_line)
        if last_index_in_document_chunks>0:
            last_index_in_document_chunks+=len(cleaned_data_text_last_line)
            len_document_chunks = len(document_chunks[:last_index_in_document_chunks].split(' '))
            len_cleaned_data_text = len(cleaned_data_text.split(' '))
            if len_document_chunks!=len_cleaned_data_text:
                num_not_match+=1
                print (num_not_match, num_docs)
        else:
            num_not_match+=1
            print (num_not_match, num_docs)


