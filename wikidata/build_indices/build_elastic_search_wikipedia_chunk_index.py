from elasticsearch import Elasticsearch 
import json 

"""
Build an elastic search index mapping the id of the document chunks in the WIKIPEDIA_CHUNKS_FILE (psgs_100w.tsv file) to the wikipedia document and the chunk id within that document. 
"""
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

count = 0
num_docs = 3232910


def build_elastic_search_wikipedia_chunk_index():
    for doc_id in range(num_docs):
        try:
            d=es.get(index='wikipedia', doc_type='wiki', id=doc_id)['_source']
        except:
            print ('Error accessing document ', doc_id)
            continue
        first_chunk_id = d['first_chunk_id']
        last_chunk_id = d['last_chunk_id']
        for chunk_id,id in enumerate(range(first_chunk_id, last_chunk_id+1)):
            d['orig_id'] = id + 1
            d['document_id'] = doc_id
            d['chunk_id'] = chunk_id
            es.index(index='wikipedia_chunk_ids', doc_type='wiki_chunk_id', body=d)
            count+=1
            if count%1000==0:
                print ('added ', count , 'documents')

if __name__=="__main__":
    
    # USAGE: python -m wikidata.build_indices.build_elastic_search_wikipedia_chunk_index

    build_elastic_search_wikipedia_chunk_index()