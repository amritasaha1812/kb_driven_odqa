from elasticsearch import Elasticsearch 
import json 
from wikidata.config_path import WIKIPEDIA_CHUNKS_FILE

"""
Build an elastic search index mapping the id of the document chunks in the WIKIPEDIA_CHUNKS_FILE (psgs_100w.tsv file) to the wikipedia document and the chunk id within that document. 
"""
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

count = 0
num_docs = 3232910


def build_elastic_search_wikipedia_chunk_index():
    count = 0
    for doc_id in range(num_docs):
        try:
            d=es.get(index='wikipedia', doc_type='wiki', id=doc_id)['_source']
        except:
            print ('Error accessing document ', doc_id)
            continue
        first_chunk_id = d['first_chunk_id']
        last_chunk_id = d['last_chunk_id']
        for chunk_id,id in enumerate(range(first_chunk_id, last_chunk_id+1)):
            d = {}
            # d['orig_id'] = id + 1
            d['orig_id'] = id
            d['document_id'] = doc_id
            d['chunk_id'] = chunk_id
            # print (id)
            es.index(index='wikipedia_chunk_ids', doc_type='wiki_chunk_id', id=id, body=d)
            count+=1
            if count%1000==0:
                print ('added ', count , 'documents')
    # count = 0
    # for line in open(WIKIPEDIA_CHUNKS_FILE).readlines()[:100]:
    #     line = line.strip().split('\t')
    #     if line[0]=='id':
    #         continue
    #     id = int(line[0])
    #     text = line[1]
    #     title = line[2]
        # if last_title is None:
        #     last_title = title
        # if title != last_title:
        #     es_title = last_title
        #     es_first_id = last_id
        #     es_last_id = id - 1
        # d = {}
        # d['title'] = title.lower()
        # d['document_chunks'] = docs
        # d['first_chunk_id'] = es_first_id
        # d['last_chunk_id'] = es_last_id
        # es.index(index='wikipedia', doc_type='wiki', id=count, body=d)
        # count+=1
        # if count%100==0:
        #     print ('added ', count , ' chunks')
        # last_id = id 
        # last_title = title
        # docs = []
        # docs.append(text)

if __name__=="__main__":
    
    # USAGE: python -m wikidata.build_indices.build_elastic_search_wikipedia_chunk_index

    '''

    To access the chunk, requires a combination of chunk indices and wikipedia indices

    res = self.es.get(index='wikipedia_chunk_ids', doc_type='wiki_chunk_id', id=idx)
    chunk = self.es.get(index='wikipedia', doc_type='wiki', id=res['_source']['document_id'])
    text = chunk['_source']['document_chunks'][res['_source']['chunk_id']]

    text returns the chunk text
    title is the same as the chunk->_source->title

    '''

    build_elastic_search_wikipedia_chunk_index()