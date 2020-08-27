import numpy as np
import json
import spacy
from elasticsearch import Elasticsearch 
import json 
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

nlp = spacy.load("en_core_web_sm")

stopwords = set(['the', 'of', 'in', 'and', 'to', '-', 's', '(', ')', 'on', 'an', 'from', ':', 'at', '.', '&', 'as'])

def tokenize(string):
    doc = nlp(string)
    return [t.text for t in doc]

    
def get_entities_in(string):
    string = str(string)
    d = es.search(index='wikidata5m_entities', body={"query":{"match":{"entity_synonyms":string}}})
    entity_map = {}
    for h in d['hits']['hits']:
        e_id = h['_source']['entity_id']
        entity_map[e_id]  = {'score':h['_score'], 'entity_synonyms': h['_source']['entity_synonyms']}
    return entity_map

def exact_match(text):
    entities_in_text = {}
    text_entities = get_entities_in(text.lower())
    text_tokens = tokenize(text)
    for e_id, e_data in text_entities.items():
        e_syn = e_data['entity_synonyms']
        for e in e_syn:
            e_tokens = tokenize(e)
            text_tokens_uncovered = (set(text_tokens)-set(e_tokens))-stopwords
            if len(text_tokens_uncovered)==0:
                entities_in_text[e_id] = e_data
                break 
    return entities_in_text

DATA_DIR='/export/share/amrita/efficientQA/'
DPR_TRAIN_DATA = DATA_DIR+'/data/retriever/nq-train.json'
DPR_DEV_DATA = DATA_DIR+'data/retriever/nq-dev.json'


def entity_linking_using_elastic_search_index(data_type):
    num_ents_q = []
    num_ents_a = []
    num_ents_t = []
    num_int_ents_qt = []

    found_ents_q = []
    found_ents_a = []
    found_ents_t = []
    found_int_ents_qt = []

    count = 0

    if data_type=='train':
        data_file = DPR_TRAIN_DATA
    elif data_type=='dev':
        data_file = DPR_DEV_DATA
    output_data_file = data_file.replace('.json', '_wikidata5m.json')
    new_data = []
    for qa in json.load(open(data_file)):
        question = qa["question"].lower().strip()
        answers = [x.lower().strip() for x in qa["answers"]]
        title = qa["positive_ctxs"][0]["title"].lower().strip()
        entities_in_question = get_entities_in(question)
        entities_in_answer = {}
        for answer in answers:
            answer_ents = exact_match(answer)
            entities_in_answer.update(answer_ents)
        entities_in_title = exact_match(title)
        overlapping_entities = set(entities_in_question.keys()).intersection(entities_in_title.keys())
        overlapping_entities = {d:entities_in_title[d] for d in overlapping_entities}
        qa["question_entities"] = entities_in_question
        qa["gold_context"] = qa["positive_ctxs"][0]
        del qa["positive_ctxs"]
        del qa["negative_ctxs"]
        del qa["hard_negative_ctxs"]
        qa["answer_entities"] = entities_in_answer
        qa["title_entities"] = entities_in_title
        qa["ques_title_entities"] = overlapping_entities
        new_data.append(qa)
        num_ents_q.append(len(entities_in_question))
        num_ents_a.append(len(entities_in_answer))
        num_ents_t.append(len(entities_in_title))
        num_int_ents_qt.append(len(overlapping_entities))
        found_ents_q.append(float(len(entities_in_question)>0))
        found_ents_a.append(float(len(entities_in_answer)>0))
        found_ents_t.append(float(len(entities_in_title)>0))
        found_int_ents_qt.append(float(len(overlapping_entities)>0))
        if count % 100==0:
            print ('finished ', count)
        count+=1
    json.dump(new_data, open(output_data_file, 'w'), indent=4)
    num_ents_q = np.mean(np.array(num_ents_q))
    num_ents_a = np.mean(np.array(num_ents_a))
    num_ents_t = np.mean(np.array(num_ents_t))
    num_int_ents_qt = np.mean(np.array(num_int_ents_qt))
    found_ents_q = np.mean(np.array(found_ents_q))
    found_ents_a = np.mean(np.array(found_ents_a))
    found_ents_t = np.mean(np.array(found_ents_t))
    found_int_ents_qt = np.mean(np.array(found_int_ents_qt))
    print ('avg num_ents: ', num_ents_q, num_ents_a, num_ents_t, num_int_ents_qt)
    print ('avg found_ents:', found_ents_q, found_ents_a, found_ents_t, found_int_ents_qt)


    #avg num_ents:  5.811665387567153 1.7056024558710667 1.192785878741366 0.3152724481964697
    #avg found_ents: 1.0 0.9680736761320031 0.9952417498081351 0.2632386799693016


if __name__=="__main__":

    # USAGE: python -m wikidata.entity_linking.entity_linking_using_elastic_search_index train 
    # USAGE: python -m wikidata.entity_linking.entity_linking_using_elastic_search_index dev
    data_type = sys.argv[1]
    entity_linking_using_elastic_search_index(data_type)

