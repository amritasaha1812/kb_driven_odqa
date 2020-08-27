
import json 
import os 
import requests 

data_file = '/export/share/amrita/efficientQA/data/retriever/nq-dev_nq_gold_appended.json'
data_file_wikidata_goldtitle = '/export/share/amrita/efficientQA/data/wikidata/gold_title/'
data_file_wikidata_qas = '/export/share/amrita/efficientQA/data/wikidata/qas/'

headers = {'Content-type': 'application/json'}
url = 'https://labs.tib.eu/falcon/falcon2/api?mode=long'

def get_wikidata_info(text):
    data = str({"text":text})
    try:
        r_data = requests.post(url, headers=headers, data=data)
        r_data = json.loads(r_ques.text)
    except:
        print ('Something wrong with ', text)
        r_data = {}
    return r_data

good_gold_title_match = 0
total_count = 0
data_wikidata_dict = {}
for data in json.load(open(data_file)):
    title = data['positive_ctxs'][0]['title']
    question = data['question']
    if question not in data_wikidata_dict:
        data_wikidata_dict[question] = {'gold_title': {'text':title}}

for file in os.listdir(data_file_wikidata_goldtitle):
    file = os.path.join(data_file_wikidata_goldtitle, file)
    data = json.load(open(file))
    for data_i in data:
        question = data_i['question']
        ques_text = question['text']
        if ques_text not in data_wikidata_dict:
            raise Exception(ques_text, 'not found!')
        #if 'entities_wikidata' not in question:
        #    question.update(get_wikidata_info(data_wikidata_dict[ques_text]['gold_title']['text']))
        if 'entities_wikidata' not in question:
            continue
        ques_entities = question['entities_wikidata']
        ques_relations = question['relations_wikidata']
        
        d = {'entities_wikidata': ques_entities, 'relations_wikidata': ques_relations}
        data_wikidata_dict[ques_text]['gold_title'].update(d)
        total_count += 1
        good_gold_title_match_i = False
        for e in ques_entities:
            e_text = e[1]
            if e_text.lower()==data_wikidata_dict[ques_text]['gold_title']['text'].lower():
                good_gold_title_match_i = True
        for r in ques_relations:
            r_text = r[1]
            if r_text.lower()==data_wikidata_dict[ques_text]['gold_title']['text'].lower():
                good_gold_title_match_i = True
        if good_gold_title_match_i:
            good_gold_title_match += 1


print ('good_gold_title_match ', good_gold_title_match, 'total_count', total_count)


for file in os.listdir(data_file_wikidata_qas):
    file = os.path.join(data_file_wikidata_qas, file)
    data = json.load(open(file))
    for data_i in data:
        question = data_i['question']
        ques_text = question['text'].replace(' ?', '')
        if ques_text not in data_wikidata_dict:
            #print(ques_text, 'not found!')
            continue
        #if 'entities_wikidata' not in question:
        #    question.update(get_wikidata_info(question['text']))
        if 'entities_wikidata' not in question:
            continue
        ques_entities = question['entities_wikidata']
        ques_relations = question['relations_wikidata']
        
        goldtitle_tokens = set(data_wikidata_dict[ques_text]['gold_title']['text'].lower().split(' '))
        ques_goldtitle_url = set([])
        if 'entities_wikidata' in data_wikidata_dict[ques_text]['gold_title']:
            ques_goldtitle_url.update(set([e[0] for e in data_wikidata_dict[ques_text]['gold_title']['entities_wikidata']]))
        if 'relations_wikidata' in data_wikidata_dict[ques_text]['gold_title']:
            ques_goldtitle_url.update(set([r[0] for r in data_wikidata_dict[ques_text]['gold_title']['relations_wikidata']]))
        matched_tokens = set([])
        good_question_match_i = False
        for e in ques_entities:
            if e[0] in ques_goldtitle_url:
                good_question_match_i = True
            matched_tokens.update(e[1].lower().split(' '))
        for r in ques_relations:
            if r[0] in ques_goldtitle_url:
                good_question_match_i = True
            matched_tokens.update(e[1].lower().split(' '))
        data_wikidata_dict[ques_text]['good_question_match'] = good_question_match_i
        unmatched_tokens = goldtitle_tokens - matched_tokens
        frac_goldtitle_unmatched_tokens = len(unmatched_tokens)/len(goldtitle_tokens)
        data_wikidata_dict[ques_text]['frac_goldtitle_unmatched_tokens'] = frac_goldtitle_unmatched_tokens
        avg_frac_answer_unmatched_tokens = []
        good_answer_match = 0
        num_answers  = len(set([x['text'] for x in data_i['answer']]))
        if 'answer' in data_i:
            for answer in data_i['answer']:
                if 'entities_wikidata' not in answer:
                    continue
                answer_entities = answer['entities_wikidata']
                answer_relations = answer['relations_wikidata']
                answer_text = answer['text'].lower()
                answer_tokens = set(answer_text.split(' '))
                matched_tokens = set([])
                good_answer_match_i = False
                for e in answer_entities:
                    e_text = e[1]
                    if e_text.lower()==answer_text:
                        good_answer_match_i = True
                    matched_tokens.update(e_text.split(' '))
                for r in answer_relations:
                    r_text = r[1]
                    if r_text.lower()==answer_text:
                        good_answer_match_i = True
                    matched_tokens.update(r_text.split(' '))
                if good_answer_match_i:
                    good_answer_match += 1.
                unmatched_tokens = answer_tokens - matched_tokens
                frac_answer_unmatched_tokens = len(unmatched_tokens)/len(answer_tokens)
                avg_frac_answer_unmatched_tokens.append(frac_answer_unmatched_tokens)
        data_wikidata_dict[ques_text]['good_answer_match']  = float(good_answer_match_i)/num_answers
        if len(avg_frac_answer_unmatched_tokens)==0:
            avg_frac_answer_unmatched_tokens = 0
        else:
            avg_frac_answer_unmatched_tokens = sum(avg_frac_answer_unmatched_tokens)/float(len(avg_frac_answer_unmatched_tokens))
        data_wikidata_dict[ques_text]['avg_frac_answer_unmatched_tokens'] = avg_frac_answer_unmatched_tokens
        

good_question_match = 0
good_answers_match = 0
total_count = 0
frac_goldtitle_notcovered = 0
frac_answer_notcovered = 0

for ques_text in data_wikidata_dict:
    if 'good_question_match' not in data_wikidata_dict[ques_text]:
        print ('Something wrong ', data_wikidata_dict[ques_text])
        continue
    total_count += 1
    good_question_match += float(data_wikidata_dict[ques_text]['good_question_match'])
    good_answers_match += data_wikidata_dict[ques_text]['good_answer_match']
    frac_goldtitle_notcovered += data_wikidata_dict[ques_text]['frac_goldtitle_unmatched_tokens'] 
    frac_answer_notcovered += data_wikidata_dict[ques_text]['avg_frac_answer_unmatched_tokens']

print ('good_question_match ', good_question_match)
print ('good_answer_match ', good_answers_match)
print ('frac_answer_notcovered ', frac_answer_notcovered, 'frac_goldtitle_notcovered', frac_goldtitle_notcovered)
print ('total_count', total_count)