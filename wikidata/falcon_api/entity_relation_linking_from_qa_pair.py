import requests
import json 

data_file = '/export/share/amrita/efficientQA/data/retriever/qas/nq-dev.csv'
data_file_wikidata = '/export/share/amrita/efficientQA/data/wikidata/qas/nq-dev'

headers = {'Content-type': 'application/json'}
url = 'https://labs.tib.eu/falcon/falcon2/api?mode=long'
count = 0
docs = []
num_files = 0
for line in open(data_file).readlines():
    line = line.strip().split('\t')
    question = line[0]+' ?'
    try:
        answer = json.loads(line[1].replace("'",'"'))
    except:
        print ('Error json loading from ', line[1].replace("'",'"'))
    try:
        data = '{"text":"'+question+'"}'
        #print (data)
        r_ques = requests.post(url, headers=headers, data=data)
        r_ques = json.loads(r_ques.text)
    except:
        r_ques = {}
        print ('Something wrong with \"'+question+'\"')
    r_ques['text'] = question
    r_answers = []
    for ans in answer:
        try:
            data='{"text":"'+ans+'"}'
            #print (data)
            r_ans = requests.post(url, headers=headers, data=data)
            r_ans = json.loads(r_ans.text)
        except:
            r_ans = {}
            print ('Something wrong with \"'+ans+'\"')
        r_ans['text'] = ans
        r_answers.append(r_ans)
    doc = {'question': r_ques, 'answer': r_answers}
    docs.append(doc)
    count+=1
    print ('Finished ', count)
    if count%100==0:
        json.dump(docs, open(data_file_wikidata+'.'+str(num_files)+'.json', 'w'), indent=4)
        num_files+=1


