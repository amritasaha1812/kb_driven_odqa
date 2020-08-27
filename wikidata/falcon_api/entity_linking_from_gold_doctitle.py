import requests
import json 

data_file = '/export/share/amrita/efficientQA/data/retriever/nq-dev_nq_gold_appended.json'
data_file_wikidata = '/export/share/amrita/efficientQA/data/wikidata/gold_title/nq-dev'

headers = {'Content-type': 'application/json'}
url = 'https://labs.tib.eu/falcon/falcon2/api?mode=long'
count = 0
docs = []
num_files = 0
for data in json.load(open(data_file)):
    title = data['positive_ctxs'][0]['title']
    question = data['question']
    try:
        data = '{"text":"'+title+'"}'
        r_title = requests.post(url, headers=headers, data=data)
        r_title = json.loads(r_title.text)
    except:
        r_title = {}
        print ('Something wrong with \"'+title+'\"')
    r_title['text'] = question
    doc = {'question': r_title}
    docs.append(doc)
    count+=1
    if count % 10 ==0:
        print ('Finished ', count)
    if count%1000==0:
        json.dump(docs, open(data_file_wikidata+'.'+str(num_files)+'.json', 'w'), indent=4)
        num_files+=1


