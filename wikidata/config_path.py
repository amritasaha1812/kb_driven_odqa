#change this to the location which contains the below files
DATA_DIR = '/export/share/amrita/efficientQA/'  

#download wikidata5m and extract here 
WIKIDATA5M_DIR = DATA_DIR+'/wikidata/wikidata5m' 
WIKIDATA5M_ENTITIES_FILE = WIKIDATA5M_DIR+'/wikidata5m_entity.txt'  

#download wikidata5m_doctitle_entity.zip from https://drive.google.com/file/d/1T2So_lvbueJhip3YuVRv503VZvtp2tl4/view?usp=sharing and extract here 
WIKIDATA5M_DOCTITLE_ENTITY_DIR = WIKIDATA5M_DIR+'/wikidata5m_doctitle_entity'   

DPR_WIKIDATA_DIR = DATA_DIR+'/data/wikipedia_split'
DPR_TRAIN_DATA = DATA_DIR+'/data/retriever/nq-train.json'
DPR_DEV_DATA = DATA_DIR+'data/retriever/nq-dev.json'
WIKIPEDIA_CHUNKS_FILE = DPR_WIKIDATA_DIR+'/psgs_w100.tsv'

#change this to the location which contains the following db
HYPERLINKED_WIKIPEDIA_DIR = '/export/share/k.hashimoto/nq_project/nq_models/wiki_db/'  

#download nq_models.zip from https://drive.google.com/file/d/120JNI49nK-W014cjneuXJuUC09To3KeQ/view and extract it and put the file 
# nq_project/nq_models/wiki_db/wiki_20181220_nq_hyper_linked.db in the above dir. 
HYPERLINKED_WIKIPEDIA_DB = HYPERLINKED_WIKIPEDIA_DIR+'/wiki_20181220_nq_hyper_linked.db'  