# change this to the location which contains the below files
DATA_DIR = '/export/share/amrita/efficientQA/'  

# make directory for downloading wikidata5m processed files
WIKIDATA5M_DIR = DATA_DIR+'/wikidata/wikidata5m' 
# download from https://www.dropbox.com/s/86gukevtdbhpcbk/wikidata5m_triplet.txt.gz?dl=1 and extract wikidata5m_triplet.txt
# download from https://www.dropbox.com/s/7jp4ib8zo3i6m10/wikidata5m_text.txt.gz?dl=1 and extract wikidata5m_text.txt 
# download from https://www.dropbox.com/s/s1q38yzqzvuodl3/wikidata5m_alias.tar.gz?dl=1 and extract wikidata5m_entity.txt and wikidata5m_relation.txt 
WIKIDATA5M_ENTITIES_FILE = WIKIDATA5M_DIR+'/wikidata5m_entity.txt'  

# download wikidata5m_doctitle_entity.zip from https://drive.google.com/file/d/1T2So_lvbueJhip3YuVRv503VZvtp2tl4/view?usp=sharing and extract here 
WIKIDATA5M_DOCTITLE_ENTITY_DIR = WIKIDATA5M_DIR+'/wikidata5m_doctitle_entity'   

# download the following data from DPR using
'''
python data/download_data.py \
	--resource {key from download_data.py's RESOURCES_MAP}  \
	[optional --output_dir {your location}]
'''
DPR_WIKIDATA_DIR = DATA_DIR+'/data/wikipedia_split'
DPR_TRAIN_DATA = DATA_DIR+'/data/retriever/nq-train.json'
DPR_DEV_DATA = DATA_DIR+'data/retriever/nq-dev.json'
WIKIPEDIA_CHUNKS_FILE = DPR_WIKIDATA_DIR+'/psgs_w100.tsv'

# change this to the location which contains the following db
HYPERLINKED_WIKIPEDIA_DIR = '/export/share/k.hashimoto/nq_project/nq_models/wiki_db/'  

# download nq_models.zip from https://drive.google.com/file/d/120JNI49nK-W014cjneuXJuUC09To3KeQ/view and extract it and put the file 
# nq_project/nq_models/wiki_db/wiki_20181220_nq_hyper_linked.db in the above dir. 
# for more details see https://github.com/AkariAsai/learning_to_retrieve_reasoning_paths
HYPERLINKED_WIKIPEDIA_DB = HYPERLINKED_WIKIPEDIA_DIR+'/wiki_20181220_nq_hyper_linked.db'  
HYPERLINKED_WIKIPEDIA_DB_TITLE_TO_ROW_MAP = DPR_WIKIDATA_DIR+'/hyperlinked_wikidb_title_to_row.db'