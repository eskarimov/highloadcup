import pymongo, json, os


entities = ['users', 'visits', 'locations']
data_folder = 'data'
mongo_uri = "mongodb://127.0.0.1:27017"
db = pymongo.MongoClient(mongo_uri).travels_db
for entity in entities:
    json_files = [pos_json for pos_json in os.listdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), data_folder)) if
                           pos_json.startswith(entity)]
    for data_file in json_files:
        entities_in_json_to_load = json.load(open('/root/highloadcup/' + data_file, encoding='utf-8'))[entity]
        db.entity.insert(entities_in_json_to_load)
    # Create index
    db.entity.create_index('id', unique=True)