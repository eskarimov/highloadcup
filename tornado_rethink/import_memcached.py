import pylibmc, sys, ujson

mc = pylibmc.Client(["127.0.0.1"], binary=True,
                    behaviors={"tcp_nodelay": True,
                               "ketama": True})

entity = sys.argv[1]
file_name = sys.argv[2]
data_folder = 'data'
entities_in_json_to_load = ujson.loads(open(file_name, encoding='utf-8').read())
for item in entities_in_json_to_load:
    check_key_exist = mc.get(entity + str(item['id']))
    if check_key_exist is None:
        mc.set(entity + str(item['id']), item)