import rethinkdb as r

result = False
while not result:
    try:
        connection = r.connect(host='127.0.0.1', port=28015, db='travels')
        r.db_create('travels').run(connection)
        r.db('travels').table_create('users', durability='soft').run(connection)
        r.db('travels').table_create('visits', durability='soft').run(connection)
        r.db('travels').table_create('locations', durability='soft').run(connection)
        r.db('travels').table('visits').index_create('user').run(connection)
        r.db('travels').table('visits').index_create('location').run(connection)
        print("DB CREATED")
        result = True
    except:
        pass