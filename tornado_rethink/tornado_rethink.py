import rethinkdb as r
from tornado import gen
import tornado.ioloop, tornado.web
import tornado.httpserver
import tornado.process
import tornado.netutil
import ujson, datetime, calendar
from dateutil.relativedelta import relativedelta
import logging
import pylibmc
from rethinkpool import RethinkPool
from contextlib import contextmanager
from tornado.platform.asyncio import AsyncIOMainLoop
import asyncio
import uvloop


entities = ['users', 'visits', 'locations']


class EntitiesHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, entity, id):
        if entity in entities:
            # Check if we have record in DB
            with mc_pool.reserve() as mc:
                data = mc.get(entity + id)
            if data is None: # Then try to get ti from DB
                with pool.get_resource() as res:
                    connection = yield res.conn
                    id = int(id)
                    data = yield r.table(entity).get(id).to_json().run(connection)
                    if data != 'null':
                        self.write(data)
                    else: # If record wasn't found in MC and DB - return 404
                        raise tornado.web.HTTPError(404)
            else:
                self.write(ujson.dumps(data, ensure_ascii=False))

    @gen.coroutine
    def post(self, entity, id):
        id = int(id)
        with pool.get_resource() as res:
            connection = yield res.conn
            # check body is None
            try:
                body = ujson.loads(self.request.body.decode('utf-8'))
            except:
                raise tornado.web.HTTPError(400)
            # Check entity exists
            if entity in ['users', "locations", "visits"]:
                data = yield r.table(entity).get_all(id).limit(1).count().run(connection)
                if data == 0:
                    raise tornado.web.HTTPError(404)
            # Check fields are not empty and integer fields are ok
            for field in body:
                if body[field] is None:
                    raise tornado.web.HTTPError(400)
                if field in ['distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
                    try:
                        int(body[field])
                    except:
                        raise tornado.web.HTTPError(400)
            # update records
            update = yield r.table(entity).get(id).update(body, return_changes=True).run(connection, durability='soft')
            # Check if we have any updates
            if len(update['changes']) != 0:
                changed_row = update['changes'][0]['new_val']
                with mc_pool.reserve() as mc: # If yes - then we need to update our key in MC
                    mc.set(entity + str(id), changed_row)
            self.write(ujson.dumps({}))


class GetUserVisitsHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, id):
        id = int(id)
        with pool.get_resource() as res:
            # Check if user exists
            connection = yield res.conn
            data = yield r.table('users').get_all(id).limit(1).count().run(connection)
            if data == 0:
                raise tornado.web.HTTPError(404)
            # Start build DB query
            result = r.table('visits').get_all(id, index='user').limit(1).eq_join('location', r.table('locations')).without(
                {'right': "id"}, {
                    "right": "city"}).zip()
            request_arguments = self.request.arguments.keys()
            for argument in ['toDate', 'fromDate', 'country', 'toDistance']:
                # Check if arguments in not None
                if argument in request_arguments:
                    if self.get_argument(argument) is None:
                        raise tornado.web.HTTPError(400)
                    # Check numbers if they are integers
                    if argument in ['toDate', 'fromDate', 'toDistance']:
                        try:
                            int(self.get_argument(argument))
                        except:
                            raise tornado.web.HTTPError(400)
                    # Working with parameters
                    if argument == 'fromDate':
                        result = result.filter(r.row['visited_at'] > int(self.get_argument(argument)))
                    elif argument == "toDate":
                        result = result.filter(r.row['visited_at'] < int(self.get_argument(argument)))
                    elif argument == 'country':
                        result = result.filter({"country": self.get_argument(argument)})
                    elif argument == 'toDistance':
                        result = result.filter(r.row["distance"] < int(self.get_argument(argument)))
            # Final request
            result = yield result.pluck('mark', 'visited_at', 'place').order_by('visited_at').run(connection)
            self.write(ujson.dumps({"visits": result}, ensure_ascii=False))


class GetLocationAvgMark(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, id):
        id = int(id)
        with pool.get_resource() as res:
            connection = yield res.conn
            # Check if user exists
            data = yield r.table('locations').get_all(id).limit(1).count().run(connection)
            if data == 0:
                raise tornado.web.HTTPError(404)
            # Start build DB query
            result = r.table('visits').get_all(id, index='location').eq_join('user',
                                                                             r.db('travels').table('users')).without(
                {'right': 'id'},
                {'right': 'first_name'},
                {'right': 'last_name'},
                {"right": "email"}).zip()
            request_arguments = self.request.arguments.keys()
            for argument in ['toDate', 'fromDate', 'fromAge', 'toAge', 'gender']:
                # Check if arguments in not None
                if argument in request_arguments:
                    if self.get_argument(argument) is None:
                        raise tornado.web.HTTPError(400)
                    # Check numbers if they are integers
                    if argument in ['toDate', 'fromDate', 'fromAge', 'toAge']:
                        try:
                            int(self.get_argument(argument))
                        except:
                            raise tornado.web.HTTPError(400)
                    # Working with parameters
                    if argument == 'fromDate':
                        result = result.filter(r.row['visited_at'] > int(self.get_argument('fromDate')))
                    elif argument == "toDate":
                        result = result.filter(r.row['visited_at'] < int(self.get_argument('toDate')))
                    elif argument == 'fromAge':
                        now = datetime.datetime.now() - relativedelta(years=int(self.get_argument('fromAge')))
                        timestamp = calendar.timegm(now.timetuple())
                        result = result.filter(r.row['birth_date'] < timestamp)
                    elif argument == 'toAge':
                        now = datetime.datetime.now() - relativedelta(years=int(self.get_argument('toAge')))
                        timestamp = calendar.timegm(now.timetuple())
                        result = result.filter(r.row['birth_date'] > timestamp)
                    elif argument == 'gender':
                        # check if gender f or m
                        if self.get_argument('gender') == 'f' or self.get_argument('gender') == 'm':
                            result = result.filter(r.row['gender'] == self.get_argument('gender'))
                        else:
                            raise tornado.web.HTTPError(400)
            # Final request
            result = yield result.avg('mark').default(None).run(connection)
            if result is not None:  # if no result - return avg: 0.0
                self.write(ujson.dumps({'avg': round(result, 5)}))
            else:
                self.write(ujson.dumps({'avg': 0.0}))


class NewEntity(tornado.web.RequestHandler):
    @gen.coroutine
    def post(self, entity):
        # Check json is not empty
        try:
            body = ujson.loads(self.request.body.decode('utf-8'))
        except:
            raise tornado.web.HTTPError(400)
        for field in body:
            # Check if body is empty
            if body[field] is None:
                raise tornado.web.HTTPError(400)
            # Check fields with numbers if they are numbers
            if field in ['id', 'distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
                try:
                    int(body[field])
                except:
                    raise tornado.web.HTTPError(400)
        with pool.get_resource() as res:
            connection = yield res.conn
            if entity in ['users', "locations", "visits"]:
                # Check entity exist
                data = yield r.table(entity).get_all(int(body['id'])).limit(1).count().run(connection)
                if data != 0:
                    raise tornado.web.HTTPError(400)
            # Inser new record
            yield r.table(entity).insert(body).run(connection, durability='soft')
            with mc_pool.reserve() as mc:
                mc.set(entity + str(body['id']), body) # Update MC
            self.write(ujson.dumps({}))


application = tornado.web.Application([
    (r"/(?P<entity>[^\/]+)/(?P<id>[0-9]+)", EntitiesHandler),
    (r"/users/(?P<id>[0-9]+)/visits", GetUserVisitsHandler),
    (r"/locations/(?P<id>[0-9]+)/avg", GetLocationAvgMark),
    (r"/(?P<entity>[^\/]+)/new", NewEntity),
    ], debug=False, autoreload=False)


# MC Client pool
class ClientPool(list):
    @contextmanager
    def reserve(self):
        mc = self.pop()
        try:
            yield mc
        finally:
            self.append(mc)


if __name__ == "__main__":
    logging.getLogger('tornado.access').setLevel(logging.CRITICAL)
    sockets = tornado.netutil.bind_sockets(80) # Bind port
    tornado.process.fork_processes(0)
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy()) # Change loop to UVLOOP
    AsyncIOMainLoop().install() # Change Main Tornado loop to AsyncIO
    # Init MC
    mc = pylibmc.Client(["127.0.0.1"], binary=True,
                        behaviors={"tcp_nodelay": True,
                                   "ketama": True})
    mc_pool = ClientPool(mc.clone() for i in range(128))
    # Init DB connection pool
    r.set_loop_type('tornado')
    pool = RethinkPool(host='127.0.0.1', port=28015, db='travels', max_conns=1024, initial_conns=512)
    # Start HTTP Server
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.add_sockets(sockets)
    print('WEB SERVER IS UP')
    asyncio.get_event_loop().run_forever()