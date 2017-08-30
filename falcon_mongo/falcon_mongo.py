from gevent import monkey
monkey.patch_all()
import falcon, pymongo, ujson, json
from dateutil.relativedelta import relativedelta
import datetime, calendar
api = application = falcon.API()


class Get_Entity(object):
    def on_get(self, req, resp, entity, id):
        mongo_uri = "mongodb://localhost:27017"
        db = pymongo.MongoClient(mongo_uri).travels_db
        try:
            id = int(id)
        except:
            raise falcon.HTTPError(falcon.HTTP_404)
        db_entity = {"users": db.users, "locations": db.locations, "visits": db.visits}
        # Check entity
        if entity in ['users', "locations", "visits"]:
            data = db_entity[entity].find_one({"id": int(id)}, {"_id": False})
            data_json = ujson.dumps(data, ensure_ascii=False)
            if data is not None:
                resp.status = falcon.HTTP_200
                resp.body = data_json
            else:
                raise falcon.HTTPError(falcon.HTTP_404)

    def on_post(self, req, resp, entity, id):
        mongo_uri = "mongodb://localhost:27017"
        db = pymongo.MongoClient(mongo_uri).travels_db
        # Update entity
        if id != 'new':
            try:
                id = int(id)
            except:
                raise falcon.HTTPError(falcon.HTTP_404)
            # Check json is not empty
            if req.content_length:
                body = json.load(req.stream)
            else:
                raise falcon.HTTPError(falcon.HTTP_400)
            # Check fields are not empty and integer fields are ok
            for field in body:
                if body[field] is None:
                    raise falcon.HTTPError(falcon.HTTP_400)
                if field in ['distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
                    try:
                        int(body[field])
                    except:
                        raise falcon.HTTPError(falcon.HTTP_400)
            # Check entity exists
            db_entity = {"users": db.users, "locations": db.locations, "visits": db.visits}
            if entity in ['users', "locations", "visits"]:
                data = db_entity[entity].find({"id": id}).count()
                if data == 0:
                    raise falcon.HTTPError(falcon.HTTP_404)
            # update records
            db_entity[entity].update({"id": id}, {"$set": body})
            resp.status = falcon.HTTP_200
            resp.body = ujson.dumps({})
        # New entity
        else:
            # Check json is not empty
            if req.content_length:
                body = json.load(req.stream)
            else:
                raise falcon.HTTPError(falcon.HTTP_400)
            for field in body:
                # Check if body is empty
                if body[field] is None:
                    raise falcon.HTTPError(falcon.HTTP_400)
                # Check fields with numbers if they are numbers
                if field in ['id', 'distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
                    try:
                        int(body[field])
                    except:
                        raise falcon.HTTPError(falcon.HTTP_400)
            # Check entity exist
            db_entity = {"users": db.users, "locations": db.locations, "visits": db.visits}
            if entity in ['users', "locations", "visits"]:
                data = db_entity[entity].find({"id": body['id']}).count()
                if data != 0:
                    raise falcon.HTTPError(falcon.HTTP_400)
            db_entity[entity].insert(body)
            resp.status = falcon.HTTP_200
            resp.body = ujson.dumps({})


class Get_visits(object):
    def on_get(self, req, resp, id):
        mongo_uri = "mongodb://localhost:27017"
        db = pymongo.MongoClient(mongo_uri).travels_db
        try:
            id = int(id)
        except:
            raise falcon.HTTPError(falcon.HTTP_404)
        # Check if user exists
        data = db.users.find({"id": id}).count()
        if data == 0:
            raise falcon.HTTPError(falcon.HTTP_404)
        # Which params we will use to search db
        match_params = [{"user": id}]  # At least id
        for argument in ['toDate', 'fromDate', 'country', 'toDistance']:
            # Check if arguments in not None
            if argument in req.params:
                if req.get_param(argument) is None:
                    raise falcon.HTTPError(falcon.HTTP_400)
                # Check numbers if they are integers
                if argument in ['toDate', 'fromDate', 'toDistance']:
                    try:
                        int(req.get_param(argument))
                    except:
                        raise falcon.HTTPError(falcon.HTTP_400)
                # Working with parameters
                if argument == 'fromDate':
                    match_params.append({"visited_at": {"$gt": int(req.get_param('fromDate'))}})
                elif argument == "toDate":
                    match_params.append({"visited_at": {"$lt": int(req.get_param('toDate'))}})
                elif argument == 'country':
                    match_params.append({"locations.country": req.get_param('country')})
                elif argument == 'toDistance':
                    match_params.append({"locations.distance": {"$lt": int(req.get_param('toDistance'))}})
        # Final request
        result = list(db.visits.aggregate([{"$lookup": {"from": "locations",  # Aggregate data from locations to get place
                                          "localField": "location",
                                          "foreignField": "id",
                                          "as": "locations"}},
                             {"$match": {"$and": match_params}},
                             {"$project": {"_id": False,
                                           "mark": True,
                                           "visited_at": True,
                                           "place": {"$arrayElemAt": ["$locations.place", 0]}, # Select only place field from locations
                                           }},
                             {"$sort": {"visited_at": pymongo.ASCENDING}}
                             ])
                      )
        resp.status = falcon.HTTP_200
        resp.body = ujson.dumps({"visits": result}, ensure_ascii=False)


class Average_mark(object):
    def on_get(self, req, resp, id):
        mongo_uri = "mongodb://localhost:27017"
        db = pymongo.MongoClient(mongo_uri).travels_db
        try:
            id = int(id)
        except:
            raise falcon.HTTPError(falcon.HTTP_404)
        # Check if user exists
        data = db.locations.find({"id": id}).count()
        if data == 0:
            raise falcon.HTTPError(falcon.HTTP_404)
        # Which params we will use to search db
        match_params = [{"location": id}]  # At least id
        for argument in ['toDate', 'fromDate', 'fromAge', 'toAge', 'gender']:
            # Check if arguments in not None
            if argument in req.params:
                if req.get_param(argument) is None:
                    raise falcon.HTTPError(falcon.HTTP_404)
                # Check numbers if they are integers
                if argument in ['toDate', 'fromDate', 'fromAge', 'toAge']:
                    try:
                        int(req.get_param(argument))
                    except:
                        raise falcon.HTTPError(falcon.HTTP_400)
                # Working with parameters
                if argument == 'fromDate':
                    match_params.append({"visited_at": {"$gt": int(req.get_param('fromDate'))}})
                elif argument == "toDate":
                    match_params.append({"visited_at": {"$lt": int(req.get_param('toDate'))}})
                elif argument == 'fromAge':
                    now = datetime.datetime.now() - relativedelta(years=int(req.get_param('fromAge')))
                    timestamp = calendar.timegm(now.timetuple())
                    match_params.append({"users.birth_date": {"$lt": timestamp}})
                elif argument == 'toAge':
                    now = datetime.datetime.now() - relativedelta(years=int(req.get_param('toAge')))
                    timestamp = calendar.timegm(now.timetuple())
                    match_params.append({"users.birth_date": {"$gt": timestamp}})
                elif argument == 'gender':
                    # check if gender f or m
                    if req.get_param('gender') == 'f' or req.get_param('gender') == 'm':
                        match_params.append({"users.gender": req.get_param('gender')})
                    else:
                        raise falcon.HTTPError(falcon.HTTP_400)
        # Final request
        result = list(db.visits.aggregate([{"$lookup": {"from": "locations",  # Aggregate data from locations to get place
                                          "localField": "location",
                                          "foreignField": "id",
                                          "as": "locations"}},
                             {"$lookup": {"from": "users",  # Join data from users collection to count age
                                          "localField": "user",
                                          "foreignField": "id",
                                          "as": "users"}},
                             {"$match": {"$and": match_params}},
                             {"$project": {"_id": False,
                                           "mark": True}},  # We interested only in marks
                             ])
                       )
        if len(result) > 0:  # if no result - return avg: 0.0
            marks_sum = 0
            for row in result:
                marks_sum += row['mark']
            resp.status = falcon.HTTP_200
            resp.body = ujson.dumps({'avg': round(marks_sum / len(result), 5)}, ensure_ascii=False)
        else:
            resp.status = falcon.HTTP_200
            resp.body = ujson.dumps({'avg': 0.0}, ensure_ascii=False)


entities = Get_Entity()
visits = Get_visits()
avg = Average_mark()


api.add_route('/{entity}/{id}', entities)
api.add_route('/users/{id}/visits', visits)
api.add_route('/locations/{id}/avg', avg)