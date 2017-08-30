from sanic import Sanic
from sanic.response import json as sanic_json
from sanic.response import text
from sanic.exceptions import abort, ServerError
from dateutil.relativedelta import relativedelta
import datetime, calendar
import asyncio
import uvloop
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient
from bson.json_util import dumps

# Init sanic
app = Sanic()

# init db
@app.listener('before_server_start')
def init_db(sanic, loop):
    global db
    mongo_uri = "mongodb://localhost:27017"
    db = AsyncIOMotorClient(mongo_uri).travels_db


@app.route("/<entity>/<id:int>", methods=['GET'])
async def get_entity(request, entity, id):
    db_entity = {"users":db.users, "locations":db.locations, "visits":db.visits}
    # Check entity
    if entity in ['users', "locations", "visits"]:
        data = await db_entity[entity].find_one({"id": id}, {"_id": False})
        if data is not None:
            return text(dumps(data))
        else:
            raise ServerError('No such data', status_code=404)


@app.route("/users/<id:int>/visits", methods=['GET'])
async def get_user_visits(request, id):
    # Check if user exists
    data = await db.users.find({"id": id}).count()
    if data == 0:
        raise ServerError('No such data', status_code=404)
    # Which params we will use to search db
    match_params = [{"user": id}] # At least id
    for argument in ['toDate', 'fromDate', 'country','toDistance']:
        # Check if arguments in not None
        if argument in request.raw_args:
            if request.raw_args.get(argument) is None:
                raise ServerError('Bad argument', status_code=400)
            # Check numbers if they are integers
            if argument in ['toDate', 'fromDate', 'toDistance']:
                try:
                    int(request.raw_args.get(argument))
                except:
                    raise ServerError('Bad argument', status_code=400)
            # Working with parameters
            if argument == 'fromDate':
                match_params.append({"visited_at":{"$gt": int(request.raw_args.get('fromDate'))}})
            elif argument == "toDate":
                match_params.append({"visited_at": {"$lt": int(request.raw_args.get('toDate'))}})
            elif argument == 'country':
                match_params.append({"locations.country": request.raw_args.get('country')})
            elif argument == 'toDistance':
                match_params.append({"locations.distance": {"$lt": int(request.raw_args.get('toDistance'))}})
    # Final request
    result = await db.visits.aggregate([{"$lookup":{"from":"locations", # Aggregate data from locations to get place
                                                   "localField":"location",
                                                   "foreignField":"id",
                                                   "as":"locations"}},
                                        {"$match":{"$and":match_params}},
                                        {"$project": {"_id": False,
                                                      "mark":True,
                                                      "visited_at":True,
                                                      "place":{"$arrayElemAt":["$locations.place", 0]}, # Select only place field from locations
                                                      }},
                                        {"$sort":{"visited_at":pymongo.ASCENDING}}
                                        ]).to_list(None)
    return sanic_json({"visits":result})


@app.route('/locations/<id:int>/avg', methods=['GET'])
async def average_mark(request, id):
    # Check if user exists
    data = await db.locations.find({"id": id}).count()
    if data == 0:
        raise ServerError('No such data', status_code=404)
    # Which params we will use to search db
    match_params = [{"location": id}]  # At least id
    for argument in ['toDate', 'fromDate', 'fromAge', 'toAge', 'gender']:
        # Check if arguments in not None
        if argument in request.raw_args:
            if request.raw_args.get(argument) is None:
                raise ServerError('Bad argument', status_code=400)
            # Check numbers if they are integers
            if argument in ['toDate', 'fromDate', 'fromAge', 'toAge']:
                try:
                    int(request.raw_args.get(argument))
                except:
                    raise ServerError('Bad argument', status_code=400)
            # Working with parameters
            if argument == 'fromDate':
                match_params.append({"visited_at": {"$gt": int(request.raw_args.get('fromDate'))}})
            elif argument == "toDate":
                match_params.append({"visited_at": {"$lt": int(request.raw_args.get('toDate'))}})
            elif argument == 'fromAge':
                now = datetime.datetime.now() - relativedelta(years=int(request.raw_args.get('fromAge')))
                timestamp = calendar.timegm(now.timetuple())
                match_params.append({"users.birth_date": {"$lt": timestamp}})
            elif argument == 'toAge':
                now = datetime.datetime.now() - relativedelta(years=int(request.raw_args.get('toAge')))
                timestamp = calendar.timegm(now.timetuple())
                match_params.append({"users.birth_date": {"$gt": timestamp}})
            elif argument == 'gender':
                # check if gender f or m
                if request.raw_args.get('gender') == 'f' or request.raw_args.get('gender') == 'm':
                    match_params.append({"users.gender":request.raw_args.get('gender')})
                else:
                    raise ServerError('Bad gender', status_code=400)
    # Final request
    result = await db.visits.aggregate([{"$lookup":{"from":"locations", # Aggregate data from locations to get place
                                                   "localField":"location",
                                                   "foreignField":"id",
                                                   "as":"locations"}},
                                        {"$lookup":{"from":"users", # Join data from users collection to count age
                                                   "localField":"user",
                                                   "foreignField":"id",
                                                   "as":"users"}},
                                        {"$match":{"$and":match_params}},
                                        {"$project":{"_id":False,
                                                     "mark":True}}, # We interested only in marks
                                       ]).to_list(None)
    if len(result) > 0: # if no result - return avg: 0.0
        marks_sum = 0
        for row in result:
            marks_sum += row['mark']
        return sanic_json({'avg': round(marks_sum / len(result), 5)})
    else:
        return sanic_json({'avg': 0.0})


@app.route('/<entity>/<id:int>', methods=['POST'])
async def change_entity(request, entity, id):
    # Check json is not empty
    body = request.json
    if body is None:
        raise ServerError('Empty body', status_code=400)
    # Check fields are not empty and integer fields are ok
    for field in body:
        if body[field] is None:
            raise ServerError('Empty update data', status_code=400)
        if field in ['distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
            try:
                int(body[field])
            except:
                raise ServerError('Wrong integer', status_code=400)
    # Check entity exists
    db_entity = {"users": db.users, "locations": db.locations, "visits": db.visits}
    if entity in ['users', "locations", "visits"]:
        data = await db_entity[entity].find({"id": id}).count()
        if data == 0:
            raise ServerError('No such entity', status_code=404)
        #data = await db_entity[entity].find_one({"id": id}, {"_id": False})
        #if data == None:
        #    raise ServerError('No such entity', status_code=404)
    # update records
    await db_entity[entity].update({"id":id}, {"$set": body})
    return sanic_json({})


async def add_new_entity(request, entity):
    # Check json is not empty
    body = request.json
    if body is None:
        raise ServerError('Empty body', status_code=400)
    for field in body:
        # Check if body is empty
        if body[field] is None:
            raise ServerError('Empty update data', status_code=400)
        # Check fields with numbers if they are numbers
        if field in ['id', 'distance', 'user', 'location', 'visited_at', 'mark', 'birth_date']:
            try:
                int(body[field])
            except:
                raise ServerError('Wrong integer', status_code=400)
    # Check entity exist
    db_entity = {"users": db.users, "locations": db.locations, "visits": db.visits}
    if entity in ['users', "locations", "visits"]:
        data = await db_entity[entity].find({"id": body['id']}).count()
        if data != 0:
            raise ServerError('No such entity', status_code=404)
        #data = await db_entity[entity].find_one({"id": body['id']}, {"_id": False})
        #if data != None:
        #    raise ServerError('Id already exists', status_code=400)
    await db_entity[entity].insert(body)
    return sanic_json({})
app.add_route(add_new_entity, '/<entity>/new', methods=['POST'])

#set event loop policy to uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
# Run app
app.run(host="0.0.0.0", port=80, debug=False, log_config=None, workers=4)
