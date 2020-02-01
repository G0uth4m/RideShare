from flask import Flask, request, Response, jsonify
import pymongo
import requests
import re
from pandas import read_csv
from datetime import datetime

app = Flask(__name__)


@app.route('/api/v1/users', methods=["PUT"])
def add_user():
    request_data = request.get_json(force=True)

    try:
        username = request_data["username"]
        password = request_data["password"]
    except KeyError:
        print("Inappropriate request received")
        return Response(status=400)

    if isUserPresent(username):
        print("Username already taken")
        return Response(status=400)

    if re.match(re.compile(r'\b[0-9a-f]{40}\b'), password) is None:
        print("Not a SHA-1 password")
        return Response(status=400)

    post_data = {"insert": [username, password], "columns": ["_id", "password"], "table": "users"}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)

    if response.status_code == 400:
        print("Error while inserting user to database")
        return Response(status=400)

    return Response(status=201, response='{}', mimetype='application/json')


@app.route('/api/v1/users/<username>', methods=["DELETE"])
def remove_user(username):
    if not isUserPresent(username):
        print("User not present")
        return Response(status=400)

    # TODO : Check if user has created any ride. If yes, don't delete

    # TODO : Check if user is in any ride. If yes, cascade delete

    post_data = {'column': '_id', 'delete': username, 'table': 'users'}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
    return Response(status=response.status_code, response='{}', mimetype='application/json')


@app.route('/api/v1/rides', methods=["POST"])
def create_ride():
    request_data = request.get_json(force=True)
    try:
        created_by = request_data['created_by']
        time_stamp = request_data['timestamp']
        source = int(request_data['source'])
        destination = int(request_data['destination'])
    except KeyError:
        print("Inappropriate request received")
        return Response(status=400)

    try:
        req_date = convert_timestamp_to_datetime(time_stamp)
    except:
        print("Invalid timestamp")
        return Response(status=400)

    if (source > len(areas) or destination > len(areas)) and (source < 1 or destination < 1):
        print("Invalid source or destination")
        return Response(status=400)

    if not isUserPresent(created_by):
        print("User not present")
        return Response(status=400)

    try:
        f = open('seq.txt', 'r')
        ride_count = int(f.read())
        f.close()

        post_data = {
            "insert": [ride_count + 1, ride_count + 1, created_by, time_stamp, areas[source-1][1], areas[destination-1][1], []],
            "columns": ["_id", "rideId", "created_by", "timestamp", "source", "destination", "users"], "table": "rides"}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)

        if response.status_code == 400:
            print("Error while writing to database")
            return Response(status=400)
        else:
            f = open('seq.txt', 'w')
            f.write(str(ride_count + 1))
            f.close()
            return Response(status=201, response='{}', mimetype='application/json')
    except:
        print("Error while writing to database")
        return Response(status=400)


@app.route('/api/v1/rides', methods=["GET"])
def list_rides_between_src_and_dst():
    source = request.args.get("source")
    destination = request.args.get("destination")

    if source is None or destination is None:
        print("Inappropriate get parameters received")
        return Response(status=400)

    try:
        source = int(source)
        destination = int(destination)
    except:
        return Response(status=400)

    if (source > len(areas) or destination > len(areas)) and (source < 1 or destination < 1):
        print("Areas not found")
        return Response(status=400)

    post_data = {"many": 1, "table": "rides", "columns": ["rideId", "created_by", "timestamp"],
                 "where": {"source": areas[source-1][1], "destination": areas[destination-1][1], "timestamp": {"$gt": convert_datetime_to_timestamp(datetime.now())}}}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)

    if response.status_code == 400:
        return Response(status=400)

    result = response.json()
    for i in range(len(result)):
        if "_id" in result[i]:
            del result[i]["_id"]

    return jsonify(result)


@app.route('/api/v1/rides/<rideId>', methods=["GET", "POST", "DELETE"])
def get_details_of_ride_or_join_ride_or_delete_ride(rideId):
    try:
        a = int(rideId)
    except:
        return Response(status=400)

    if request.method == "GET":
        post_data = {"table": "rides",
                     "columns": ["rideId", "created_by", "users", "timestamp", "source", "destination"],
                     "where": {"rideId": int(rideId)}}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
        res = response.json()
        print(res)
        if res is None:
            return Response(status=204)
        del res["_id"]
        return jsonify(res)

    elif request.method == "POST":
        username = request.get_json(force=True)["username"]
        if not isUserPresent(username):
            print("User not present")
            return Response(status=400)
        post_data = {"table": "rides", "where": {"rideId": int(rideId)}, "update": "users", "data": username,
                     "operation": "push"}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
        return Response(status=response.status_code, response='{}', mimetype='application/json')

    elif request.method == "DELETE":
        if not isRidePresent(rideId):
            print("Ride not present")
            return Response(status=400)

        post_data = {'column': 'rideId', 'delete': int(rideId), 'table': 'rides'}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
        return Response(status=response.status_code, response='{}', mimetype='application/json')


@app.route('/api/v1/db/write', methods=["POST"])
def write_to_db():
    request_data = request.get_json(force=True)

    if 'delete' in request_data:
        try:
            delete = request_data['delete']
            column = request_data['column']
            collection = request_data['table']
        except KeyError:
            print("Inappropriate request received")
            return Response(status=400)

        try:
            query = {column: delete}
            collection = db[collection]
            collection.delete_one(query)
            return Response(status=200)
        except:
            print("Mongo query failed")
            return Response(status=400)

    if 'update' in request_data:
        try:
            collection = request_data['table']
            where = request_data['where']
            array = request_data['update']
            data = request_data['data']
            operation = request_data['operation']
        except KeyError:
            print("Inappropriate request received")
            return Response(status=400)

        try:
            collection = db[collection]
            collection.update_one(where, {"$" + operation: {array: data}})
            return Response(status=200)
        except:
            return Response(status=400)

    try:
        insert = request_data['insert']
        columns = request_data['columns']
        collection = request_data['table']
    except KeyError:
        print("Inappropriate request received")
        return Response(status=400)

    try:
        document = {}
        for i in range(len(columns)):
            if columns[i] == "timestamp":
                document[columns[i]] = convert_timestamp_to_datetime(insert[i])
            else:
                document[columns[i]] = insert[i]

        collection = db[collection]
        collection.insert_one(document)
        return Response(status=201)

    except:
        return Response(status=400)


@app.route('/api/v1/db/read', methods=["POST"])
def read_from_db():
    request_data = request.get_json(force=True)
    try:
        table = request_data['table']
        columns = request_data['columns']
        where = request_data['where']
    except KeyError:
        print("Inappropriate request received")
        return Response(status=400)

    if "timestamp" in where:
        where["timestamp"]["$gt"] = convert_timestamp_to_datetime(where["timestamp"]["$gt"])

    filter = {}
    for i in columns:
        filter[i] = 1

    if 'many' in request_data:
        try:
            collection = db[table]
            res = []
            for i in collection.find(where, filter):
                if "timestamp" in i:
                    i["timestamp"] = convert_datetime_to_timestamp(i["timestamp"])
                res.append(i)

            return jsonify(res)
        except:
            return Response(status=400)

    try:
        collection = db[table]
        result = collection.find_one(where, filter)
        if "timestamp" in result:
            result["timestamp"] = convert_datetime_to_timestamp(result["timestamp"])
        return jsonify(result)
    except:
        return Response(status=400)


def isUserPresent(username):
    post_data = {'table': 'users', 'columns': ['_id'], 'where': {'_id': username}}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    return response.status_code != 400 and response.text != 'null\n'


def isRidePresent(rideId):
    post_data = {'table': 'rides', 'columns': ['rideId'], 'where': {'rideId': int(rideId)}}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    return response.status_code != 400 and response.text != 'null\n'


def isUserInRide(username):
    pass


def convert_datetime_to_timestamp(k):
    day = str(k.day) if len(str(k.day)) == 2 else "0" + str(k.day)
    month = str(k.month) if len(str(k.month)) == 2 else "0" + str(k.month)
    year = str(k.year)
    second = str(k.second) if len(str(k.second)) == 2 else "0" + str(k.second)
    minute = str(k.minute) if len(str(k.minute)) == 2 else "0" + str(k.minute)
    hour = str(k.hour) if len(str(k.hour)) == 2 else "0" + str(k.hour)
    return day + "-" + month + "-" + year + ":" + second + "-" + minute + "-" + hour


def convert_timestamp_to_datetime(time_stamp):
    day = int(time_stamp[0:2])
    month = int(time_stamp[3:5])
    year = int(time_stamp[6:10])
    seconds = int(time_stamp[11:13])
    minutes = int(time_stamp[14:16])
    hours = int(time_stamp[17:19])
    return datetime(year, month, day, hours, minutes, seconds)


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
    db = client["rideshare"]
    places = open("AreaNameEnum.csv", "r")
    areas = places.read()
    areas = areas.split('\n')
    for i in range(len(areas)):
        areas[i] = areas[i].split(',')
    areas.pop(0)
    areas.pop(-1)
    app.run(debug=True)
