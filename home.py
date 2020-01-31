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

    post_data = {'column': '_id', 'delete': username, 'table': 'users'}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
    return Response(status=response.status_code, response='{}', mimetype='aplication/json')


@app.route('/api/v1/rides', methods=["POST"])
def create_ride():
    request_data = request.get_json(force=True)
    try:
        created_by = request_data['created_by']
        time_stamp = request_data['timestamp']
        source = request_data['source']
        destination = request_data['destination']
    except KeyError:
        print("Inappropriate request received")
        return Response(status=400)

    try:
        day = int(time_stamp[0:2])
        month = int(time_stamp[3:5])
        year = int(time_stamp[6:10])
        seconds = int(time_stamp[11:13])
        minutes = int(time_stamp[14:16])
        hours = int(time_stamp[17:19])
        req_date = datetime(year, month, day, hours, minutes, seconds)
    except:
        print("Timestamp invalid")
        return Response(status=400)


    if source not in areas or destination not in areas:
        return Response(status=400)

    if re.match(re.compile(r''), time_stamp) is None:
        print("Invalid timestamp")
        return Response(status=400)

    if not isUserPresent(created_by):
        print("User not present")
        return Response(status=400)

    try:
        f = open('seq.txt', 'r')
        ride_count = int(f.read())
        f.close()

        post_data = {
            "insert": [ride_count + 1, ride_count + 1, created_by, time_stamp, source, destination, [created_by]],
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
    # if request.method != "GET":
    #     return Response(status=405)

    source = request.args.get("source")
    destination = request.args.get("destination")
    if source is None or destination is None:
        print("Inappropriate get parameters received")
        return Response(status=400)

    if source not in areas and destination not in areas:
        print("Areas not found")
        return Response(status=400)

    post_data = {"many": 1, "table": "rides", "columns": ["rideId", "created_by", "timestamp"], "where": {"source": source, "destination": destination}}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    if response.status_code == 400:
        return Response(status=400)
    return jsonify(response.json())


@app.route('/api/v1/rides/<rideId>', methods=["GET", "POST", "DELETE"])
def get_details_of_ride_or_join_ride_or_delete_ride(rideId):
    # if request.method not in ["GET", "POST", "DELETE"]:
    #     return Response(status=405)\
    try:
        a = int(rideId)
    except:
        return Response(status=405)

    if request.method == "GET":
        post_data = {"table": "rides",
                     "columns": ["rideId", "created_by", "users", "timestamp", "source", "destination"],
                     "where": "rideId=" + rideId}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
        res = response.json()
        if res is None:
            return Response(status=204)
        del res["_id"]
        return jsonify(res)

    elif request.method == "POST":
        username = request.get_json(force=True)["username"]
        if not isUserPresent(username):
            print("User not present")
            return Response(status=400)
        post_data = {"table": "rides", "where": "rideId=" + rideId, "update": "users", "data": username,
                     "operation": "push"}
        response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
        return Response(status=response.status_code, response='{}', mimetype='application/json')

    elif request.method == "DELETE":
        if not isRidePresent(rideId):
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
            if "=" in where:
                where = where.split("=")
                try:
                    where[1] = int(where[1])
                except:
                    pass
                print(where, operation, array, data)
                collection.update_one({where[0]: where[1]}, {"$" + operation: {array: data}})
                return Response(status=200)
            else:
                collection.update_one({}, {"$" + operation: {array: data}})
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



    filter = {}
    for i in columns:
        filter[i] = 1

    if 'many' in request_data:
        try:
            collection = db[table]
            res = []
            for i in collection.find(where, filter):
                res.append(i)

            return jsonify(res)
        except:
            return Response(status=400)

    try:
        collection = db[table]
        if where != '':
            where = where.split("=")
            try:
                where[1] = int(where[1])
            except:
                pass

            result = collection.find_one({where[0]: where[1]}, filter)
        else:
            result = collection.find_one({}, filter)

        return jsonify(result)
    except:
        return Response(status=400)


def isUserPresent(username):
    post_data = {'table': 'users', 'columns': ['_id'], 'where': '_id=' + username}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    return response.status_code != 400 and response.text != 'null\n'

def isRidePresent(rideId):
    post_data = {'table': 'rides', 'columns': ['rideId'], 'where': 'rideId=' + rideId}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    return response.status_code != 400 and response.text != 'null\n'


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
    db = client["rideshare"]
    areas = read_csv("AreaNameEnum.csv").iloc[:, 1:2].values
    app.run(debug=True)
