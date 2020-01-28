from flask import Flask, request, Response, jsonify
import pymongo
import requests
import re

app = Flask(__name__)


@app.route('/api/v1/users', methods=["PUT"])
def add_user():
    if request.method != "PUT":
        return Response(status=405)

    request_data  = request.get_json(force=True)

    try:
        username = request_data["username"]
        password = request_data["password"]
    except:
        return Response(status=400)

    if re.match(re.compile(r'\b[0-9a-f]{40}\b'), password) is None:
        return Response(status=400)

    post_data = {"insert": [username, password], "columns": ["_id", "password"], "table": "users"}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
    if response.status_code == 400:
        return Response(status=400)

    return Response(status=201)


@app.route('/api/v1/users/<username>', methods=["DELETE"])
def remove_user(username):
    if request.method != "DELETE":
        return Response(status=405)

    post_data = {'table': 'users', 'columns': ['_id'], 'where': '_id='+username}
    response = requests.post('http://127.0.0.1:5000/api/v1/db/read', json=post_data)
    if response.status_code == 400:
        return Response(status=400)

    else:
        if response.text == 'null\n':
            # user_present = False
            return Response(status=400)
        else:
            # user_present = True
            post_data = {'column': '_id', 'delete': username, 'table': 'users'}
            response = requests.post('http://127.0.0.1:5000/api/v1/db/write', json=post_data)
            return Response(status=response.status_code)

@app.route('/api/v1/rides', methods=["POST"])
def create_ride():
    request_data = request.get_json(force=True)
    try:
        created_by = request_data['created_by']
        time_stamp = request_data['timestamp']
        source = request_data['source']
        destination = request_data['destination']
    except:
        return Response(status=400)
    # TODO : Create ride and add to database


@app.route('/api/v1/rides', methods=["GET"])
def list_rides_between_src_and_dst():
    source = request.args.get("source")
    destination = request.args.get("destination")
    # TODO : send all rides between given source and destination as response


@app.route('/api/v1/rides/<rideId>', methods=["GET", "POST", "DELETE"])
def get_details_of_ride_or_join_ride_or_delete_ride(rideId):
    if request.method == "GET":
        pass
        # TODO : Get details of given rideId
    elif request.method == "POST":
        username = request.get_json(force=True)["username"]
        # TODO : Join an existing ride
    elif request.method == "DELETE":
        pass
        # TODO : Delete a given rideId


@app.route('/api/v1/db/write', methods=["POST"])
def write_to_db():
    request_data = request.get_json(force=True)

    if 'delete' in request_data:
        try:
            delete = request_data['delete']
            column = request_data['column']
            collection = request_data['table']
        except:
            return Response(status=400)

        try:
            query = {column: delete}
            collection = db[collection]
            collection.delete_one(query)
            return Response(status=200)
        except:
            return Response(status=400)

    try:
        insert = request_data['insert']
        columns = request_data['columns']
        collection = request_data['table']
    except:
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
    except:
        return Response(status=400)

    filter = {}
    for i in columns:
        filter[i] = 1

    try:
        collection = db[table]
        if where != '':
            where = where.split("=")
            result = collection.find_one({where[0]: where[1]}, filter)
        else:
            result = collection.find_one({}, filter)

        return jsonify(result)
    except:
        return Response(status=400)


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
    db = client["rideshare"]
    app.run(debug=True)
