from flask import Flask, request, Response
import pymongo
import requests
import re

app = Flask(__name__)


@app.route('/api/v1/users', methods=["PUT"])
def add_user():
    if request.method != "PUT":
        return Response(status=405)

    username = request.get_json(force=True)["username"]
    password = request.get_json(force=True)["password"]

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

    # TODO : Remove user from database


@app.route('/api/v1/rides', methods=["POST"])
def create_ride():
    created_by = request.get_json(force=True)['created_by']
    time_stamp = request.get_json(force=True)['timestamp']
    source = request.get_json(force=True)['source']
    destination = request.get_json(force=True)['destination']
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
        pass
        # TODO : Join an existing ride
    elif request.method == "DELETE":
        pass
        # TODO : Delete a given rideId


@app.route('/api/v1/db/write', methods=["POST"])
def write_to_db():
    insert = request.get_json(force=True)['insert']
    columns = request.get_json(force=True)['columns']
    collection = request.get_json(force=True)['table']

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
    table = request.get_json(force=True)['table']
    columns = request.get_json(force=True)['columns']
    where = request.get_json(force=True)['where']

    # TODO : Read from table


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
    db = client["rideshare"]
    app.run(debug=True)
