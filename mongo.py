import pymongo

client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
db = client["rideshare"]
users = db["users"]
#
mydict = {"_id":"Member1", "password":"lolol"}
#
# x = users.insert_one(mydict)
# print(x.inserted_id)

for i in users.find():
    print(i)

# x = users.delete_many({})