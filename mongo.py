import pymongo

client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
db = client["rideshare"]
users = db["users"]
#
# print(users.find_one({"_id": "iOS"}, {"_id": 1}))

for i in users.find():
    print(i)

# x = users.delete_many({})