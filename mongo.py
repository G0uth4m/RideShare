import pymongo

client = pymongo.MongoClient("mongodb://neutron:myindia@172.28.128.10/rideshare")
db = client["rideshare"]
users = db["users"]
rides = db["rides"]
#
# print(users.find_one({"_id": "iOS"}, {"_id": 1}))

for i in users.find():
    print(i)
#
print()



for j in rides.find():
    print(j)

print()

res = []
for k in rides.find({"source": "Sanjaynagar", "destination": "Jaynagar"}, {"rideId": 1, "created_by": 1, "timestamp": 1}):
    del k["_id"]
    res.append(k)

print(res)

# x = users.delete_many({})