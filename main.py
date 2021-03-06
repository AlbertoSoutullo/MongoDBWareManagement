import json

from Client import Client
from Wagon import Wagon
from Ware import Ware

__author__ = 'Alberto Soutullo'

from pymongo import MongoClient

#Queries:

# Q1: List all wares of a client given.
client_name = 'Cafes el amanencer'
Q1 = [{'$match': {'name': client_name}}]

# Q2: List every wares with origin and destination given.
origin = "Cadiz"
destination = "Castellon"
Q2 = [{"$match": {"$and": [{"origin": origin}, {"destination": destination}]}}]

# Q3: Calculate total weight and total volume of a given client a given day.
# dia = timedate.timedate(2003, 11, 24)
day = "ISODate(2003-11-24)"
client = "Cafes el amanencer"
Q3 = [{"$match": {"name": client, "fecha": day}},
      {"$group": {"_id": {"name": "$name", "date": "$date"},
                  "totalWeight": {"$sum": "$weight"}, "totalVolume": {"$sum": "$volume"}}}]

# Q4: Calculate average density of all wares of a given client a given year.
client = "Cafes el amanencer"
year = 2001
Q4 = [{"$match": {"name": client}},
      {"$project": {"name": "$name", "weight": "$weight", "volume": "$volume", "year": {"$year": "$date"}}},
      {"$match": {"year": year}},
      {"$group": {"_id": {"name": "$name", "year": "$year"}, "density": {"$avg": {"$divide": ["$weight", "$volume"]}}}}]

# Q5: Shipping number for every month between an origin and a destination given.
origin = "Cadiz"
destination = "Castellon"
Q5 = [{"$match": {"origin": origin, "destination": destination}},
      {"$project": {"month": {"$month": "$date"}, "year": {"$year": "$date"}}},
      {"$group": {"_id": {"month": "$month", "year": "$year"}, "shipments": {"$sum": 1}}},
      {"$group": {"_id": "$_id.month", "totalShipments": {"$sum": "$shipments"}, "totalYears": {"$sum": 1}}},
      {"$project": {"_id": 0, "month": "$_id", "averageShipping": {"$divide": ["$totalShipments", "$totalYears"]}}}]

# Q6: 3 destinations which have received more shipments with their number, for an origin and year given.
origin = "Cadiz"
year = 2003
destinations_number = 3
Q6 = [{"$match": {"origin": origin}},
      {"$project": {"origin": "$origin", "destination": "$destination", "year": {"$year": "$year"}}},
      {"$match": {"year": year}},
      {"$group": {"_id": "$destination", "shipments": {"$sum": 1}}},
      {"$sort": {"shipments": -1}},
      {"$limit": destinations_number}]

# Q7: Wares within a 100km circle distance of a destination given, ordered by distance.
latitude = -16.2536378
longitude = 28.4597823
distance = 100000
Q7 = [{"$geoNear": {"near": {"type": "Point", "coordinates": [latitude,  longitude]},
                    "maxDistance": distance, "spherical": "true", "distanceField": "distance",
                    "distanceMultiplier": 0.001, "includeLocs": "geolocDestino"}},
      {"$sort": {"distance": 1}}]

# Q8: All wares within a wagon.
wagon_id = 1330
Q8 = [{"$match": {"id": wagon_id}}, {"$project": {"_id": 0, "wages": 1}}]

# Q9: All kind of wares existent in a wagon and how many of each one.
wagon_id = 1330
Q9 = [{"$match": {"id": wagon_id}}, {"$unwind": "wares"},
      {"$group": {"_id": "wares.type", "count": {"$sum": 1}}}]


def init_classes():
    Ware.init_class(db['Ware'], "templates/WareVariables.txt")
    Client.init_class(db['Client'], "templates/ClientVariables.txt")
    Wagon.init_class(db['Wagon'], "templates/WagonVariables.txt")


def fulfill_data():
    dataFile = open('templates/tesm.data', 'r')
    data = json.load(dataFile)

    wagons = []
    clients = []

    for line in data:
        cl = Client(**line['client'])
        if cl not in clients:
            clients.append(cl)
            cl.save()

        merc = Ware(**line['ware'])
        merc.update(**{"name": cl.name})
        merc.save()

        wag = Wagon(**line['wagon'])
        if wag not in wagons:
            wag.assignMercancia(merc)
            wagons.append(wag)
        else:
            for wagon in wagons:
                if wagon == wag:
                    wagon.assignMercancia(merc)

    for wagon in wagons:
        wagon.save()


def query(query):
    cursor = db.Mercancia.aggregate(query)
    while cursor.alive:
        document = cursor.next()
        print(document)


if __name__ == '__main__':

    client = MongoClient(host=['mongodb://127.0.0.1:27017'])
    db = client['TrainProject']

    init_classes()
    fulfill_data()

    query(Q3)
