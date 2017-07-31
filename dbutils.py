from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import loads

dict_sensors = {}


def init_db():
    global client; client = MongoClient()
    global db; db = client.themo

    for buoy in db.buoys.find():
        global buoy_id; buoy_id = str(buoy["_id"])
        global buoy_name; buoy_name = buoy["name"]
        print(buoy_name)
        print(buoy_id)

        filter = {}
        filter["buoy_name"] = buoy_name
        for sensor in db.sensors.find(filter):
            dict_sensors[sensor["name"]] = sensor["_id"]


def insert_samples(document):
    db.samples.insert_one(loads(document))
