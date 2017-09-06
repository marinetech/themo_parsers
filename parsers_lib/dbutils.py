from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import loads
import json

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


def get_s9_sensors():
    init_db()
    #s9_dict = {}
    filter = {}
    filter["name"] = "s9"
    return db.sensors.find(filter)[0]["child_sensors"]
    # s9 = db.sensors.find(filter)
    # child_sensors = s9[0]["child_sensors"]
    # for i in child_sensors:
    #     #print(i)
    #     s9_dict.update(i)
    # print(s9_dict)
