from pymongo import MongoClient
from bson import Binary, Code
from bson.objectid import ObjectId
from bson.json_util import loads

dict_sensors = {}
buoy_id = ""


def init_db():
    global client; client = MongoClient()
    global db; db = client.themo


def init_buoy(buoy_name):
    filter = {}
    filter["name"] = buoy_name
    global buoy_id; buoy_id = db.buoys.find_one(filter)["_id"]

    dict_sensors.clear()
    filter = {}
    filter["buoy_name"] = buoy_name
    for sensor in db.sensors.find(filter):
        dict_sensors[sensor["name"]] = sensor["_id"]


def insert_samples(document):
    db.samples.insert_one(loads(document))


def get_s9_sensors():
    filter = {'name':'s9'}
    s9 = db.sensors.find(filter)
    return s9[0]["child_sensors"]


# return caliibration-related constants for FLNTU
def get_callibration_values(sensor_id):
    filter = {"_id": ObjectId(sensor_id)}
    flntu = db.sensors.find(filter)

    # no error checking, we'll have to add some
    ret = {}
    ret["chl_dark_count"] = flntu[0]["chl_dark_count"]
    ret["chl_sf"] = flntu[0]["chl_sf"]
    ret["ntu_dark_count"] = flntu[0]["ntu_dark_count"]
    ret["ntu_sf"] = flntu[0]["ntu_sf"]

    return ret

def find_subscribers(subscription):
    filter = {"subscription": subscription}
    return db.alarms.find(filter)
