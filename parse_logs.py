import os
import glob
from pymongo import MongoClient
from bson import Binary, Code

from parsers import *
from dbutils import *

from parsers_lib.dcs import *
from parsers_lib.mp101a import *
from parsers_lib.vaisala import *
from parsers_lib.windsonic import *
from parsers_lib.microcat import *
from parsers_lib.metpak import *
from parsers_lib.flntu import *
from parsers_lib.s9 import *



#---------- global variables -----------#

logs_dir = "/home/ilan/Downloads/tabs225m09_sea"
archive_dir = logs_dir + "/archive"



#-------- functions ------------#

def get_sensor_id(sensor_name):
    if (sensor_name in dict_sensors):
        return str(dict_sensors[sensor_name])
    else:
        return None


def extract_compressed_logs():
    if not os.path.exists(archive_dir):
        os.mkdir(archive_dir)

    for log in glob.glob(logs_dir + "/*.7z"):
        archived_log = archive_dir + "/" + os.path.basename(log)
        os.system( '7z x ' + log + ' -aoa -o' + logs_dir )
        os.rename(log, archived_log)


def identify_and_route_to_parser():
    # the following are all the logs that should be parsed
    dict_log_types = {}
    dict_log_types["metpak-averaged"] = "metpak"
    dict_log_types["dcs-averaged"] = "dcs"
    dict_log_types["wetlabs_flntu-averaged"] = "flntu"
    dict_log_types["external_temperature_humidity_MP101A-HUMIDITY-averaged"] = "mp101a_humidity"
    dict_log_types["external_temperature_humidity_MP101A-TEMPERATURE-averaged"] = "mp101a_temprature"
    dict_log_types["microcat-averaged"] = "microcat"
    dict_log_types["vaisala-ptb-210-barometer-averaged"] = "barometer"
    dict_log_types["windsonic-averaged"] = "windsonic"
    dict_log_types["sound_nine_ultimodem-averaged"] = "s9"

    #dict_log_types["eplab-pyranometer-spp-averaged"] = "spp"
    #dict_log_types["eplab-radiometer-spp-averaged"] = "pir"



    #every file in our log dir
    for log in glob.glob(logs_dir + "/*.txt"):
        flag_was_parsed = False
        log_base_name = os.path.basename(log)

        #match file name with each of the keys in dict_log_types
        for key in dict_log_types.keys():
            if log_base_name.startswith(key):
                print("\n-I- parsing " + log)
                #print("parsing " + os.path.basename(log))
                sensor_name = dict_log_types[key]
                route_to_parser(log, sensor_name)
                flag_was_parsed = True
        #break

        if not flag_was_parsed:
            if "averaged" in log_base_name:
                print("-W- log was ignored: " + log_base_name)


def route_to_parser(log, sensor_name):
    parser = sensor_name + "_parser"
    sensor_id = get_sensor_id(sensor_name)
    if sensor_id:
        #try:
            json_data = globals()[parser](log, sensor_name, sensor_id)
            print("\n\n---{}---\n".format(parser))
            if json_data != None:
                for document in json_data:
                    insert_samples(document)
                    print(document)
                    print()
        # except:
        #     print()
        #     print("-E- " +  parser + " is missing?")
        #     print()
        #     exit(1)
    else:
        print()
        print("-E- unknown sensor: " + sensor_name)
        exit(1)



#-----------------------------------------Main Body--------------------------------------------------#


init_db()
extract_compressed_logs()
identify_and_route_to_parser()
