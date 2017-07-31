import os
import glob
from pymongo import MongoClient
from bson import Binary, Code

from parsers import *
from dbutils import *



#---------- global variables -----------#

#dict_sensors = {}
logs_dir = "/home/ilan/Downloads/tabs225m09_sea"
archive_dir = logs_dir + "/archive"



#-------- helpers ------------#

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
    dict_log_types["eplab-pyranometer-spp-averaged"] = "spp"
    dict_log_types["eplab-radiometer-spp-averaged"] = "pir"
    dict_log_types["external_temperature_humidity_MP101A-HUMIDITY-averaged"] = "mp101a"
    dict_log_types["external_temperature_humidity_MP101A-TEMPERATURE-averaged"] = "mp101a"
    dict_log_types["microcat-averaged"] = "microcat"
    dict_log_types["vaisala-ptb-210-barometer-averaged"] = "barometer"
    dict_log_types["windsonic-averaged"] = "windsonic"

    # the following are all the logs that should not be parsed, or we don't know how to parse at this point
    dict_ignore = {}
    dict_ignore["adcp_voltage-averaged"] = ""
    dict_ignore["compass-averaged"] = ""
    dict_ignore["humidity_internal-averaged"] = ""
    dict_ignore["gps_time-averaged"] = ""
    dict_ignore["eb505gps-averaged"] = ""
    dict_ignore["sound_nine_ultimodem-averaged"] = "" # unclear!!
    dict_ignore["microstrain_gx3-25-averaged"] = ""  # unclear!!
    dict_ignore["ad2cp-averaged"] = ""  # BINARY!!
    dict_ignore["ad2cp-telemetry"] = ""  # unclear !!


    #every file in our log dir
    for log in glob.glob(logs_dir + "/*.txt"):
        #print(log)
        print("parsing " + os.path.basename(log))
        flag_continue = False
        log_base_name = os.path.basename(log)

        #some logs will be ignored for now
        for key in dict_ignore.keys():
            if log_base_name.startswith(key):
                flag_continue = True
        if flag_continue:
            continue

        #match file name with each of the keys in dict_log_types
        for key in dict_log_types.keys():
            if log_base_name.startswith(key):
                sensor_name = dict_log_types[key]
                route_to_parser(log, sensor_name)
        #break


def route_to_parser(log, sensor_name):
    parser = sensor_name + "_parser"
    sensor_id = get_sensor_id(sensor_name)
    if sensor_id:
        #try:
            json_data = globals()[parser](log, sensor_name, sensor_id)
            print("\n\n---{}---\n".format(parser))
            for document in json_data:
                insert_samples(document)
                print(document)
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
