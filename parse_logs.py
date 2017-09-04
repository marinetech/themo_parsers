import os
import glob
import time
from pymongo import MongoClient
from bson import Binary, Code

#from parsers import *
from dbutils import *

from parsers_lib.dcs import *
from parsers_lib.mp101a import *
from parsers_lib.vaisala import *
from parsers_lib.windsonic import *
from parsers_lib.microcat import *
from parsers_lib.metpak import *
from parsers_lib.flntu import *
from parsers_lib.s9 import *
from parsers_lib.microstrain import *
from parsers_lib.ad2cp import *




#---------- global variables -----------#
parse_info = [
                # "dir with buoy logs", "where to archive processed logs", "which buoy is associated to that location, "where to write the log for this script"
                ("/home/ilan/sea", "/home/ilan/sea/archive", "tabs225m09", "/home/ilan/sea/logs"),
                ("/home/tabs225m09", "/mnt/themo/tabs225m09_archive", "tabs225m09", "/mnt/themo/logs"),
                ("/home/tabs225m10", "/mnt/themo/tabs225m10_archive", "tabs225m10", "/mnt/themo/logs")
             ]


#-------- functions ------------#

def get_sensor_id(sensor_name):
    if (sensor_name in dict_sensors):
        return str(dict_sensors[sensor_name])
    else:
        return None


def extract_compressed_logs(plog):
    if not os.path.exists(archive_dir):
        print_log("mkdir " + archive_dir, plog)
        os.mkdir(archive_dir)

    for log in glob.glob(buoy_logs_dir + "/*.7z"):
        archived_log = archive_dir + "/" + os.path.basename(log)
        print_log("extracting " + os.path.basename(log), plog)
        os.system( '7z x ' + log + ' -aoa -o' + buoy_logs_dir )
        os.rename(log, archived_log)


def identify_and_route_to_parser(plog):
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
    dict_log_types["microstrain_gx3-25-averaged"] = "waves"
    # dict_log_types["ad2cp-averaged"] = "adcp"

    #dict_log_types["eplab-pyranometer-spp-averaged"] = "spp"
    #dict_log_types["eplab-radiometer-spp-averaged"] = "pir"



    #every file in our log dir
    for log in glob.glob(buoy_logs_dir + "/*.txt"):
        flag_was_parsed = False
        log_base_name = os.path.basename(log)

        #match file name with each of the keys in dict_log_types
        for key in dict_log_types.keys():
            if log_base_name.startswith(key):
                print_log("\n-I- parsing " + log, plog)
                #print("parsing " + os.path.basename(log))
                sensor_name = dict_log_types[key]
                route_to_parser(log, sensor_name, plog)
                flag_was_parsed = True
        #break

        if not flag_was_parsed:
            if "averaged" in log_base_name:
                print_log("-W- log was ignored: " + log_base_name, plog)


def route_to_parser(log, sensor_name, plog):
    parser = sensor_name + "_parser"
    sensor_id = get_sensor_id(sensor_name)
    if sensor_id:
        #try:
            json_data = globals()[parser](log, sensor_name, sensor_id)
            print_log("\n\n---{}---\n".format(parser), plog, "")
            if json_data != None:
                for document in json_data:
                    print(document)
                    insert_samples(document)
                    print()
        # except:
        #     print()
        #     print("-E- " +  parser + " is missing?")
        #     print()
        #     exit(1)
    else:
        print_log("", plog, "")
        print_log("-E- unknown sensor: " + sensor_name, plog, "-E-")
        exit(1)



#-----------------------------------------Main Body--------------------------------------------------#


init_db()
for tpl in parse_info:
    buoy_logs_dir = tpl[0]
    archive_dir = tpl[1]
    buoy = tpl[2]
    log_dir = tpl[3]
    log = log_dir + "/" + buoy + '_' + now()

    if os.path.isdir(buoy_logs_dir):
        init_log(log)
        print_log("inspecting: " + buoy_logs_dir, log)
        init_buoy(buoy)
        extract_compressed_logs(log)
        identify_and_route_to_parser(log)
