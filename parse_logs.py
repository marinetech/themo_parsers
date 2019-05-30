import os
import sys
import shutil
import glob
import time
from pymongo import MongoClient
from bson import Binary, Code
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
from parsers_lib.adcp import *
from parsers_lib.battery import *
from parsers_lib.current import *
from parsers_lib.system_volt import *
from parsers_lib.disk_space import *
from parsers_lib.adcp_wh import *
from parsers_lib.turner_c3 import *
from parsers_lib.rad import *
from parsers_lib.mmp_c import *
from parsers_lib.mmp_e import *



#---------- global variables -----------#
parse_info = [
                # "dir with buoy logs", "where to archive processed logs", "which buoy is associated to that location, "where to write the log for this script"
                #("/home/ilan/Desktop/tabsbuoy09", "/home/ilan/Desktop/tabsbuoy09/archive", "tabs225m09", "/home/ilan/Desktop/tabsbuoy09/logs"),
                ("/home/tabs225m09", "/mnt/themo/tabs225m09_archive", "tabs225m09", "/mnt/themo/logs"),
                #("/home/tabs225m10", "/mnt/themo/tabs225m10_archive", "tabs225m09", "/mnt/themo/logs"),
                ("/home/tabs225m11", "/mnt/themo/tabs225m11_archive", "tabs225m11", "/mnt/themo/logs")
                #("/home/ilan/sea_", "/home/ilan/sea/tabs225m10_archive", "tabs225m09", "/home/ilan/sea/logs"),
                #("/home/ilan/Desktop/tabs225m11", "/home/ilan/Desktop/tabs225m11/tabs225m11_archive", "tabs225m11", "/home/ilan/Desktop/tabs225m11/logs")
             ]

debug_mode = False


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

    for zip in glob.glob(buoy_logs_dir + "/*.7z"):
        archived_log = archive_dir + "/" + os.path.basename(zip)
        print_log("extracting " + os.path.basename(zip), plog)
        os.system( '7z x ' + zip + ' -aoa -o' + buoy_logs_dir )
        #os.rename(log, archived_log)
        try:
            shutil.move(zip, archived_log)
        except:
            print_log("failed to move zip" + zip, plog, "-E-")


def identify_and_route_to_parser(plog, buoy):
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
    dict_log_types["microstrain_gx5-25-averaged"] = "waves"
    dict_log_types["ad2cp-telemetry"] = "adcp"
    dict_log_types["battery_voltage1-averaged"] = "battery"
    dict_log_types["battery_voltage2-averaged"] = "battery"
    dict_log_types["battery_voltage3-averaged"] = "battery"
    dict_log_types["charge_current1-averaged"] = "current"
    dict_log_types["charge_current2-averaged"] = "current"
    dict_log_types["charge_current3-averaged"] = "current"
    dict_log_types["system_voltage-averaged"] = "system_volt"
    dict_log_types["disk_space-log"] = "disk_space"
    dict_log_types["adcp-averaged-tabs225m11"] = "adcp_wh"
    dict_log_types["turner_c3-averaged"] = "turner_c3"
    dict_log_types["rad_pir_spp-averaged"] = "rad"
    dict_log_types["mmp-C`"] = "mmp_c"
    dict_log_types["mmp-E"] = "mmp_e"



    #dict_log_types["eplab-pyranometer-spp-averaged"] = "spp"
    #dict_log_types["eplab-radiometer-spp-averaged"] = "pir"



    #every file in our log dir
    for log in glob.glob(buoy_logs_dir + "/*.txt"):
        flag_was_parsed = False
        log_base_name = os.path.basename(log)

        #match file name with each of the keys in dict_log_types
        for key in dict_log_types.keys():
            if log_base_name.startswith(key):
                print_log("\nparsing " + log, plog)
                #print("parsing " + os.path.basename(log))
                sensor_name = dict_log_types[key]
                route_to_parser(log, sensor_name, plog, buoy)
                flag_was_parsed = True
        #break

        if not flag_was_parsed:
            if "averaged" in log_base_name:
                print_log("log was ignored: " + log_base_name, plog, "-W-")
                try:
                    if not debug_mode:
                        os.remove(log)
                except:
                    print_log("failed to remove: " + log, plog, "-E-")



def route_to_parser(log, sensor_name, plog, buoy):
    parser = sensor_name + "_parser"
    sensor_id = get_sensor_id(sensor_name)
    if sensor_id:
        json_data = globals()[parser](log, sensor_name, sensor_id)
        print_log("\n\n---{}---\n".format(parser), plog, "")
        # if not debug_mode:
        #     os.remove(log)
        if json_data != None:
            for document in json_data:
                insert_samples(document, buoy)                
                trigger_alert(json.loads(document)) #json to py dictionary
        else:
            print_log("-W- empty json")
        # except:
        #     print_log("-E- " +  "error during insert")
        #     exit(1)
    else:
        print_log("", plog, "")
        print_log("-E- unknown sensor: " + sensor_name, plog, "-E-")
        exit(1)



#-----------------------------------------Main Body--------------------------------------------------#

# debug mode
if len(sys.argv) > 1:
    if sys.argv[1] == "-debug":
        debug_mode = True

init_db()
for tpl in parse_info:
    print("-I- processing: " + str(tpl) + "\n")
    buoy_logs_dir = tpl[0]
    if debug_mode:
        print("-D- buoy_logs_dir: " + buoy_logs_dir)
    archive_dir = tpl[1]
    buoy = tpl[2]
    plog_dir = tpl[3]
    plog = plog_dir + "/" + buoy + '_' + now()

    if os.path.isdir(buoy_logs_dir):
        try:
            init_log(plog)
            # print_log("inspecting: " + buoy_logs_dir, plog)
            init_buoy(buoy)
            extract_compressed_logs(plog)
            identify_and_route_to_parser(plog, buoy)
        except Exception as ex:
            print("-E- " + ex)
            continue
    else:
        print("-I- no such dir: " + buoy_logs_dir + "\n")
