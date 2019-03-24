from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import time
import datetime
import struct

flntu_records = []
dict_mpp_exit_codes = {}


def mpp_e_parser(mpp_e_log, sensor_name, sensor_id):
    init_err_codes()

    data = {}
    json_data = []

    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = mpp_e_log

    e_file = open(mpp_e_log, "rb")

    header = e_file.read(16)
    sensor_start_time = convert_unix_time(e_file.read(4).hex())
    profle_start_time = convert_unix_time(e_file.read(4).hex())

    # The next four bytes in the file will either be a time stamp that indicates
    # a data record is coming up, or it will be a profile message code which indicates
    # that we have reached the end of the profile's data records.

    # If these four bytes are not one of these profile message codes,
    # then it is a data record time stamp which will be followed by
    # a variable number of data structures (18 bytes each)
    while(True):
        next_record = e_file.read(4).hex()
        if is_data_record(next_record):
            next_record_full = next_record + e_file.read(18).hex()
            parse_data_record(next_record_full)
        else:
            break

    ramp_exit = dict_mpp_exit_codes[int(e_file.read(2).hex(),16)]
    profile_exit = dict_mpp_exit_codes[int(e_file.read(2).hex(),16)]
    vehicle_motion_stopped = convert_unix_time(e_file.read(4).hex())
    sensor_logging_stopped = convert_unix_time(e_file.read(4).hex())

    for r in flntu_records:
        # data["header"] = header.hex()
        data["sensor_start_time"] = sensor_start_time
        data["profle_start_time"] = profle_start_time
        data["ramp_exit"] = ramp_exit
        data["profile_exit"] = profile_exit
        data["vehicle_motion_stopped"] = vehicle_motion_stopped
        data["sensor_logging_stopped"] = sensor_logging_stopped
        data["d_stamp"] = r[0]
        data["t_stamp"] = r[1]
        data["motor_current"] = r[2]
        data["battery_voltage"] = r[3]
        data["pressure"] = r[4]
        data["chl"] = r[5]
        data["bb"] = r[6]
        data["cdom"] = r[7]

        if  __name__ == "__main__":
            print(data)

        json_data.append(json.dumps(data))

    # -*- coding: utf-8 -*-
    e_file.close()
    return json_data


def parse_data_record(rec):
    record = []

    d_stamp = str(convert_unix_time(rec[0:8])).split(' ')[0]
    t_stamp = str(convert_unix_time(rec[0:8])).split(' ')[1]

    motor_current = struct.unpack('!f', bytes.fromhex(rec[8:16]))[0]
    motor_current = round(motor_current,2)

    battery_voltage = struct.unpack('!f', bytes.fromhex(rec[16:24]))[0]
    battery_voltage = round(battery_voltage,2)

    pressure = struct.unpack('!f', bytes.fromhex(rec[24:32]))[0]
    pressure = round(pressure,2)

    chl = int(rec[32:36],16) # Hex string => Int
    bb = int(rec[36:40],16) # Hex string => Int
    cdom = int(rec[40:],16) # Hex string => Int

    record.append(d_stamp)
    record.append(t_stamp)
    record.append(motor_current)
    record.append(battery_voltage)
    record.append(pressure)
    record.append(chl)
    record.append(bb)
    record.append(cdom)
    flntu_records.append(record)


def init_err_codes():
    dict_mpp_exit_codes[0]  = "SMOOTH_RUNNING"
    dict_mpp_exit_codes[1]  = "MISSION_COMPLETE"
    dict_mpp_exit_codes[2]  = "OPERATOR_CTRL_C"
    dict_mpp_exit_codes[3]  = "TT8_COMM_FAILURE"
    dict_mpp_exit_codes[4]  = "CTD_COMM_FAILURE"
    dict_mpp_exit_codes[5]  = "ACM_COMM_FAILURE"
    dict_mpp_exit_codes[6]  = "TIMER_EXPIRED"
    dict_mpp_exit_codes[7]  = "MIN_BATTERY"
    dict_mpp_exit_codes[8]  = "AVG_MOTOR_CURRENT"
    dict_mpp_exit_codes[9]  = "MAX_MOTOR_CURRENT"
    dict_mpp_exit_codes[10] = "SINGLE_PRESSURE"
    dict_mpp_exit_codes[11] = "AVG_PRESSURE"
    dict_mpp_exit_codes[12] = "AVG_TEMPERATURE"
    dict_mpp_exit_codes[13] = "TOP_PRESSURE"
    dict_mpp_exit_codes[14] = "BOTTOM_PRESSURE"
    dict_mpp_exit_codes[15] = "PRESSURE_RATE_ZERO"
    dict_mpp_exit_codes[16] = "STOP_NULL"
    dict_mpp_exit_codes[17] = "FLASH_CARD_FULL"
    dict_mpp_exit_codes[18] = "FILE_SYSTEM_FULL"
    dict_mpp_exit_codes[19] = "TOO_MANY_OPEN_FILES"
    dict_mpp_exit_codes[20] = "AANDERAA_COMM_FAILURE"
    dict_mpp_exit_codes[21] = "STATIONARY_EXPIRED"
    dict_mpp_exit_codes[22] = "DOCK_PROXIMITY"


def is_data_record(next_record):
     if next_record == '':
         return False

     message_codes = [0xFFFFFFFF, 0xFFFFFFFE, 0xFFFFFFFD, 0xFFFFFFFC ,0xFFFFFFFB ,0xFFFFFFFA]
     if int(next_record, 16) in message_codes:
        return False
     else:
        return True


def convert_unix_time(unix_time):
    int_unix_time = int(unix_time,16) # Hex string => Int
    return datetime.datetime.utcfromtimestamp(int_unix_time).strftime('%Y-%m-%d %H:%M:%S')
    # return time.ctime(int_unix_time))


if  __name__ == "__main__":
    mpp_e_log = "/home/ilan/Desktop/MMP/E0000006.DAT"
    mpp_e_parser(mpp_e_log, "mpp_e", "99999999991")
