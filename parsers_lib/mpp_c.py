from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import time
import datetime
import struct
from decimal import *

ctd_records = []


def mpp_c_parser(mpp_c_log, sensor_name, sensor_id):
    data = {}
    json_data = []

    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = mpp_c_log

    c_file = open(mpp_c_log, "rb")
    rec = c_file.read(11).hex()
    while int(rec, 16) != 0xFFFFFFFFFFFFFFFFFFFFFF:
        parse_data_record(rec)
        rec = c_file.read(11).hex()

    sensor_start_time = convert_unix_time(c_file.read(4).hex())
    sensor_end_time = convert_unix_time(c_file.read(4).hex())

    for r in ctd_records:
        d_stamp = clean_str(sensor_start_time.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(sensor_start_time.split(' ')[0])
        data["t_stamp"] = format_time(t_stamp)

        data["sensor_end_time"] = sensor_end_time

        data["conductivity"] = float(r[0])
        data["temperature"] = float(r[1])
        data["pressure"] = float(r[2])
        data["disolved_oxygen"] = float(r[3])

        if  __name__ == "__main__":
            print(data)

        json_data.append(json.dumps(data))

    c_file.close()
    return json_data


def parse_data_record(rec):

    record = []

    getcontext().prec = 4
    conductivity = Decimal((int(rec[0:6],16)/10000)-0.5)/1
    record.append(conductivity)

    getcontext().prec = 6
    temperature = Decimal((int(rec[6:12],16)/10000)-5)/1
    record.append(temperature)

    getcontext().prec = 3
    pressure = Decimal((int(rec[12:18],16)/100)-10)/1
    record.append(pressure)

    oxygen = int(rec[18:],16)
    record.append(oxygen)

    ctd_records.append(record)


def convert_unix_time(unix_time):
    int_unix_time = int(unix_time,16) # Hex string => Int
    return datetime.datetime.utcfromtimestamp(int_unix_time).strftime('%Y-%m-%d %H:%M:%S')
    # return time.ctime(int_unix_time))


if  __name__ == "__main__":
    mpp_c_log = "/home/ilan/Desktop/MMP/C0000006.DAT"
    mpp_c_parser(mpp_c_log, "mpp_c", "99999999999")
