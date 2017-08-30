from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import io

def adcp_parser(ad2cp_log, sensor_name, sensor_id):

    json_data = []
    data = {}
    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = ad2cp_log

    date_time = extract_date_time_from_file_name(ad2cp_log)

    d_stamp = date_time[0]
    data["d_stamp"] = d_stamp

    t_stamp = date_time[1]
    data["t_stamp"] = format_time(t_stamp)


    fo = io.open(ad2cp_log,'r',encoding='utf-8',errors='ignore')
    for line in fo:
        print(line)

    exit()
