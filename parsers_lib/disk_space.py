from utils import *
from dbutils import *
import json
from bson.json_util import dumps


def disk_space_parser(disk_space_log, sensor_name, sensor_id):
    # [2017-08-10 04:03:08.069] battery_voltage1 AvgVal, 3808.600098, AvgVolt, 4.299145, AvgLinearAdjVal, 12.897436
    data = {}
    json_data = []

    fo = open(disk_space_log)
    for line in fo:
        if "Determined %" in line:
            data["sensor_name"] = sensor_name
            data["sensor_id"] = sensor_id
            data["source"] = disk_space_log

            d_stamp = get_d_stamp(line)
            data["d_stamp"] = d_stamp

            t_stamp = clean_str(line.split(' ')[4])
            data["t_stamp"] = format_time(t_stamp)

            disk_space = clean_str(line.split(' ')[-1]).strip() + "%"
            data["free_disk_space"] = disk_space


    if  __name__ == "__main__":
        print(data)

    json_data.append(json.dumps(data))
    fo.close()
    return json_data


def get_d_stamp(line):
    arr_line = line.split(' ')
    year = arr_line[6]
    month = arr_line[2]
    day = arr_line[3]
    if len(day) < 2:
        day = "0" + day
    str2convert = year + "-" + month + "-" + day
    ret = str(datetime.strptime(str2convert, "%Y-%b-%d"))
    return ret.split(' ')[0]


if  __name__ == "__main__":
    init_db()
    disk_space_log = "/home/ilan/Desktop/tabsbuoy09/disk_space-log-tabs225m09-201811050949.txt"
    sensor_id = "59b0ce8b57e69a661ad6eed2"
    data = disk_space_parser(disk_space_log, "disk_space", sensor_id)
