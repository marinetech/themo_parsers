from utils import *
from dbutils import *
import json
from bson.json_util import dumps


def battery_parser(battery_log, sensor_name, sensor_id):
    # [2017-08-10 04:03:08.069] battery_voltage1 AvgVal, 3808.600098, AvgVolt, 4.299145, AvgLinearAdjVal, 12.897436
    data = {}
    json_data = []

    fo = open(battery_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = battery_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        battery_id = line.split(' ')[2]
        battery_id = battery_id[-1]
        data["battery_id"] = battery_id

        level = battery_id = line.split(' ')[-1].strip('\n')
        data["level"] = float(level)

        if  __name__ == "__main__":
            print(data)

        json_data.append(json.dumps(data))
    fo.close()
    return json_data


if  __name__ == "__main__":
    init_db()
    battery_log = "/home/ilan/sea/battery_voltage1-averaged-tabs225m09-201708100403.txt"
    sensor_id = "59b0ce8b57e69a661ad6eed2"
    data = battery_parser(battery_log, "battery", sensor_id)
