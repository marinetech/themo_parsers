from utils import *
from dbutils import *
import json
from bson.json_util import dumps



def turner_c3_parser(turner_log, sensor_name, sensor_id):
    data = {}
    json_data = []

    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = turner_log

    skip_line = True
    fo = open(turner_log)
    for line in fo:
        if skip_line: #skip headers
            skip_line = False
            continue

        arr_line = line.split()
        data['d_stamp'] = format_date(arr_line[0])
        data['t_stamp'] = arr_line[1]
        data['chlorophyl'] = arr_line[2]
        data['turbidity'] = arr_line[4]
        data['temperature'] = arr_line[6]
        # break #one line is good enough

        if  __name__ == "__main__":
             print(data)

        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def format_date(c3_date):
    # the date in these logs is like:  3/07/19
    # we want to convert it to: 2019-03-07
    arr_date = c3_date.split('/')
    if len(arr_date[0]) < 2:
        arr_date[0] = '0' + arr_date[0] #ensure 2-digit month
    if len(arr_date[1]) < 2:
        arr_date[1] = '0' + arr_date[1] #ensure 2-digit day
    return '20' + arr_date[2] + '-' + arr_date[0] + '-' + arr_date[1]




if  __name__ == "__main__":
    init_db()
    turner_log = "/home/ilan/Desktop/tabs225m11/turner_c3-averaged-tabs225m11-201903071430.txt"
    sensor_id = "59b0ce8b57e69a661ad6eed2"
    data = turner_c3_parser(turner_log, "turner_c3", sensor_id)
