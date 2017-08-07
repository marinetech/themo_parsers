from utils import *
import json
from bson.json_util import dumps


def flntu_parser(flntu_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["field1", "field2", "field3", "field4", "field5"]
    fo = open(flntu_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = flntu_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        fields = line.split(']')[1].strip().split()
        
        i = 3
        while (i < len(fields)):
            data[headers[i-3]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data
