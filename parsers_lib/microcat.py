from utils import *
import json
from bson.json_util import dumps



def microcat_parser(microcat_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["Temperature", "Conductivity", "Salinity"]
    fo = open(microcat_log)
    for line in fo:
        # e.g. [2017-07-24 22:01:14.129] MICROCAT   28.0474,  0.00004,   0.0130
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = microcat_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        fields = line.split('MICROCAT')[1].split(',')
        i = 0
        while (i < len(fields)):
            data[headers[i]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data
