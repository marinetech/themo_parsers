from utils import *
import json
from bson.json_util import dumps

def barometer_parser(vaisala_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    fo = open(vaisala_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = vaisala_log

        fields = line.split()

        d_stamp = clean_str(fields[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(fields[1])
        data["t_stamp"] = format_time(t_stamp)

        try:
            data[fields[2]] = float(fields[3])
        except:
            continue

        json_data.append(json.dumps(data))
    fo.close()
    return json_data
