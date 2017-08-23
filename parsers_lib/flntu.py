from utils import *
from dbutils import *
import json
from bson.json_util import dumps


def flntu_parser(flntu_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    flntu_calibration = get_callibration_values(sensor_id)

    fo = open(flntu_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = flntu_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        fields = line.split()[5:] #takes only the relavant numbers from field 5

        data["chlorophyll_concentration"] = flntu_calibration["chl_sf"] * (int(fields[1]) - flntu_calibration["chl_dark_count"])
        data["turbidity_units"] = flntu_calibration["ntu_sf"] * (int(fields[3]) - flntu_calibration["ntu_dark_count"])

        json_data.append(json.dumps(data))
    fo.close()
    return json_data
