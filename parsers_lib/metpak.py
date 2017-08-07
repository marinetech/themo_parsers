from utils import *
import json
from bson.json_util import dumps

def metpak_parser(metpak_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["node_letter", "wind_direction", "wind_speed", "pressure", "humidity", "temperature", "dewpoint", "supply_voltage", "status_code", "checksum"]

    fo = open(metpak_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = metpak_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(',')
        i = 0
        while (i < len(fields)):
            data[headers[i]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data
