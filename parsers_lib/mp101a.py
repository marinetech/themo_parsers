from utils import *
import json
from bson.json_util import dumps

def mp101a_humidity_parser(humidity_log, sensor_name, sensor_id):
   data = {}
   json_data = []
   fo = open(humidity_log)
   for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = humidity_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(',')
        i = 0
        while (i < len(fields)):
            try:
                data[fields[i].strip().replace(' ', '_')] = float(fields[i+1].strip())
            except:
                return None
            i += 2
        #json_data.append(json.dumps(data))
        json_data.append(dumps(data))
   fo.close()
   return json_data


def mp101a_temprature_parser(temprature_log, sensor_name, sensor_id):
   return mp101a_humidity_parser(temprature_log, sensor_name, sensor_id)
