from utils import *
import json
from bson.json_util import dumps

def windsonic_parser(windsonic_log, sensor_name, sensor_id):
   data = {}
   json_data = []
   fo = open(windsonic_log)
   for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = windsonic_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(' ')[1:]
        i = 0
        idx = 1 #will enumarate MAGNITUDE fields
        while (i < len(fields)):
            #This log contains more than one "MAGNITUDE" :(
            if fields[i].strip() == "MAGNITUDE":
                fields[i] = "MAGNITUDE_" + str(idx)
                idx += 1
            try:
                data[fields[i].lower().strip().replace(' ', '_')] = float(fields[i+1].strip())
                i += 2
            except:
                continue
        #json_data.append(json.dumps(data))
        json_data.append(dumps(data))
   fo.close()
   return json_data
