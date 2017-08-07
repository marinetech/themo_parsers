from utils import *
import json
from bson.json_util import dumps
import io


dict_s9_map = {}



def s9_parser(s9_log, sensor_name, sensor_id):
   data = {}
   json_data = []

   data["sensor_name"] = sensor_name
   data["sensor_id"] = sensor_id
   data["source"] = s9_log

   date_time = extract_date_time_from_file_name(s9_log)

   d_stamp = date_time[0]
   data["d_stamp"] = d_stamp

   t_stamp = date_time[1]
   data["t_stamp"] = format_time(t_stamp)


   fo = io.open(s9_log,'r',encoding='utf-8',errors='ignore')
   for line in fo:
        if line.startswith("AT:"):
            print(line)




        # rest_of_the_line = line.split(']')[1].strip()
        # fields = rest_of_the_line.split(' ')[1:]
        # i = 0
        # idx = 1 #will enumarate MAGNITUDE fields
        # while (i < len(fields)):
        #     #This log contains more than one "MAGNITUDE" :(
        #     if fields[i].strip() == "MAGNITUDE":
        #         fields[i] = "MAGNITUDE_" + str(idx)
        #         idx += 1
        #     data[fields[i].lower().strip().replace(' ', '_')] = fields[i+1].strip()
        #     i += 2
        # #json_data.append(json.dumps(data))
        # json_data.append(dumps(data))
   fo.close()
   #return json_data
   return None


s9_parser("/home/ilan/Downloads/tabs225m09_sea/sound_nine_ultimodem-averaged-tabs225m09-201707251300.txt", "s9", 456)
