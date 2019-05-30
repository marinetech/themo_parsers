from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import io
import os

# Globals
S9_LOG = None

def get_dept(s9_id):
    print("calculating depth for " + s9_id)
    if not s9_id in s9_dict:
        return 0

    refernce_depth = s9_dict('A00W')



def s9_parser(s9_log, sensor_name, sensor_id):

   data = {}
   json_data = []
   global s9_dict; s9_dict = {}
   for s in get_s9_sensors():
       s9_dict.update(s)

   s9_samples_dict = {}


   data["sensor_name"] = sensor_name
   data["sensor_id"] = sensor_id
   data["source"] = s9_log

   date_time = extract_date_time_from_file_name(s9_log)

   d_stamp = date_time[0]
   data["d_stamp"] = d_stamp

   t_stamp = date_time[1]
   data["t_stamp"] = format_time(t_stamp)

   init_debug_log(d_stamp+t_stamp)
   global S9_LOG


   fo = io.open(s9_log,'r',encoding='utf-8',errors='ignore')
   for line in fo:

        if line.startswith("AT:") or line.startswith("APT:"):

            S9_LOG.write("parsing " + line)

            #get rid of the checksun at the end e.g. *13*44402,32*
            line = line.split("*")[0] #get rid of the checksun at the end e.g. *13*44402,32*

            # ignore ileagal lines...
            if len(line.split(',')) < 11:
                S9_LOG.write("< 11 tokens - continue \n")
                continue

            # print()
            # print(line)

            if (line.split(":")[1].split(",")[1]) == "ATPES":
                line = line.split(":")[1].split(",")
                S9_LOG.write("ATPES\n")
            else: #ATE
                line = line.split(",")
                line[0] = line[0].split(":")[1] #getting rid of 'AT:' in 'AT:A016'
                S9_LOG.write("ATE\n")
                # print(line[0])
                # print(line)


            try:
                s9_id = line[0]
                if s9_id not in s9_dict:
                    S9_LOG.write("not in dict: " + s9_id + "\n")
                    if __name__ == "__main__":
                        print("-D- " + s9_id + " not in s9_dict")
                    continue #because sometimes log is corrupted and we get a wrong S9 ID


                s9_type = line[1]
                s9_temprature = line[3]
                s9_pressure = line[4]
                s9_tilt = line[5]
                s9_accel_x = line[8]
                s9_accel_y = line[9]
                s9_accel_z = line[10]
                S9_LOG.write("line was parsed successfully\n\n\n")
            except:
                print("-E- failed to parse " + str(line))
                S9_LOG.write("-E- failed to parse " + str(line) + "\n")
                continue

            # The dictionary contains sensor_id as key name e.g. 'A015'
            # It's value is an array - element0 is accumulated tempratures and element1 is number os temprature samples.
            # so when we finish, element0/element1 will give the average tempratue
            try:
                if s9_id in s9_samples_dict:
                    temprature_sum = s9_samples_dict[s9_id][0] + float(s9_temprature) #add the new temprature
                    samples_count = s9_samples_dict[s9_id][1] + 1 #increase samples count
                    s9_samples_dict[s9_id] = [temprature_sum, samples_count] #and update dictionary
                else:
                    s9_samples_dict[s9_id] = [float(s9_temprature), 1] #first sample for this sensor
            except:
                continue


   fo.close()


   s9_samples_list = []
   for s in s9_samples_dict:

       avg_temprature = round(s9_samples_dict[s][0] / s9_samples_dict[s][1], 2)
       depth = s9_dict[s]

       data["s9_id"] = s
       data["temperature"] = avg_temprature
       data["depth"] = depth
       S9_LOG.write("appending to json: " + str(data) + "\n")
       json_data.append(dumps(data))


   if len(s9_samples_dict) > 0:
       S9_LOG.write("records in json:")
       for rec in json_data:
           S9_LOG.write(str(rec) + "\n")

       if __name__ == "__main__":
           for rec in json_data:
               print(str(rec))

       return json_data
   else:
        return None



def init_debug_log(_datestr):
    global S9_LOG

    datestr = _datestr.replace("-", "").replace(":","")
    logname = "s9_" + datestr + ".log"

    if os.path.isdir("/mnt/themo/logs"):
        S9_LOG = open("/mnt/themo/logs/" + logname, "w")
    else:
        S9_LOG = open(logname, "w")



#s9_parser("/home/ilan/Downloads/tabs225m09_sea/sound_nine_ultimodem-averaged-tabs225m09-201707251300.txt", "s9", 456)




if __name__ == "__main__":
    init_db()
    s9_log = '/home/ilan/Desktop/s9_20190529/sound_nine_ultimodem-averaged-tabs225m09-201905290525.txt'
    s9_parser(s9_log, "s9", 456)
