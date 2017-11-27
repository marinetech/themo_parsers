from utils import *
import json
from bson.json_util import dumps



def waves_parser(microstrain_log, sensor_name, sensor_id):
    data = {}
    json_data = []

    #headers = ["Temprature", "Conductivity", "Salinity"]
    fo = open(microstrain_log)
    for line in fo:

        arr_line = ' '.join(line.split()).split() # this strange split deals with unknown number of white-spaces between the fields

        if arr_line[2:4]  == ['MicroStrain_PROCESSOR', 'Significant']: # we are looking for ['MicroStrain_PROCESSOR', 'Significant']
            #e.g. [2017-08-02 16:43:15.125] MicroStrain_PROCESSOR Significant Height:  0.512406  Mean Period:  3.933000  Dominant Period:  6.250000
            print(line)

            data["sensor_name"] = sensor_name
            data["sensor_id"] = sensor_id
            data["source"] = microstrain_log

            d_stamp = clean_str(arr_line[0])
            data["d_stamp"] = d_stamp

            t_stamp = clean_str(arr_line[1])
            data["t_stamp"] = format_time(t_stamp)

            try:
                data["significant_height"] = float(arr_line[5])
                data["mean_period"] = float(arr_line[8])
                data["dominant_period"] = float(arr_line[11])
            except:
                continue

            json_data.append(json.dumps(data))
            break
    fo.close()
    return json_data
