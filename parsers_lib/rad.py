from utils import *
import json
from bson.json_util import dumps

def rad_parser(rad_log, sensor_name, sensor_id):
    data = {}
    data["longwave_irradiance"] = 0
    data["case_temperature"] = 0
    data["dome_temperature"] = 0
    data["shortwave_irradiance"] = 0
    num_of_samples = 0
    json_data = []

    fo = open(rad_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = rad_log

        if line.startswith("$WIR27"):
            fields = line.split(",")
            if len(fields) == 11:
                try:
                    data["d_stamp"] = "20" + fields[1].replace('/', '-') # e.g. 18/01/08 => 2018-01-08
                    data["t_stamp"] = fields[2]
                    data["longwave_irradiance"] += float(fields[5])
                    data["case_temperature"] += float(fields[6])
                    data["dome_temperature"] += float(fields[7])
                    data["shortwave_irradiance"] += float(fields[8])
                    num_of_samples += 1
                except:
                    return None

    # find the average
    data["longwave_irradiance"] /= num_of_samples
    data["case_temperature"] /= num_of_samples
    data["shortwave_irradiance"] /= num_of_samples

    json_data.append(json.dumps(data))
    fo.close()
    return json_data


#For debug purposes only
if __name__ == "__main__":
    example = "/home/ilan/Desktop/tabs225m11/rad_pir_spp-averaged-tabs225m11-201903100648.txt"
    print(rad_parser(example, "rad", 45))
