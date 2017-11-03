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

        try:
            d_stamp = clean_str(line.split(' ')[0])
            data["d_stamp"] = d_stamp

            t_stamp = clean_str(line.split(' ')[1])
            data["t_stamp"] = format_time(t_stamp)

            rest_of_the_line = line.split(']')[1].strip()
            fields = rest_of_the_line.split(',')

            data["wind_direction"] = get_winddirection(get_compass_file_name(metpak_log))

            i = 2
            while (i < len(fields)):
                data[headers[i]] = fields[i].strip()
                i += 1
            json_data.append(json.dumps(data))
        except:
            print("-E- failes to parse: " + line)

    fo.close()
    return json_data

def get_compass_file_name(metpak_log):
    dirname = os.path.dirname(metpak_log)
    filename = os.path.basename(metpak_log)
    metpak_timestamp = filename.split('-')[-1].split(".")[0]
    metpak_buoyname = filename.split('-')[2]
    metpak_compass = "metpak-" + metpak_buoyname + "-compasswind_metpak-" + metpak_timestamp + ".txt"
    return dirname + "/" + metpak_compass


def get_winddirection(compassfile):
    if not os.path.isfile(compassfile):
        return ""

    fo = open(compassfile)
    for line in fo:
        direction = line.split(' ')[4]

    return direction


#For debug purposes only
if __name__ == "__main__":
    example = "/home/ilan/sea/metpak-averaged-tabs225m09-201707301630.txt"
    print(metpak_parser(example, "metpak", 45))
