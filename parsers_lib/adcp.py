from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import io

def adcp_parser(ad2cp_log, sensor_name, sensor_id):

    json_data = []
    data = {}
    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = ad2cp_log

    date_time = extract_date_time_from_file_name(ad2cp_log)

    d_stamp = date_time[0]
    data["d_stamp"] = d_stamp

    t_stamp = date_time[1]
    data["t_stamp"] = format_time(t_stamp)


    fo = io.open(ad2cp_log,'r',encoding='utf-8',errors='ignore')

    # extracting the cell-size is a must. If faild there is no point to parse the data
    cell_size = get_cell_size(fo)
    if cell_size is None:
        return None
    try:
        data["cell_size"] = float(cell_size)
    except:
        return None

    #now with a cell_size in hand(hopefully) we can parse PNORC lines
    for line in fo:
        if line.startswith("$PNORC"):
            try:
                pnorc_arr = line.split(",")
                data["cell_number"] = int(pnorc_arr[3])
                data["depth[m]"] = data["cell_number"] * data["cell_size"] + 1 #1 is the installation depath - currently hard-coded :(
                data["speed[m/s]"] = float(pnorc_arr[8])
                data["direction[deg]"] = float(pnorc_arr[9])
                json_data.append(json.dumps(data))
            except:
                print("-E- failed to parse line: " + line)



    # json_data.append(json.dumps(data))
    fo.close()
    return json_data


def get_cell_size(fo):
    for line in fo:
        if line.startswith("$PNORI"):
            try:
                return(line.split(",")[6])
            except:
                print("-E- failed to parse line: " + line)
                return None
