import json
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads


#---------- global variables -----------#

dict_sensors = {}


#-------- helpers ------------#

def clean_str(str):
    ret = str
    ret = ret.replace('[', '')
    ret = ret.replace(']', '')
    ret = ret.replace('(', '')
    ret = ret.replace(')', '')
    ret = ret.replace('{', '')
    ret = ret.replace('}', '')
    return ret

def format_time(str):
    ret = str
    #ret = ret.replace(':', '-')
    ret = ret.split(".")[0]
    return ret


def get_sensor_id(sensor_name):
    if (sensor_name in dict_sensors):
        return str(dict_sensors[sensor_name])
    else:
        return None


#-----------  DB related stuff  -----------#

def init_db():
    global client; client = MongoClient()
    global db; db = client.themo

    for buoy in db.buoys.find():
        global buoy_id; buoy_id = str(buoy["_id"])
        global buoy_name; buoy_name = buoy["name"]
        print(buoy_name)
        print(buoy_id)

        filter = {}
        filter["buoy_name"] = buoy_name
        for sensor in db.sensors.find(filter):
            dict_sensors[sensor["name"]] = sensor["_id"]


def insert_samples(document):
    db.samples.insert_one(loads(document))




#-------------   PARSERS ---------------#


def adcp_parser(adcp_log, sensor_name, sensor_id):
   data = {}
   json_data = []
   fo = open(adcp_log)
   for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(',')
        i = 0
        while (i < len(fields)):
            data[fields[i].strip().replace(' ', '_')] = fields[i+1].strip()
            i += 2
        #json_data.append(json.dumps(data))
        json_data.append(dumps(data))
   fo.close()
   return json_data


def barometer_parser(vaisala_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    fo = open(vaisala_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        fields = line.split()
        d_stamp = clean_str(fields[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(fields[1])
        data["t_stamp"] = format_time(t_stamp)
        data[fields[2]] = fields[3]
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def pir_parser(radiometerr_log, sensor_name, sensor_id):
    return adcp_parser(radiometerr_log, sensor_name, sensor_id)


def spp_parser(pyranometer_log, sensor_name, sensor_id):
    return adcp_parser(pyranometer_log, sensor_name, sensor_id)


def metpak_parser(metpak_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["node_letter", "wind_direction", "wind_speed", "pressure", "humidity", "temperature", "dewpoint", "supply_voltage", "status_code", "checksum"]
    fo = open(metpak_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
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


def mp101a_parser(temp_humidity_log, sensor_name, sensor_id):
    return adcp_parser(temp_humidity_log, sensor_name, sensor_id)


def internal_parser(humidity_int_log, sensor_name, sensor_id):
    return adcp_parser(humidity_int_log, sensor_name, sensor_id)


def flntu_parser(flntu_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["field1", "field2", "field3", "field4", "field5"]
    fo = open(flntu_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        fields = line.split(']')[1].strip().split()
        i = 3
        while (i < len(fields)):
            data[headers[i-3]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def microcat_parser(microcat_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["field1", "field2", "field3"]
    fo = open(microcat_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        fields = line.split(']')[1].strip().split()
        i = 1
        while (i < len(fields)):
            data[headers[i-1]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def gps_parser(gps_log, sensor_name, sensor_id):
    data = {}
    json_data = []
    headers = ["gps_t_stamp", "validity", "latitude", "north_south", "longitude", "east_west",
                "speed_in_knots", "true_course", "gps_d_stamp", "variation", "east_west_2"]
    fo = open(gps_log)
    for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)
        fields = line.split('$GPRMC,')[1].strip().split(",")
        i = 0
        while (i < len(fields)):
            data[headers[i]] = fields[i].strip()
            i += 1
        data["east_west_2"] = data["east_west_2"].split('*')[0] #remove the checksum part from last field (e.g. E*64 -> E)
        json_data.append(json.dumps(data))
    fo.close()
    return json_data




#-----------------------------------------Main Body--------------------------------------------------#

init_db()


adcp_file = '/home/ilan/projects/themo_parsers/raw/adcp_voltage-averaged-tabs225m10-200002250631.txt'
sensor_name = "adcp"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](adcp_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: " + parser)

vaisala_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405175902/vaisala-ptb-210-barometer-averaged-tabs225m09-201704051730.txt"
sensor_name = "barometer"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](vaisala_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

pyranometer_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/eplab-pyranometer-spp-averaged-tabs225m09-201704052140.txt"
sensor_name = "spp"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](pyranometer_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

radiometer_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/eplab-radiometer-spp-averaged-tabs225m09-201704052141.txt"
sensor_name = "pir"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](radiometer_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

metpak_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/metpak-averaged-tabs225m09-201704052130.txt"
sensor_name = "metpak"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](metpak_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

temp_humidity_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/external_temperature_humidity_MP101A-HUMIDITY-averaged-tabs225m09-201704052130.txt"
sensor_name = "mp101a"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](temp_humidity_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
         insert_samples(document)
         print(document)
else:
    print("error: parser")

humidity_int_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/humidity_internal-averaged-tabs225m09-201704052142.txt"
sensor_name = "internal"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](humidity_int_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")


flntu_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405172902/wetlabs_flntu-averaged-tabs225m09-201704051700.txt"
sensor_name = "flntu"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](flntu_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

microcat_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405172902/microcat-averaged-tabs225m09-201704051700.txt"
sensor_name = "microcat"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](microcat_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
        insert_samples(document)
        print(document)
else:
    print("error: parser")

gps_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405165903/gps_time-averaged-tabs225m09-201704051630.txt"
sensor_name = "gps"
parser = sensor_name + "_parser"
sensor_id = get_sensor_id(sensor_name)
if sensor_id:
    json_data = locals()[parser](gps_file, sensor_name, sensor_id)
    print("\n\n---{}---\n".format(parser))
    for document in json_data:
         insert_samples(document)
         print(document)
else:
    print("error: parser")
