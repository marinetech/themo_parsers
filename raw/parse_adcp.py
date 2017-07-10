import json
from pymongo import MongoClient


def clean_str(str):
    ret = str
    ret = ret.replace('[', '')
    ret = ret.replace(']', '')
    ret = ret.replace('(', '')
    ret = ret.replace(')', '')
    ret = ret.replace('{', '')
    ret = ret.replace('}', '')
    return ret

#-----------   DB functions  -----------#


class buoy:
    _id = ""
    name = ""
    sensors = []

    def display(self):
        print(self._id)
        print(self.name)
        for sensor in self.sensors:
            print(sensor["name"] + " " + str(sensor["_id"]))



client = MongoClient()
db = client.themo
collection = db.buoys
for item in collection.find():
    _buoy = buoy()
    _buoy._id = item["_id"]
    _buoy.name = item["name"]

    for sensor in item["sensors"]:
         _buoy.sensors.append(sensor)

    _buoy.display()



#-------------   PARSERS ---------------#


def adcp_parser(adcp_log):
   data = {}
   json_data = []
   fo = open(adcp_log)
   for line in fo:
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = t_stamp
        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(',')
        i = 0
        while (i < len(fields)):
            data[fields[i].strip().replace(' ', '_')] = fields[i+1].strip()
            i += 2
        json_data.append(json.dumps(data))
   fo.close()
   return json_data


def vaisala_parser(vaisala_log):
    data = {}
    json_data = []
    fo = open(vaisala_log)
    for line in fo:
        fields = line.split()
        d_stamp = clean_str(fields[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(fields[1])
        data["t_stamp"] = t_stamp
        data[fields[2]] = fields[3]
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def radiometer_parser(radiometerr_log):
    return adcp_parser(radiometerr_log)


def pyranometer_parser(pyranometer_log):
    return adcp_parser(pyranometer_log)


def metpak_parser(metpak_log):
    data = {}
    json_data = []
    headers = ["node_letter", "wind_direction", "wind_speed", "pressure", "humidity", "temperature", "dewpoint", "supply_voltage", "status_code", "checksum"]
    fo = open(metpak_log)
    for line in fo:
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = t_stamp
        rest_of_the_line = line.split(']')[1].strip()
        fields = rest_of_the_line.split(',')
        i = 0
        while (i < len(fields)):
            data[headers[i]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def mp101a_parser(temp_humidity_log):
    return adcp_parser(temp_humidity_log)


def int_humidity_parser(humidity_int_log):
    return adcp_parser(humidity_int_log)


def flntu_parser(flntu_log):
    data = {}
    json_data = []
    headers = ["field1", "field2", "field3", "field4", "field5"]
    fo = open(flntu_log)
    for line in fo:
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = t_stamp
        fields = line.split(']')[1].strip().split()
        i = 3
        while (i < len(fields)):
            data[headers[i-3]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def microcat_parser(microcat_log):
    data = {}
    json_data = []
    headers = ["field1", "field2", "field3"]
    fo = open(microcat_log)
    for line in fo:
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = t_stamp
        fields = line.split(']')[1].strip().split()
        i = 1
        while (i < len(fields)):
            data[headers[i-1]] = fields[i].strip()
            i += 1
        json_data.append(json.dumps(data))
    fo.close()
    return json_data


def gps_parser(gps_log):
    data = {}
    json_data = []
    headers = ["gps_t_stamp", "validity", "latitude", "north_south", "longitude", "east_west",
                "speed_in_knots", "true_course", "gps_d_stamp", "variation", "east_west_2"]
    fo = open(gps_log)
    for line in fo:
        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp
        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = t_stamp
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



# adcp_file = '/home/ilan/projects/themo_parsers/raw/adcp_voltage-averaged-tabs225m10-200002250631.txt'
# parser = "adcp_parser"
# json_data = locals()[parser](adcp_file)
# for document in json_data:
#     print(document)
#
# vaisala_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405175902/vaisala-ptb-210-barometer-averaged-tabs225m09-201704051730.txt"
# parser = "vaisala_parser"
# json_data = locals()[parser](vaisala_file)
# for document in json_data:
#     print(document)
#
# pyranometer_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/eplab-pyranometer-spp-averaged-tabs225m09-201704052140.txt"
# parser = "pyranometer_parser"
# json_data = locals()[parser](pyranometer_file)
# for document in json_data:
#     print(document)
#
# radiometer_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/eplab-radiometer-spp-averaged-tabs225m09-201704052141.txt"
# parser = "radiometer_parser"
# json_data = locals()[parser](radiometer_file)
# for document in json_data:
#     print(document)
#
# metpak_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/metpak-averaged-tabs225m09-201704052130.txt"
# parser = "metpak_parser"
# json_data = locals()[parser](metpak_file)
# for document in json_data:
#     print(document)
#
# temp_humidity_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/external_temperature_humidity_MP101A-HUMIDITY-averaged-tabs225m09-201704052130.txt"
# parser = "mp101a_parser"
# json_data = locals()[parser](temp_humidity_file)
# for document in json_data:
#      print(document)
#
# humidity_int_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405215902/humidity_internal-averaged-tabs225m09-201704052142.txt"
# parser = "int_humidity_parser"
# json_data = locals()[parser](humidity_int_file)
# for document in json_data:
#     print(document)
#
#
# flntu_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405172902/wetlabs_flntu-averaged-tabs225m09-201704051700.txt"
# parser = "flntu_parser"
# json_data = locals()[parser](flntu_file)
# for document in json_data:
#     print(document)
#
# microcat_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405172902/microcat-averaged-tabs225m09-201704051700.txt"
# parser = "microcat_parser"
# json_data = locals()[parser](microcat_file)
# for document in json_data:
#     print(document)
#
# gps_file = "/home/ilan/projects/themo_parsers/raw/averaged-data-tabs225m09-20170405165903/gps_time-averaged-tabs225m09-201704051630.txt"
# parser = "gps_parser"
# json_data = locals()[parser](gps_file)
# for document in json_data:
#      print(document)
