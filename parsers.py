from utils import *
import json
from bson.json_util import dumps


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


# def barometer_parser(vaisala_log, sensor_name, sensor_id):
#     data = {}
#     json_data = []
#     fo = open(vaisala_log)
#     for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         fields = line.split()
#         d_stamp = clean_str(fields[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(fields[1])
#         data["t_stamp"] = format_time(t_stamp)
#         data[fields[2]] = fields[3]
#         json_data.append(json.dumps(data))
#     fo.close()
#     return json_data


def pir_parser(radiometerr_log, sensor_name, sensor_id):
    return adcp_parser(radiometerr_log, sensor_name, sensor_id)


def spp_parser(pyranometer_log, sensor_name, sensor_id):
    return adcp_parser(pyranometer_log, sensor_name, sensor_id)


# def metpak_parser(metpak_log, sensor_name, sensor_id):
#     data = {}
#     json_data = []
#     headers = ["node_letter", "wind_direction", "wind_speed", "pressure", "humidity", "temperature", "dewpoint", "supply_voltage", "status_code", "checksum"]
#     fo = open(metpak_log)
#     for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         d_stamp = clean_str(line.split(' ')[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(line.split(' ')[1])
#         data["t_stamp"] = format_time(t_stamp)
#         rest_of_the_line = line.split(']')[1].strip()
#         fields = rest_of_the_line.split(',')
#         i = 0
#         while (i < len(fields)):
#             data[headers[i]] = fields[i].strip()
#             i += 1
#         json_data.append(json.dumps(data))
#     fo.close()
#     return json_data


# def mp101a_parser(temp_humidity_log, sensor_name, sensor_id):
#     return adcp_parser(temp_humidity_log, sensor_name, sensor_id)


# def internal_parser(humidity_int_log, sensor_name, sensor_id):
#     return adcp_parser(humidity_int_log, sensor_name, sensor_id)


# def flntu_parser(flntu_log, sensor_name, sensor_id):
#     data = {}
#     json_data = []
#     headers = ["field1", "field2", "field3", "field4", "field5"]
#     fo = open(flntu_log)
#     for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         d_stamp = clean_str(line.split(' ')[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(line.split(' ')[1])
#         data["t_stamp"] = format_time(t_stamp)
#         fields = line.split(']')[1].strip().split()
#         i = 3
#         while (i < len(fields)):
#             data[headers[i-3]] = fields[i].strip()
#             i += 1
#         json_data.append(json.dumps(data))
#     fo.close()
#     return json_data


# def microcat_parser(microcat_log, sensor_name, sensor_id):
#     data = {}
#     json_data = []
#     headers = ["field1", "field2", "field3"]
#     fo = open(microcat_log)
#     for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         d_stamp = clean_str(line.split(' ')[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(line.split(' ')[1])
#         data["t_stamp"] = format_time(t_stamp)
#         fields = line.split(']')[1].strip().split()
#         i = 1
#         while (i < len(fields)):
#             data[headers[i-1]] = fields[i].strip()
#             i += 1
#         json_data.append(json.dumps(data))
#     fo.close()
#     return json_data


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


# def windsonic_parser(windsonic_log, sensor_name, sensor_id):
#    data = {}
#    json_data = []
#    fo = open(windsonic_log)
#    for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         d_stamp = clean_str(line.split(' ')[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(line.split(' ')[1])
#         data["t_stamp"] = format_time(t_stamp)
#         rest_of_the_line = line.split(']')[1].strip()
#         fields = rest_of_the_line.split(' ')[1:]
#         i = 0
#         idx = 1 #will enumarate MAGNITUDE fields
#         while (i < len(fields)):
#             #This log contains more than one "MAGNITUDE" :(
#             if fields[i].strip() == "MAGNITUDE":
#                 fields[i] = "MAGNITUDE_" + str(idx)
#                 idx += 1
#             data[fields[i].lower().strip().replace(' ', '_')] = fields[i+1].strip()
#             i += 2
#         #json_data.append(json.dumps(data))
#         json_data.append(dumps(data))
#    fo.close()
#    return json_data


# def s9_parser(s9_log, sensor_name, sensor_id):
#    data = {}
#    json_data = []
#    fo = open(windsonic_log)
#    for line in fo:
#         data["sensor_name"] = sensor_name
#         data["sensor_id"] = sensor_id
#         d_stamp = clean_str(line.split(' ')[0])
#         data["d_stamp"] = d_stamp
#         t_stamp = clean_str(line.split(' ')[1])
#         data["t_stamp"] = format_time(t_stamp)
#         rest_of_the_line = line.split(']')[1].strip()
#         fields = rest_of_the_line.split(' ')[1:]
#         i = 0
#         idx = 1 #will enumarate MAGNITUDE fields
#         while (i < len(fields)):
#             #This log contains more than one "MAGNITUDE" :(
#             if fields[i].strip() == "MAGNITUDE":
#                 fields[i] = "MAGNITUDE_" + str(idx)
#                 idx += 1
#             data[fields[i].lower().strip().replace(' ', '_')] = fields[i+1].strip()
#             i += 2
#         #json_data.append(json.dumps(data))
#         json_data.append(dumps(data))
#    fo.close()
#    return json_data
