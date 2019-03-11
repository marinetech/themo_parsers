from utils import *
from dbutils import *
import json
from bson.json_util import dumps
import io

adcp_struct = {}

def adcp_wh_parser(adcp_log, sensor_name, sensor_id):

    json_data = []
    data = {}
    data["sensor_name"] = sensor_name
    data["sensor_id"] = sensor_id
    data["source"] = adcp_log

    fo = io.open(adcp_log,'r',encoding='utf-8',errors='ignore')
    for line in fo:

        fields = line.split()

        d_stamp = clean_str(fields[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(fields[1])
        data["t_stamp"] = t_stamp

        encrypted_str = fields[3]
        # print(encrypted_str)
        # print("len: " + str(len(encrypted_str)))
        decrypt_header(encrypted_str)
        decrypt_fixed_leader_data(encrypted_str)
        decrypt_var_leader_data(encrypted_str)
        decrypt_velocity_data(encrypted_str)

        for cell in adcp_struct["velocity_data"]["cells"]:
            # print(cell)
            data["cell_id"] = cell["id"]
            data["beam1"] = cell["beam1"]
            data["beam2"] = cell["beam2"]
            data["beam3"] = cell["beam3"]
            data["beam4"] = cell["beam4"]
            json_data.append(json.dumps(data))

    fo.close()
    return json_data


def decrypt_header(encrypted_str):

    header = {} # will be added to  adcp_struct{} as a nested dictionary

    hdr_id = encrypted_str[0:2]
    header["hdr_id"] = hdr_id

    data_src_id = encrypted_str[2:4]
    header["data_src_id"] = data_src_id

    bytes_in_ensemble = int(encrypted_str[6:8] + encrypted_str[4:6], 16) # 2 bytes- encrypted_str[4:8] - [LSB:MSB]
    header["bytes_in_ensemble"] = bytes_in_ensemble

    spare = encrypted_str[8:10]  #currently not in use
    header["spare"] = spare

    num_of_data_types = int(encrypted_str[10:12], 16)
    header["num_of_data_types"] = num_of_data_types

    data_type_offsets = []
    # every data type has its 2 bytes offset (4 hex charcters) - the 1st offset always starts at position 12
    l = 12
    h = 16
    for i in range (num_of_data_types):
        next_2_bytes = encrypted_str[l:h]
        offset = int(next_2_bytes[2:] + next_2_bytes[:2], 16)
        data_type_offsets.append(offset)
        l = l + 4
        h = h + 4

    header["first_hex_after_header"] = h - 4
    header["data_type_offsets"] = data_type_offsets
    adcp_struct["header"] = header


def decrypt_fixed_leader_data(encrypted_str):
    fixed_leader = {}

    start_of_fixed_leader_data = adcp_struct["header"]["first_hex_after_header"]
    end_of_fixed_leader_data = start_of_fixed_leader_data + 59*2 #59 bytes are the fixed size of that section
    fixed_leader_data = encrypted_str[start_of_fixed_leader_data: end_of_fixed_leader_data] #hex string

    fixed_leader_id = lsb2msb(fixed_leader_data[:4])
    fixed_leader["id"] = fixed_leader_id

    cpu_fw_ver = int(fixed_leader_data[4:6], 16)
    fixed_leader["cpu_fw_ver"] = cpu_fw_ver

    cpu_fw_rev = int(fixed_leader_data[6:8], 16)
    fixed_leader["cpu_fw_rev"] = cpu_fw_rev

    system_conf = bin(int(lsb2msb(fixed_leader_data[8:12]), 16)).zfill(16)
    fixed_leader["system_conf"] = system_conf

    pd_real_sim_flg = int(fixed_leader_data[12:14], 16)
    fixed_leader["pd_real_sim_flg"] = pd_real_sim_flg

    lag_length = int(fixed_leader_data[14:16], 16)
    fixed_leader["lag_length"] = lag_length

    num_of_beams = int(fixed_leader_data[16:18], 16)
    fixed_leader["num_of_beams"] = num_of_beams

    num_of_cells = int(fixed_leader_data[18:20], 16)
    fixed_leader["num_of_cells"] = num_of_cells

    pings_per_ensemble = int(lsb2msb(fixed_leader_data[20:24]), 16)
    fixed_leader["pings_per_ensemble"] = pings_per_ensemble

    depth_cell_length = int(lsb2msb(fixed_leader_data[24:28]), 16)
    fixed_leader["depth_cell_length"] = depth_cell_length
    # print("depth_cell_length: " + str(depth_cell_length))

    fixed_leader["first_hex_after_fixed_leader"] = end_of_fixed_leader_data
    adcp_struct["fixed_leader"] = fixed_leader


def decrypt_var_leader_data(encrypted_str):
    var_leader ={}

    start_of_var_leader_data = adcp_struct["fixed_leader"]["first_hex_after_fixed_leader"]
    end_of_var_leader_data = start_of_var_leader_data + 65*2 #65 bytes are the fixed size of that section
    var_leader_data = encrypted_str[start_of_var_leader_data: end_of_var_leader_data] #hex string

    var_leader["first_hex_after_var_leader"] = end_of_var_leader_data
    adcp_struct["var_leader"] = var_leader


def decrypt_velocity_data(encrypted_str):
    velocity_data = {}
    cells = []

    num_of_cells = adcp_struct["fixed_leader"]["num_of_cells"]
    start_of_velocity_data = adcp_struct["var_leader"]["first_hex_after_var_leader"]
    end_of_velocity_data = start_of_velocity_data + (num_of_cells*16) + 4
    velocity_data_hex_str = encrypted_str[start_of_velocity_data:end_of_velocity_data]

    # DEBUG PRINTS
    # print("start_of_velocity_data: " + str(start_of_velocity_data))
    # print("num_of_cells: " + str(num_of_cells))
    # print("velocity_data_hex_str: " + velocity_data_hex_str)
    # print("len: " + str(len(velocity_data_hex_str)))

    # The WorkHorse ADCP scales velocity data in millimeters per second (mm/s). A value of –32768 (8000h) indicates bad velocity values.
    velocity_data["velocity_id"] = int(lsb2msb(velocity_data_hex_str[:4]), 16)
    cursor = 4
    for c in range (num_of_cells):
        cell_id = "cell" + str(c+1) #c+1 - since the range starts from 0
        # each cell has 4 beams
        # each beam value is 4 hex digits (LSB first)
        # the value is encoded in two's complement
        beam1 = int(lsb2msb(velocity_data_hex_str[cursor : cursor + 4]), 16)
        beam2 = int(lsb2msb(velocity_data_hex_str[cursor + 4 : cursor + 8]), 16)
        beam3 = int(lsb2msb(velocity_data_hex_str[cursor + 8 : cursor + 12]), 16)
        beam4 = int(lsb2msb(velocity_data_hex_str[cursor + 12 : cursor + 16]), 16)

        # convert each beam value from two's complement to human readable int
        if beam1 >= 1<<15: beam1 -= 1<<16
        if beam2 >= 1<<15: beam2 -= 1<<16
        if beam3 >= 1<<15: beam3 -= 1<<16
        if beam4 >= 1<<15: beam4 -= 1<<16

        cell = {}
        cell["id"] = cell_id
        cell["beam1"] = beam1
        cell["beam2"] = beam2
        cell["beam3"] = beam3
        cell["beam4"] = beam4
        cells.append(cell)

        cursor = cursor + 16

    velocity_data["cells"] = cells
    adcp_struct["velocity_data"] = velocity_data


def lsb2msb(string):
    return string[2:] + string[:2]



if  __name__ == "__main__":
    adcp_wh_log = "/home/ilan/Documents/Themo/sensors/RDI WorkHorse/adcp-averaged/adcp-averaged-tabs225m11-201801111000.txt"
    adcp_wh_parser(adcp_wh_log, "adcp_wh", "99999999999")
    # print(str(adcp_struct))
