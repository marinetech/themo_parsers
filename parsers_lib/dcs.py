from utils import *
import json
from bson.json_util import dumps

def dcs_parser(dcs_log, sensor_name, sensor_id):
   data = {}
   json_data = []
   fo = open(dcs_log)
   for line in fo:
        data["sensor_name"] = sensor_name
        data["sensor_id"] = sensor_id
        data["source"] = dcs_log

        d_stamp = clean_str(line.split(' ')[0])
        data["d_stamp"] = d_stamp

        t_stamp = clean_str(line.split(' ')[1])
        data["t_stamp"] = format_time(t_stamp)

        formatted_line = ' '.join(line.split()).split()

        try:
            # e.g. DCS_MEASUREMENT = 4830 164
            col = '_'.join(formatted_line[2:4])
            val = ' '.join(formatted_line[4:6])
            data[col] = val


            # e.g. Abs_Speed[cm/s] - 3.765768E+01
            col = '_'.join(formatted_line[6:8])
            val = '_'.join(formatted_line[8:9])
            data[col] = float(val)

            # e.g. Direction[Deg.M] - 4.719830E+01
            col = '_'.join(formatted_line[9:10]).replace(".", "")
            val = '_'.join(formatted_line[10:11])
            data[col] = float(val)

            # e.g. North[cm/s] - 2.558701E+01
            col = '_'.join(formatted_line[11:12])
            val = '_'.join(formatted_line[12:13])
            data[col] = float(val)

            # e.g. East[cm/s] - 2.762981E+01
            col = '_'.join(formatted_line[13:14])
            val = '_'.join(formatted_line[14:15])
            data[col] = float(val)

            # e.g. Heading[Deg.M] - 3.230069E+02
            col = '_'.join(formatted_line[15:16]).replace(".", "")
            val = '_'.join(formatted_line[16:17])
            data[col] = float(val)

            # e.g. Tilt_X[Deg] - -1.784003E+00
            col = '_'.join(formatted_line[17:19])
            val = '_'.join(formatted_line[19:20])
            data[col] = float(val)

            # e.g. Tilt_Y[Deg] - 2.804762E+00
            col = '_'.join(formatted_line[20:22])
            val = '_'.join(formatted_line[22:23])
            data[col] = float(val)

            # e.g. SP_Std[cm/s] - 2.007264E+01
            col = '_'.join(formatted_line[23:25])
            val = '_'.join(formatted_line[25:26])
            data[col] = float(val)

            # e.g. Strength[dB] - -3.337246E+01
            col = '_'.join(formatted_line[26:27])
            val = '_'.join(formatted_line[27:28])
            data[col] = float(val)

            # e.g. Ping_Count - 150
            col = '_'.join(formatted_line[28:30])
            val = '_'.join(formatted_line[30:31])
            data[col] = float(val)

            # e.g. Abs_Tilt[Deg] - 3.495862E+00
            col = '_'.join(formatted_line[31:33])
            val = '_'.join(formatted_line[33:34])
            data[col] = float(val)

            # e.g. Max_Tilt[Deg] - 6.413986E+00
            col = '_'.join(formatted_line[34:36])
            val = '_'.join(formatted_line[36:37])
            data[col] = float(val)

            # e.g. Std_Tilt[Deg] - 1.256565E+00
            col = '_'.join(formatted_line[37:39])
            val = '_'.join(formatted_line[39:40])
            data[col] = float(val)

            # e.g. Temperature[DegC] - 3.011922E+01
            col = '_'.join(formatted_line[40:41])
            val = '_'.join(formatted_line[41:42])
            data[col] = float(val)

        except:
            continue

        json_data.append(dumps(data))
   fo.close()
   return json_data
