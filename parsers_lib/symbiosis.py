import os
import shutil


def symbiosis_parser(symbiosis_file, sensor_name, sensor_id):
    symbiosis_data_dir = "/mnt/themo/symbiosis"
    # os.makedirs(symbiosis_data_dir, mode=0o777, exist_ok=False)
    shutil.move(symbiosis_file, symbiosis_data_dir)
    return None


if  __name__ == "__main__":
    symbiosis_file = "/home/ilan/sea/battery_voltage1-averaged-tabs225m09-201708100403.txt"
    sensor_id = "666"
    data = battery_parser(battery_log, "battery", sensor_id)
