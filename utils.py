import os
import time
from datetime import date
from datetime import datetime
import socket

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


def extract_date_time_from_file_name(log):
    base = os.path.basename(log)
    base_no_ext = os.path.splitext(base)[0]
    date_time = base_no_ext.split('-')[-1]

    year = date_time[0:4]
    month = date_time[4:6]
    day = date_time[6:8]
    hour = date_time[8:10]
    minutes = date_time[10:]

    str_date = year + "-" + month + "-" + day
    str_time = hour + ":" + minutes + ":" + "00"

    return[str_date, str_time]


def print_log(msg, log, prefix="-I- "):
    log_location = os.path.dirname(log)
    if not os.path.exists(log_location):
        os.mkdir(log_location)
    with open(log, "a") as f:
        f.write(prefix + msg + "\n")
    print(prefix + " " + msg)



def init_log(log):
    print_log("+++++++++++++++++++++++THEMO PARSER+++++++++++++++++++++++++", log, "")
    print_log("", log, "")
    print_log("start time: " + now(), log, "")
    print_log("running on: " + socket.gethostname(), log, "")
    print_log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++", log, "")



def now():
    curret_date = time.strftime("%x").replace("/", "")
    current_time = time.strftime("%X").replace(":", "")
    return curret_date + "_" + current_time
