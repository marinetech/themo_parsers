import os
import time
import smtplib
from datetime import date
from datetime import datetime
import socket
from dbutils import *

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


def send_notification(receiver, objData):
    sender = 'themo@univ.haifa.ac.il'

    message = "\r\n".join([
        "From: themo@univ.haifa.ac.il",
        "To: %s" % receiver,
        "Subject: Themo notifications",
        "",
        "Sensor: %s" % objData["sensor_name"],
        "Data Field: %s" % objData["measurement"],
        "Value: %s" % objData["value"],
        "Trigger: %s" % objData["trigger"],
        "",
        "",
        """
        This email was sent to you based on your prefernces in the Themo website.
        Please visit http://themo.haifa.ac.il if you wish to reconfigure these prefernces."""
    ])

    try:
       smtpObj = smtplib.SMTP('mr1.haifa.ac.il', 25)
       smtpObj.sendmail(sender, receiver, message)
       print("Successfully sent email")
    except SMTPException:
       print("Error: unable to send email")


def trigger_alert(doc):
    sensor = doc["sensor_id"]
    # go through alldata fields that are valid for alert
    for key in doc:
        if key not in ("d_stamp", "t_stamp", "source", "sensor_id", "sensor_name"):
            subscription = sensor + "_" + key
            subscribers = find_subscribers(subscription)
            for subscriber in subscribers:
                print(subscriber)
                if eval(subscriber["expression"].replace("$$VAL", str(doc[key]))):
                    objData = {"trigger" : subscriber["expression"], "measurement" : key, "value" : str(doc[key]), "sensor_name" : doc["sensor_name"]}
                    send_notification(subscriber["email"], objData)
