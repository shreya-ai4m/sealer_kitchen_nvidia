from datetime import datetime
from config import (
    grey_drum_status,
    blue_drum_status,
    brown_drum_status,
)
import snap7
from snap7.util import set_string, set_int

import logging

logging.basicConfig(
    level=logging.INFO,  # Set the log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log format
    handlers=[
        logging.FileHandler("logics.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

def handle_interlocks(camera_ip, message):
    """Function to handle interlocks based on expiry date, pump code, and product code."""
    try:
        product_code = check_pump_code(camera_ip)
        pump_code = message.get('product_code')
        expiry_date = message.get("expiry_date")

        interlock = 1  # Default interlock
        if expiry_date:
            if datetime.strptime(expiry_date, "%d/%m/%y") < datetime.today():
                interlock = 3
        if product_code != str(pump_code):
            interlock = 2
        if (product_code != str(pump_code)) and expiry_date and datetime.strptime(expiry_date, "%d/%m/%y") < datetime.today():
            interlock = 4

        return interlock
    except Exception as e:
        logging.error("Error handling interlocks: %s", e)
        return None

def check_pump_code(camera_ip):
    """Function to return the pump code based on the camera IP."""
    if camera_ip == "rtsp://admin:admin@172.168.16.205:554/unicaststream/2":
        return 3097
    if camera_ip == "rtsp://admin:admin@172.168.16.203:554/unicaststream/2":
        return 3098
    if camera_ip == "rtsp://admin:admin@172.168.16.201:554/unicaststream/2":
        return 3090

def write_plc(camera_ip, product_code, batch, expiry, interlock):
    try:
        if camera_ip == "rtsp://admin:admin@172.168.16.201:554/unicaststream/2":
            buffer = bytearray(len(product_code) + 2)
            set_string(buffer, 0, product_code, len(product_code) + 2)
            plc.db_write(8, 14, buffer)

            buffer = bytearray(len(batch) + 2)
            set_string(buffer, 0, batch, len(batch) + 2)
            plc.db_write(8, 36, buffer)

            buffer = bytearray(len(expiry) + 2)
            set_string(buffer, 0, expiry, len(expiry) + 2)
            plc.db_write(8, 58, buffer)

            data = bytearray(2)
            set_int(data, 0, interlock)
            plc.db_write(8, 82, data)

        if camera_ip == "rtsp://admin:admin@172.168.16.203:554/unicaststream/2":
            buffer = bytearray(len(product_code) + 2)
            set_string(buffer, 0, product_code, len(product_code) + 2)
            plc.db_write(8, 86, buffer)

            buffer = bytearray(len(batch) + 2)
            set_string(buffer, 0, batch, len(batch) + 2)
            plc.db_write(8, 108, buffer)

            buffer = bytearray(len(expiry) + 2)
            set_string(buffer, 0, expiry, len(expiry) + 2)
            plc.db_write(8, 130, buffer)

            data = bytearray(2)
            set_int(data, 0, interlock)
            plc.db_write(8, 80, data)

        if camera_ip == "rtsp://admin:admin@172.168.16.205:554/unicaststream/2":
            buffer = bytearray(len(product_code) + 2)
            set_string(buffer, 0, product_code, len(product_code) + 2)
            plc.db_write(8, 152, buffer)

            buffer = bytearray(len(batch) + 2)
            set_string(buffer, 0, batch, len(batch) + 2)
            plc.db_write(8, 174, buffer)

            buffer = bytearray(len(expiry) + 2)
            set_string(buffer, 0, expiry, len(expiry) + 2)
            plc.db_write(8, 196, buffer)

            data = bytearray(2)
            set_int(data, 0, interlock)
            plc.db_write(8, 84, data)
    except Exception as e:
        logging.error("Error writing to PLC: %s", e)

def assign_drum_status(camera_ip, interlock, message):
    """Assign drum status based on the camera IP and interlock values."""
    if camera_ip == "rtsp://admin:admin@172.168.16.205:554/unicaststream/1":
        grey_drum_status["drum_status"], grey_drum_status["alignment"], grey_drum_status["interlock"] = message[
            "drum_status"], message['aligned'], str(interlock)
    if camera_ip == "rtsp://admin:admin@172.168.16.203:554/unicaststream/1":
        blue_drum_status["drum_status"], blue_drum_status["alignment"], blue_drum_status["interlock"] = message[
            "drum_status"], message['aligned'], str(interlock)
    if camera_ip == "rtsp://admin:admin@172.168.16.201:554/unicaststream/1":
        brown_drum_status["drum_status"], brown_drum_status["alignment"], brown_drum_status["interlock"] = message[
            "drum_status"], message['aligned'], str(interlock)