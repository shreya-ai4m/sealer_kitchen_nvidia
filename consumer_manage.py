import functools
import logging
import pika
from pika.exchange_type import ExchangeType
import psycopg2
from psycopg2.extras import Json
import json
from datetime import datetime, timedelta
import zmq
from PIL import Image
import os
import sys
import snap7
from snap7.util import set_string, get_string
from snap7 import util
import zmq
import time as t

LOG_FORMAT = ('%(levelname)-10s %(asctime)s %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# Load config from JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

shifts = {
    "I": {
        "start": datetime.strptime(config["shifts"]["I"]["start"], "%H:%M:%S").time(),
        "end": datetime.strptime(config["shifts"]["I"]["end"], "%H:%M:%S").time()
    },
    "II": {
        "start": datetime.strptime(config["shifts"]["II"]["start"], "%H:%M:%S").time(),
        "end": datetime.strptime(config["shifts"]["II"]["end"], "%H:%M:%S").time()
    },
    "III": {
        "start": datetime.strptime(config["shifts"]["III"]["start"], "%H:%M:%S").time(),
        "end": datetime.strptime(config["shifts"]["III"]["end"], "%H:%M:%S").time()
    }
}
MESSAGE = {
    "camera_ip": "",
    "current_shift": "",
    "drum_status": "",
    "label_status": "",
    "pump_status": "",
    "drum_loaded": "",
    "volume_transfer": "",
    "drum_level": "",
    "aligned": "",
    "product_name": "",
    "expiry_date": "",
    "batch": "",
    "weight": "",
    "station":""
}

brown_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}
blue_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}
grey_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}

class SEALER(object):
    def __init__(self):
        self.pump_code = None
        self.interlock = 1  # interlock = {1:"OKAY", 2:"CODE-MISMATCH", 3:"EXPIRED", 4:"CODE-MISMATCH & EXPIRED"}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(config["socket_address"])
        self.connection = psycopg2.connect(config["database_connection"])
        self.plc = snap7.client.Client()
        self.internal_plc = snap7.client.Client()
        try:
            self.plc.connect('192.168.0.70', 0, 0)
            self.plc.get_connected()
        except Exception as e:
            print(e)
        try:
            self.internal_plc.connect('192.168.1.21', 0, 1)
            print("Internal_plc_connected:",self.internal_plc.get_connected())
        except Exception as e:
            print(e)
    
    def read_qr_code(self, image_path):
        try:
            image = Image.open(image_path)
            decoded_objects = decode(image)
            for obj in decoded_objects:
                return obj.data.decode('utf-8')
        except Exception as e:
            print(f"pyzbar failed to read the QR code: {e}")

        return None

    def check_shift(self):
        try:
            current_shift = datetime.now().time()

            if shifts["I"]["start"] <= current_shift <= shifts["I"]["end"]:
                MESSAGE["current_shift"] = "I"
            elif shifts["II"]["start"] <= current_shift <= shifts["II"]["end"]:
                MESSAGE["current_shift"] = "II"
            elif (shifts["III"]["start"] <= current_shift) or (current_shift <= shifts["III"]["end"]):
                MESSAGE["current_shift"] = "III"
        except Exception as e:
            print(e)

    def rawdata_insertion(self, camera_ip, data):
        try:
            with self.connection.cursor() as cur:
                if camera_ip == config["camera_ips"]["grey"]:
                    insert_query = config["queries"]["grey_raw_insert_query"]
                elif camera_ip == config["camera_ips"]["blue"]:
                    insert_query = config["queries"]["blue_raw_insert_query"]
                elif camera_ip == config["camera_ips"]["brown"]:
                    insert_query = config["queries"]["brown_raw_insert_query"]
                else:
                    raise ValueError(f"Unknown camera IP: {camera_ip}")

                cur.execute(insert_query, data)
                self.connection.commit()
                print('Raw Data Inserted')
                
        except Exception as e:
            self.connection.rollback()
            LOGGER.error("Exception: %s", e)

    def is_centroid_in_middle_90(self, drum_x1, drum_y1, drum_x2, drum_y2, centroid_x, centroid_y):
        drum_width = drum_x2 - drum_x1
        drum_height = drum_y2 - drum_y1

        mid_70_x1 = drum_x1 + 0.05 * drum_width
        mid_70_x2 = drum_x2 - 0.05 * drum_width
        mid_70_y1 = drum_y1 + 0.05 * drum_height
        mid_70_y2 = drum_y2 - 0.05 * drum_height

        in_x_range = mid_70_x1 <= centroid_x <= mid_70_x2
        in_y_range = mid_70_y1 <= centroid_y <= mid_70_y2

        return in_x_range and in_y_range

    def check_pump_code(self, camera_ip):
        self.pump_code = config["pump_codes"].get(camera_ip)

    def check_pump_status(self, camera_ip):
        if camera_ip=="172.168.16.205":
            data = self.plc.db_read(8,12, 1)
            grey_moving = util.get_bool(data, 0, 2)
            grey_ready_to_run = util.get_bool(data, 0, 5)
            if grey_moving == True:
                MESSAGE["pump_status"]="Move"
            elif grey_ready_to_run == True:
                MESSAGE["pump_status"]="Ready"
            else:
                MESSAGE["pump_status"]="Stop"
        if camera_ip=="172.168.16.203":
            data = self.plc.db_read(8,12, 1)
            blue_moving = util.get_bool(data, 0, 0)
            blue_ready_to_run = util.get_bool(data, 0, 3)
            if blue_moving == True:
                MESSAGE["pump_status"]="Move"
            elif blue_ready_to_run == True:
                MESSAGE["pump_status"]="Ready"
            else:
                MESSAGE["pump_status"]="Stop"
        if camera_ip=="172.168.16.201":
            data = self.plc.db_read(8,12, 1)
            brown_moving = util.get_bool(data, 0, 1)
            brown_ready_to_run = util.get_bool(data, 0, 4)
            if brown_moving == True:
                MESSAGE["pump_status"]="Move"
            elif brown_ready_to_run == True:
                MESSAGE["pump_status"]="Ready"
            else:
                MESSAGE["pump_status"]="Stop"
        

    def write_ascii_string(self, data, string, length):
        string_bytes = string.encode('ascii')
        string_bytes += b' ' * (length - len(string_bytes))
        data[:length] = string_bytes

    def run_tower_light(self, camera_ip):
        try:
            data = bytearray(1)

            if (brown_drum_status["drum_status"]=="") or (brown_drum_status["drum_status"]=="Present" and brown_drum_status["alignment"]=="Not Aligned"):
                snap7.util.set_bool(data,0,5,True)

            if brown_drum_status["alignment"]=="Aligned" and brown_drum_status["interlock"]=="1":
                snap7.util.set_bool(data,0,6,True)
                

            #if brown_drum_status["alignment"]=="Aligned" and (brown_drum_status["interlock"]=="2" or brown_drum_status["interlock"]=="3" or brown_drum_status["interlock"]=="4"):
                #snap7.util.set_bool(data,0,4,True)

            if (blue_drum_status["drum_status"]=="") or (blue_drum_status["drum_status"]=="Present" and blue_drum_status["alignment"]=="Not Aligned"):
                snap7.util.set_bool(data,0,2,True)

            if blue_drum_status["alignment"]=="Aligned" and blue_drum_status["interlock"]=="1":
                snap7.util.set_bool(data,0,3,True)


            #if blue_drum_status["alignment"]=="Aligned" and (blue_drum_status["interlock"]=="2" or blue_drum_status["interlock"]=="3" or blue_drum_status["interlock"]=="4"):
                #snap7.util.set_bool(data,0,1,True)


            if grey_drum_status["alignment"]=="Aligned" and grey_drum_status["interlock"]=="1":
                snap7.util.set_bool(data,0,0,True)

            self.internal_plc.db_write(1,528,data)
            snap7.util.set_bool(data,0,6,False) 
            #if grey_drum_status["alignment"]=="Aligned" and (grey_drum_status["interlock"]=="2" or grey_drum_status["interlock"]=="3" or grey_drum_status["interlock"]=="4"):
                #snap7.util.set_bool(data,0,6,True)
            
            if (grey_drum_status["drum_status"]=="") or (grey_drum_status["drum_status"]=="Present" and grey_drum_status["alignment"]=="Not Aligned"):
                snap7.util.set_bool(data,0,7,True)
        
            self.internal_plc.db_write(1,527,data)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

    def write_plc(self, camera_ip, product_code, batch, expiry_date, interlock):
        try:
            if camera_ip == "172.168.16.201":
                buffer = bytearray(len(product_code)+2)
                set_string(buffer, 0,product_code, len(product_code)+2)
                self.plc.db_write(8,14,buffer)

                buffer = bytearray(len(batch)+2)
                set_string(buffer, 0, batch, len(batch)+2)
                self.plc.db_write(8,36,buffer)

                buffer = bytearray(len(expiry_date)+2)
                set_string(buffer, 0,expiry_date, len(expiry_date)+2)
                print(expiry_date,buffer)
                self.plc.db_write(8,58,buffer)

                data = bytearray(2)
                snap7.util.set_int(data,0,interlock)
                self.plc.db_write(8,82,data)
            
            if camera_ip == "172.168.16.203":
                buffer = bytearray(len(product_code)+2)
                set_string(buffer, 0,product_code, len(product_code)+2)
                self.plc.db_write(8,86,buffer)

                buffer = bytearray(len(batch)+2)
                set_string(buffer, 0, batch, len(batch)+2)
                self.plc.db_write(8,108,buffer)

                buffer = bytearray(len(expiry_date)+2)
                set_string(buffer, 0,expiry_date, len(expiry_date)+2)
                print(expiry_date,buffer)
                self.plc.db_write(8,130,buffer)

                data = bytearray(2)
                snap7.util.set_int(data,0,interlock)
                self.plc.db_write(8,80,data)
            if camera_ip == "172.168.16.205":
                buffer = bytearray(len(product_code)+2)
                set_string(buffer, 0,product_code, len(product_code)+2)
                self.plc.db_write(8,152,buffer)

                buffer = bytearray(len(batch)+2)
                set_string(buffer, 0, batch, len(batch)+2)
                self.plc.db_write(8,174,buffer)

                buffer = bytearray(len(expiry_date)+2)
                set_string(buffer, 0,expiry_date, len(expiry_date)+2)
                print(expiry_date,buffer)
                self.plc.db_write(8,196,buffer)

                data = bytearray(2)
                snap7.util.set_int(data,0,interlock)
                self.plc.db_write(8,84,data)
        except Exception as e:
            print(e)

    def drum_count(self, drum_status, label_status, aligned, station, timestamp, shift):
        current_time = timestamp
        current_date = timestamp.date()
        
        # Check if the required conditions for increasing the count are met
        if drum_status == 'Absent' and label_status == 'Absent' and aligned == 'Not Aligned':
            # Query to check the last update time and count from the table
            query = """
                SELECT last_time_update, count 
                FROM drum_changes 
                WHERE station = %s AND shift = %s AND date = %s
            """
            
            with self.connection.cursor() as cur:
                cur.execute(query, (station, shift, current_date))
                result = cur.fetchone()
                
                if result:
                    last_time_update, drum_count = result
                    # Check if 30 minutes have passed since the last update
                    if current_time >= last_time_update + timedelta(minutes=30):
                        # Update the count and last_time_update in the table
                        update_query = """
                            UPDATE drum_changes 
                            SET count = count + 1, last_time_update = %s 
                            WHERE station = %s AND shift = %s AND date = %s
                        """
                        cur.execute(update_query, (current_time, station, shift, current_date))
                    else:
                        # 30 minutes have not passed, no update
                        return
                else:
                    # If no entry exists for today, insert a new one
                    insert_query = """
                        INSERT INTO drum_changes (station, shift, date, count, last_time_update) 
                        VALUES (%s, %s, %s, 1, %s)
                    """
                    cur.execute(insert_query, (station, shift, current_date, current_time))
            
            # Commit the transaction
            self.connection.commit()

    
    def on_message(self, channel, method, properties, body):

        try:
            # Check and handle if the last data insertion failed
            
            message = body.decode('utf-8')
            data = json.loads(message)
            LOGGER.info("Received message: %s", data)
            timestamp = datetime.now()

            # Define shift end time for Shift III
            shift_end_time = datetime.strptime("06:40", "%H:%M").time()

            # Adjust the timestamp date if the current time is between 00:00 and 06:40
            if timestamp.time() <= shift_end_time:
                timestamp = timestamp - timedelta(days=1)

            camera_ip = data.get("sensorId")
            # Map sensorId to camera IP

            objects = data.get("objects", [])
            self.check_pump_code(camera_ip)
            self.check_pump_status(camera_ip)

            self.check_shift()
            # List of keywords to check in objects
            keywords = ["drum", "Label", "QR_Code"]
            statuses = {}
            statuses_status = {}

            # Enhanced logic to check for 'drum' presence based on specified strings
            drum_keywords = ['grey_drum', 'blue_drum', 'brown_drum', 'Grey_Drum', 'Blue_Drum', 'Brown_Drum']
            if any(keyword in obj for keyword in drum_keywords for obj in objects):
                statuses["drum"] = "True"
                statuses_status["drum"] = "Present"
            else:
                statuses["drum"] = "False"
                statuses_status["drum"] = "Absent"

            # Check for 'Label' and 'QR_Code' in the message
            for keyword in keywords:
                if keyword != "drum":  # Skip the drum as it is already handled
                    found = any(keyword in obj for obj in objects)
                    statuses[keyword] = "True" if found else "False"
                    statuses_status[keyword] = "Present" if found else "Absent"

            # If drum is absent or false, make label and QR_Code absent or false as well
            if statuses.get("drum", "False") == "False":
                statuses["Label"] = "False"
                statuses["QR_Code"] = "False"
                statuses_status["Label"] = "Absent"
                statuses_status["QR_Code"] = "Absent"

            MESSAGE['aligned'] = "Not Aligned"
            if statuses_status["drum"] == "Present":
                print("Checking label lie in mid 90% of drum... ")
                drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y=None,None,None,None,None,None     
                for obj in objects:
                    if 'Grey_Drum' in obj or 'Blue_Drum' in obj or 'Brown_Drum' in obj:
                        drum_x1,drum_y1,drum_x2,drum_y2=float(obj.split('|')[1]),float(obj.split('|')[2]),float(obj.split('|')[3]),float(obj.split('|')[4])
                    if 'Label' in obj:
                        label_x = (float(obj.split('|')[1]) + float(obj.split('|')[3])) / 2
                        label_y = (float(obj.split('|')[2]) + float(obj.split('|')[4])) / 2
                if drum_x1 is not None and drum_y1 is not None and drum_x2 is not None and drum_y2 is not None and label_x is not None and label_y is not None:
                    if self.is_centroid_in_middle_90(drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y):
                        MESSAGE['aligned'] = "Aligned"  
                    
            if statuses["Label"] == "True" and statuses["QR_Code"] == "True":
                print("Checking qr lie in mid 90% of label...")
                label_x1,label_y1,label_x2,label_y2,qr_x,qr_y=None,None,None,None,None,None
                for obj in objects:
                    if 'Label' in obj:
                        label_x1,label_y1,label_x2,label_y2=float(obj.split('|')[1]),float(obj.split('|')[2]),float(obj.split('|')[3]),float(obj.split('|')[4])
                    if 'QR_Code' in obj:
                        qr_x = (float(obj.split('|')[1]) + float(obj.split('|')[3])) / 2
                        qr_y = (float(obj.split('|')[2]) + float(obj.split('|')[4])) / 2
                
                if label_x1 is not None and label_y1 is not None and label_x2 is not None and label_y2 is not None and qr_x is not None and qr_y is not None:
                    if self.is_centroid_in_middle_90(label_x1,label_y1,label_x2,label_y2,qr_x,qr_y):
                        MESSAGE['aligned'] = "Aligned"

            # Extract product data (shared logic for QR_Code and Label)
            product_name, weight, batch, expiry_date = "--", "--", "--", "--"

            for keyword in ["Label","QR_Code"]:
                if statuses[keyword] == "True":
                    data_list = [obj.split('|')[6] for obj in objects if keyword in obj]
                    
                    if data_list:
                        data_text = data_list[0]
                        print(f"data_list for {keyword}: {data_text}")
                        parts = data_text.split()
                        if len(parts) >= 5 and len(parts[4]) >= 20:  # Check if the parts list has enough items and the batch_expiry part is long enough
                            try:
                            
                                product_name = ' '.join(parts[:3])
                                weight = parts[3]
                                batch_expiry = parts[4]
                                batch = batch_expiry[:12]
                                expiry_date = batch_expiry[12:20]
                                product_code = parts[2]
                            except IndexError:
                                LOGGER.warning(f'{keyword.capitalize()} filename not in data')
                            
                        else:
                            LOGGER.warning(f'{keyword.capitalize()} filename appears to be noisy or incomplete')     
                    break  # Only process the first match    
                else:
                    product_name = '--'
                    weight = '--'
                    batch_expiry = '--'
                    batch = '--'
                    expiry_date = '--'

                print(f"product_name: {product_name}")
                print(f"weight: {weight}")
            self.connection.autocommit = False

            try:
                # Insert into the appropriate table based on the camera IP
                raw_data = (
                    timestamp, statuses["drum"], statuses["Label"], statuses["QR_Code"],
                    statuses["drum"], Json(objects), product_name, weight, batch, expiry_date
                )
                self.rawdata_insertion(camera_ip, raw_data)

                # Insert into the appropriate status table
                status_query = None
                if camera_ip == config["camera_ips"]["grey"]:
                    status_query = config["queries"]["grey_status_insert_query"]
                    station = "Hem Sealer"
                elif camera_ip == config["camera_ips"]["blue"]:
                    status_query = config["queries"]["blue_status_insert_query"]
                    station = "Seam Sealer"
                elif camera_ip == config["camera_ips"]["brown"]:
                    status_query = config["queries"]["brown_status_insert_query"]
                    station = "UBC"

                if status_query:
                    with self.connection.cursor() as cur:
                        cur.execute(
                            status_query,
                            (station, MESSAGE["current_shift"], timestamp, statuses_status["drum"], statuses_status["Label"], MESSAGE["pump_status"], MESSAGE["aligned"])
                        )
                        self.connection.commit()

                LOGGER.info("Data inserted successfully into both tables")
                channel.basic_ack(delivery_tag=method.delivery_tag)

                # Call drum_count with the required parameters
                self.drum_count(statuses_status["drum"], statuses_status["Label"], MESSAGE["aligned"],station,timestamp,MESSAGE["current_shift"])
         

            except Exception as e:
                LOGGER.error(f"Error processing message: {e}")
                self.connection.rollback()

            

            MESSAGE["camera_ip"] = camera_ip
            MESSAGE["drum_status"] = statuses_status["drum"]
            MESSAGE["label_status"] = statuses_status["Label"]
            
            try:
                if datetime.strptime(expiry_date, "%d/%m/%y") < datetime.today():
                    self.interlock = 3
                if product_code != str(self.pump_code):
                    self.interlock = 2
                if (product_code != str(self.pump_code) )and datetime.strptime(expiry_date, "%d/%m/%y") < datetime.today():
                    self.interlock = 4
                self.write_plc(camera_ip, product_code,batch, expiry_date, self.interlock)
                print("Data has been written in mvml PLC")
            except Exception as e:
                print(e)

            if camera_ip=="172.168.16.205":
                grey_drum_status["drum_status"],grey_drum_status["alignment"],grey_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
            if camera_ip=="172.168.16.203":
                blue_drum_status["drum_status"],blue_drum_status["alignment"],blue_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
            if camera_ip=="172.168.16.201":
                brown_drum_status["drum_status"],brown_drum_status["alignment"],brown_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
       
          

            
        except json.JSONDecodeError as e:
            LOGGER.error(f"JSON decode error: {e}")
        except Exception as e:
            LOGGER.error(f"Exception in on_message: {e}")


    def run(self):
        credentials = pika.PlainCredentials(config["rabbitmq"]["username"], config["rabbitmq"]["password"])
        parameters = pika.ConnectionParameters(
            config["rabbitmq"]["host"],
            config["rabbitmq"]["port"],
            config["rabbitmq"]["virtual_host"],
            credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(exchange=config["rabbitmq"]["exchange"], exchange_type=ExchangeType.topic, durable=True)

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        binding_keys = ["#"]

        for binding_key in binding_keys:
            channel.queue_bind(exchange=config["rabbitmq"]["exchange"], queue=queue_name, routing_key=binding_key)

        channel.basic_consume(queue=queue_name, on_message_callback=self.on_message, auto_ack=False)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()

if __name__ == '__main__':
    sealer = SEALER()
    sealer.run()