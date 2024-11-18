import pika
import json
import zmq
import snap7
from snap7 import util
import psycopg2
from datetime import datetime,timezone,time
from shapely.geometry import Polygon, Point
import time as sleep_time
import sys
import os

shifts = {"I":{"start":time(6, 30),"end":time(15, 10)},"II":{"start":time(15, 10),"end":time(23, 50)},"III":{"start":time(23, 50),"end":time(6, 30)}}

MESSAGE = {
        "camera_ip":"",
        "date":"",        
        "time":"",
        "current_shift":"",
        "drum_status":"",
        "label_status":"",
        "pump_status":"",
        "drum_loaded":"",
        "volume_transfer":"",
        "drum_level":"",
        "aligned":"",
        "product_name":"",
        "expiry_date":"",
        "batch":"",
        "weight":""
        }
config_data = {
        "station":"",
        "timestamp":datetime.now(),
        "config":{}
        }
brown_drum_status = {"drum_status":"","alignment":"","interlock":""}
blue_drum_status = {"drum_status":"","alignment":"","interlock":""}
grey_drum_status = {"drum_status":"","alignment":"","interlock":""}

sk_history = {"date":'', "timestamp":datetime.now(), "camera_ip":'', "station":'', "drum_status":'', "product_name":'', "expiry_date":'', "batch":'', "weight":'', "interlock":''}
station_name = {"192.168.1.30": "Hem Flange Pump", "192.168.1.31":"Seam Sealer (ISS) Pump", "192.168.1.32": "Underbody Coating (UBC) Pump", "LH_carriage":"dummy_station","RH_carriage":"dummy_station"}

class SEALER(object):
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        #self.socket.setsockopt(zmq.CONFLATE, 1)
        self.socket.bind("tcp://192.168.1.10:5555")
        self.raw_insert_query = """INSERT INTO raw_data (version, id, timestamp, sensor_id, label, qr_code, qr_value, grey_drum, blue_drum, brown_drum, no_drum) VALUES %s"""
        self.update_query = """INSERT INTO sk_config(station, timestamp, config) VALUES (%s, %s, %s) ON CONFLICT (station) DO UPDATE SET timestamp = EXCLUDED.timestamp, config = EXCLUDED.config;"""
        self.new_drum_query = """INSERT INTO sk_history(date, timestamp, camera_ip, station, drum_status, product_name, expiry_date, batch, weight, interlock) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.db_connection = """postgres://postgres:ai4m2024@localhost:5432/selaer_kitchen?sslmode=disable"""
        self.connection = psycopg2.connect(self.db_connection)
        self.interlock = 1 #interlock = {1:"OKAY", 2:"CODE-MISMATCH", 3:"EXPIRED", 4:"CODE-MISMATCH & EXPIRED"}
        self.pump_code = None
        self.new_drum = {'192.168.1.30':False,'192.168.1.31':False,'192.168.1.32':False}
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

    def rawdata_insertion(self,data):
        try:
            cur = self.connection.cursor()
            cur.execute(self.raw_insert_query, [data])
            self.connection.commit()
            print('Raw Data Inserted')

        except Exception as e:
            self.connection.rollback()
            print("Exception: ", e)



    def update_config(self,data):
        try:
            cur = self.connection.cursor()
            data_update = (data["station"],data["timestamp"],json.dumps(data["config"]))
            cur.execute(self.update_query,data_update)
            self.connection.commit()
            print('Data updated in postgres config')

        except Exception as e :
            self.connection.rollback()
            print(e)

    def insert_new_drum_data(self, data):
        try:
            cur = self.connection.cursor()
            data_insert = (data["date"],data["timestamp"],data["camera_ip"],data["station"],data["drum_status"],data["product_name"],data["expiry_date"],data["batch"],data["weight"],data["interlock"],)
            cur.execute(self.new_drum_query, data_insert)
            self.connection.commit()
            print('New drum data inserted in postgres sk_history')

        except Exception as e:
            self.connection.rollback()
            print("Exception: ", e)


    def send_message(self,message):
        try:
            self.socket.send_json(message)
            print("ZMQ sent the message")
        except Exception as e:
            print(e)
    
    def check_shift(self):
        try:
            current_shift = datetime.now().time()
            if shifts["I"]["start"] < current_shift < shifts["I"]["end"]:
                MESSAGE["current_shift"] = "I"
            if shifts["II"]["start"] < current_shift < shifts["II"]["end"]:
                MESSAGE["current_shift"] = "II"
            if shifts["III"]["start"] < current_shift < shifts["III"]["end"]:
                MESSAGE["current_shift"] = "III"
        except Exception as e:
            print(e)

    def check_pump_code(self,camera_ip):
        if camera_ip == "192.168.1.30":
            self.pump_code = 3097
        if camera_ip == "192.168.1.31":
            self.pump_code = 3098
        if camera_ip == "192.168.1.32":
            self.pump_code = 3090

    def write_ascii_string(self, data, string, length):
        string_bytes = string.encode('ascii')
        string_bytes += b' ' * (length - len(string_bytes))
        data[:length] = string_bytes


    def write_plc(self, camera_ip, product_code, batch, expiry, interlock):
        try:
            if product_code == "3098":
                data = bytearray(4)
                self.write_ascii_string(data, product_code, 4)
                self.plc.db_write(8,14,data)

                data = bytearray(12)
                self.write_ascii_string(data, batch, 12)
                self.plc.db_write(8,36,data)

                data = bytearray(8)
                self.write_ascii_string(data, expiry, 8)
                self.plc.db_write(8,58,data)

                data = bytearray(2)
                #if camera_ip == "192.168.1.30":
                #    snap7.util.set_int(data,0,interlock)
                #    self.plc.db_write(8,80,data)
                if camera_ip == "192.168.1.31":
                    snap7.util.set_int(data,0,interlock)
                    self.plc.db_write(8,82,data)
                #if camera_ip == "192.168.1.32":
                #    snap7.util.set_int(data,0,interlock)
                #    self.plc.db_write(8,84,data)
        except Exception as e:
            print(e)

        #updated_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        #data = bytearray(19)
        #snap7.util.set_string(data,0,updated_at,19)
        #self.plc.db_write(1,2,data)

    def run_tower_light(self):
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


            if (grey_drum_status["drum_status"]=="") or (grey_drum_status["drum_status"]=="Present" and grey_drum_status["alignment"]=="Not Aligned"):
                snap7.util.set_bool(data,0,7,True)

            if grey_drum_status["alignment"]=="Aligned" and grey_drum_status["interlock"]=="1":
                snap7.util.set_bool(data,0,0,True)

            #if grey_drum_status["alignment"]=="Aligned" and (grey_drum_status["interlock"]=="2" or grey_drum_status["interlock"]=="3" or grey_drum_status["interlock"]=="4"):
                #snap7.util.set_bool(data,0,6,True)
            self.internal_plc.db_write(1,528,data)    
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

    def is_centroid_in_middle_70(self,drum_x1, drum_y1, drum_x2, drum_y2, centroid_x, centroid_y):
        drum_width = drum_x2 - drum_x1
        drum_height = drum_y2 - drum_y1

        mid_70_x1 = drum_x1 + 0.15 * drum_width
        mid_70_x2 = drum_x2 - 0.15 * drum_width
        mid_70_y1 = drum_y1 + 0.15 * drum_height
        mid_70_y2 = drum_y2 - 0.15 * drum_height

        in_x_range = mid_70_x1 <= centroid_x <= mid_70_x2
        in_y_range = mid_70_y1 <= centroid_y <= mid_70_y2
        
        return in_x_range and in_y_range

    def raw_data_insert(self,json_string):
        data = json.loads(json_string)
        objects = data.get("objects", [])
    
        label_present = any('Label' in obj for obj in objects)
        qr_codes = [obj.split('|')[6] for obj in objects if 'QR_Code' in obj]
        qr_code_present = bool(qr_codes)
        qr_value = qr_codes[0] if qr_codes else ''
    
        grey_drum_present = any('grey_drum' in obj for obj in objects)
        blue_drum_present = any('blue_drum' in obj for obj in objects)
        brown_drum_present = any('brown_drum' in obj for obj in objects)
        no_drum_present = any('no_drum' in obj for obj in objects)

        current_timestamp = datetime.now()
        data_row = (data.get("version", ""),data.get("id", ""),current_timestamp,data.get("sensorId", ""),label_present,qr_code_present,qr_value,grey_drum_present,blue_drum_present,brown_drum_present,no_drum_present) 
        self.rawdata_insertion(data_row)

    def rabbitmq_message(self,ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            print("Received message:", message)
            #with open('amqp_messages.txt', 'a') as file:
            #    file.write(json.dumps(message) + '\n')
            """
            self.raw_data_insert(json.dumps(message))
            MESSAGE['aligned'] = "Not Aligned" 
            MESSAGE["camera_ip"] = message["sensorId"]
            MESSAGE["date"]=datetime.now().strftime("%d-%m-%Y")
            MESSAGE["time"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            self.check_shift()
            self.check_pump_code(MESSAGE["camera_ip"])
            
            if message["sensorId"]=="192.168.1.30":
                data = self.plc.db_read(8,12, 1)
                grey_moving = util.get_bool(data, 0, 2)
                grey_ready_to_run = util.get_bool(data, 0, 5)
                if grey_moving == True:
                    MESSAGE["pump_status"]="Move"
                elif grey_ready_to_run == True:
                    MESSAGE["pump_status"]="Ready"
                else:
                    MESSAGE["pump_status"]="Stop"

            if message["sensorId"]=="192.168.1.31":
                data = self.plc.db_read(8,12, 1)
                blue_moving = util.get_bool(data, 0, 0)
                blue_ready_to_run = util.get_bool(data, 0, 3)
                if blue_moving == True:
                    MESSAGE["pump_status"]="Move"
                elif blue_ready_to_run == True:
                    MESSAGE["pump_status"]="Ready"
                else:
                    MESSAGE["pump_status"]="Stop"

            if message["sensorId"]=="192.168.1.32":
                data = self.plc.db_read(8,12, 1)
                brown_moving = util.get_bool(data, 0, 1)
                brown_ready_to_run = util.get_bool(data, 0, 4)
                if brown_moving == True:
                    MESSAGE["pump_status"]="Move"
                elif brown_ready_to_run == True:
                    MESSAGE["pump_status"]="Ready"
                else:
                    MESSAGE["pump_status"]="Stop"


            
            #MESSAGE["pump_status"] = self.plc.db_read(1,0, 10).decode()

            # MESSAGE["drum_loaded"]
            #if message["sensorId"]=="192.168.1.30":
            #    hem_flow_rate = self.plc.db_read(8, 8, 4)
            #    MESSAGE["volumer_transfer"] = util.get_real(hem_flow_rate,0)*60
            #if message["sensorId"]=="192.168.1.31":
            #    seam_flow_rate = self.plc.db_read(8, 0, 4)
            #    MESSAGE["volumer_transfer"] = util.get_real(seam_flow_rate,0)*60
            #if message["sensorId"]=="192.168.1.32":
            #    ubc_flow_rate = self.plc.db_read(8, 4, 4)
            #    MESSAGE["volumer_transfer"] = util.get_real(ubc_flow_rate,0)*60

            # MESSAGE["drum_level"]=
            drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y=None,None,None,None,None,None 
            for object in message['objects']:
                try:
                    if 'no_drum' in object:
                        MESSAGE["drum_status"]= ""
                    if ('blue_drum' in object) or ('brown_drum' in object) or ('grey_drum' in object) or ('Brown_Drum' in object):
                        MESSAGE["drum_status"]= "Present"
                        drum_x1,drum_y1,drum_x2,drum_y2=float(object.split('|')[1]),float(object.split('|')[2]),float(object.split('|')[3]),float(object.split('|')[4])
                        if self.is_centroid_in_middle_70(drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y):
                            MESSAGE['aligned'] = "Aligned"

                    if 'Label' in object:
                        MESSAGE["label_status"] = "Present"
                        label_x = (float(object.split('|')[1]) + float(object.split('|')[3])) / 2
                        label_y = (float(object.split('|')[2]) + float(object.split('|')[4])) / 2

                    if 'QR_Code' in object:
                        try:
                            qr_details = object.split('|')[6]
                            MESSAGE["product_name"]= ' '.join(qr_details.split()[:3])
                            MESSAGE["expiry_date"]= qr_details.split()[4][12:20]
                            MESSAGE["batch"]=qr_details.split()[4][:12]
                            MESSAGE["weight"]=qr_details.split()[3]

                            pump_code_from_message = qr_details.split()[2]
                            if datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
                                self.interlock = 3
                            if pump_code_from_message != str(self.pump_code):
                                self.interlock = 2
                            if (pump_code_from_message != str(self.pump_code) )and datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
                                self.interlock = 4

                            config_data["station"]=station_name[message["sensorId"]] 
                            config_data["timestamp"]=MESSAGE["time"]
                            config_data["config"]= {"product_code":qr_details.split()[2],
                                                    "drum_colour":"",
                                                    "camera_ip":message["sensorId"],
                                                    "pump_status":MESSAGE["pump_status"],
                                                    "drum_loaded_since":"",#self.plc.db_read(1,0,10).decode(),
                                                    "flow_rate":"",#self.plc.db_read(1,0,10).decode(),
                                                    "batch":MESSAGE["batch"],
                                                    "expiry_date":MESSAGE["expiry_date"],
                                                    "interlock": str(self.interlock),
                                                    "updated_at": str(datetime.now())
                                                    }
                        except:
                            print("Qr Not Detected")
                except Exception as e:
                    print(e)
                """
            print("MESSAGE:",MESSAGE)
            sk_history["date"]= MESSAGE["date"]
            sk_history["timestamp"]= MESSAGE["time"]
            sk_history["camera_ip"]= MESSAGE["camera_ip"]
            sk_history["station"]= station_name[MESSAGE["camera_ip"]]
            sk_history["drum_status"]=MESSAGE["drum_status"]
            sk_history["product_name"]=MESSAGE["product_name"]
            sk_history["expiry_date"]= MESSAGE["expiry_date"]
            sk_history["batch"]=MESSAGE["batch"]
            sk_history["weight"]=MESSAGE["weight"]
            sk_history["interlock"]= str(self.interlock)
            if MESSAGE["camera_ip"]=="192.168.1.30":
                grey_drum_status["drum_status"],grey_drum_status["alignment"],grey_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
            if MESSAGE["camera_ip"]=="192.168.1.31":
                blue_drum_status["drum_status"],blue_drum_status["alignment"],blue_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
            if MESSAGE["camera_ip"]=="192.168.1.32":
                brown_drum_status["drum_status"],brown_drum_status["alignment"],brown_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)

            self.update_config(config_data)
            self.send_message(MESSAGE)
            # self.write_plc(self.pump_code, MESSAGE["batch"], MESSAGE["expiry_date"], self.interlock)
            self.run_tower_light()
            try:
                self.write_plc(MESSAGE["camera_ip"], MESSAGE["product_name"].split()[2], MESSAGE["batch"], MESSAGE["expiry_date"], self.interlock)
            except Exception as e:
                print(e)
            if self.new_drum[MESSAGE["camera_ip"]] == False and MESSAGE["drum_status"]=="Present":
                self.new_drum[MESSAGE["camera_ip"]] = True
                print("new drum")
                self.insert_new_drum_data(sk_history)

            if self.new_drum[MESSAGE["camera_ip"]] == True and MESSAGE["drum_status"]=="":
                self.new_drum[MESSAGE["camera_ip"]] = False
                print("drum absent")
                    
            for key in MESSAGE:
                MESSAGE[key] = ""
            for key in sk_history:
                sk_history[key]=""
            config_data["station"],config_data["timestamp"],config_data["config"]='','',{}

                #except Exception as e:
                #    print(e)

            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print("Error processing message:", str(e))

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('localhost', credentials=credentials, heartbeat=5)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='myqueue', durable=True)
channel.basic_qos(prefetch_count=1)
#channel.basic_consume(queue='myqueue', on_message_callback=SEALER().rabbitmq_message())
channel.basic_consume(queue='myqueue', on_message_callback=SEALER().rabbitmq_message)
try:
    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

connection.close()


