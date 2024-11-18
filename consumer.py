import functools
import logging
import pika
from pika.exchange_type import ExchangeType
import psycopg2
from psycopg2.extras import Json
import json
from datetime import datetime,timezone,time
import cv2
from PIL import Image
import os
import snap7
from snap7.util import set_string, get_string
from snap7 import util
import zmq

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

shifts = {"I":{"start":time(6, 30),"end":time(15, 10)},"II":{"start":time(15, 10),"end":time(23, 50)},"III":{"start":time(23, 50),"end":time(6, 30)}}

MESSAGE = {
        "camera_ip":"",
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

brown_drum_status = {"drum_status":"","alignment":"","interlock":""}
blue_drum_status = {"drum_status":"","alignment":"","interlock":""}
grey_drum_status = {"drum_status":"","alignment":"","interlock":""}


class SEALER(object):
    def __init__(self):
        self.pump_code = None
        self.interlock = 1 #interlock = {1:"OKAY", 2:"CODE-MISMATCH", 3:"EXPIRED", 4:"CODE-MISMATCH & EXPIRED"}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://localhost:5555")
        self.brown_raw_insert_query = """INSERT INTO sealer_brown(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
        self.blue_raw_insert_query = """INSERT INTO sealer_blue(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
        self.grey_raw_insert_query = """INSERT INTO sealer_grey(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
        self.db_connection = """postgres://postgres:ai4m2024@localhost:5432/mahindra?sslmode=disable"""
        self.connection = psycopg2.connect(self.db_connection)
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
        """
        try:
            image = cv2.imread(image_path)
            gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            qr_code_detector = cv2.QRCodeDetector()
            decoded_text, points, _ = qr_code_detector.detectAndDecode(gray_image)
            if decoded_text:
                return decoded_text
        except Exception as e:
            print(f"OpenCV failed to read the QR code: {e}")
        """
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
            if shifts["I"]["start"] < current_shift < shifts["I"]["end"]:
                MESSAGE["current_shift"] = "I"
            if shifts["II"]["start"] < current_shift < shifts["II"]["end"]:
                MESSAGE["current_shift"] = "II"
            if (shifts["III"]["start"] <= current_shift) or (current_shift < shifts["III"]["end"]):
                MESSAGE["current_shift"] = "III"
        except Exception as e:
            print(e)


    def rawdata_insertion(self, camera_ip, data):
        try:
            cur = self.connection.cursor()
            if camera_ip == '172.168.16.205':
                insert_query = self.grey_raw_insert_query
            if camera_ip == '172.168.16.203':
                insert_query = self.blue_raw_insert_query
            if camera_ip == '172.168.16.201':
                insert_query = self.brown_raw_insert_query
            cur.execute(insert_query, [data])
            self.connection.commit()
            print('Raw Data Inserted')

        except Exception as e:
            self.connection.rollback()
            print("Exception: ", e)


    def is_centroid_in_middle_90(self,drum_x1, drum_y1, drum_x2, drum_y2, centroid_x, centroid_y):
        drum_width = drum_x2 - drum_x1
        drum_height = drum_y2 - drum_y1

        mid_70_x1 = drum_x1 + 0.05 * drum_width
        mid_70_x2 = drum_x2 - 0.05 * drum_width
        mid_70_y1 = drum_y1 + 0.05 * drum_height
        mid_70_y2 = drum_y2 - 0.05 * drum_height

        in_x_range = mid_70_x1 <= centroid_x <= mid_70_x2
        in_y_range = mid_70_y1 <= centroid_y <= mid_70_y2
        print("Lie in mid 90% area") 
        return in_x_range and in_y_range

    
    def check_pump_code(self,camera_ip):
        if camera_ip == "172.168.16.205":
            self.pump_code = 3097
        if camera_ip == "172.168.16.203":
            self.pump_code = 3098
        if camera_ip == "172.168.16.201":
            self.pump_code = 3090

    def check_pump_status(self,cameraip):
        if cameraip=="172.168.16.205":
            data = self.plc.db_read(8,12, 1)
            grey_moving = util.get_bool(data, 0, 2)
            grey_ready_to_run = util.get_bool(data, 0, 5)
            if grey_moving == True:
                MESSAGE["pump_status"]="Move"
            elif grey_ready_to_run == True:
                MESSAGE["pump_status"]="Ready"
            else:
                MESSAGE["pump_status"]="Stop"
        if cameraip=="172.168.16.203":
            data = self.plc.db_read(8,12, 1)
            blue_moving = util.get_bool(data, 0, 0)
            blue_ready_to_run = util.get_bool(data, 0, 3)
            if blue_moving == True:
                MESSAGE["pump_status"]="Move"
            elif blue_ready_to_run == True:
                MESSAGE["pump_status"]="Ready"
            else:
                MESSAGE["pump_status"]="Stop"
        if cameraip=="172.168.16.201":
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

    
    def write_plc(self, camera_ip, product_code, batch, expiry, interlock):
        try:
            if camera_ip == "172.168.16.201":
                buffer = bytearray(len(product_code)+2)
                set_string(buffer, 0,product_code, len(product_code)+2)
                self.plc.db_write(8,14,buffer)

                buffer = bytearray(len(batch)+2)
                set_string(buffer, 0, batch, len(batch)+2)
                self.plc.db_write(8,36,buffer)

                buffer = bytearray(len(expiry)+2)
                set_string(buffer, 0,expiry, len(expiry)+2)
                print(expiry,buffer)
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

                buffer = bytearray(len(expiry)+2)
                set_string(buffer, 0,expiry, len(expiry)+2)
                print(expiry,buffer)
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

                buffer = bytearray(len(expiry)+2)
                set_string(buffer, 0,expiry, len(expiry)+2)
                print(expiry,buffer)
                self.plc.db_write(8,196,buffer)

                data = bytearray(2)
                snap7.util.set_int(data,0,interlock)
                self.plc.db_write(8,84,data)
        except Exception as e:
            print(e)


    def on_message(self, chan, method_frame, header_frame, body, userdata=None):
        #print(body)
        message = json.loads(body.decode('utf-8'))
        print("Received message:", message)
        data = message
        camera_ip = data.get("sensorId")
        objects = data.get("objects", [])
        label_present = any('Label' in obj for obj in objects)
        qr_codes = [obj.split('|')[5] for obj in objects if 'QR_Code' in obj]
        qr_code_present = bool(qr_codes)
        qr_code_filenames = [obj.split('|')[6] for obj in objects if 'QR_Code' in obj]
        current_timestamp = datetime.now()
        text = ''
        try:
            text = qr_code_filenames[0]
            print("text: ",text)
            MESSAGE["product_name"]= ' '.join(text.split()[:3])
            MESSAGE["expiry_date"]= text.split()[4][12:20]
            MESSAGE["batch"]=text.split()[4][:12]
            MESSAGE["weight"]=text.split()[3]
            product_code = text.split()[2]
        except:
            print('QR filename not in data')
       
        presence_status = any('grey_drum' in obj or 'blue_drum' in obj or 'brown_drum' in obj for obj in objects)
        yolo_status = any('Grey_Drum' in obj or 'Blue_Drum' in obj or 'Brown_Drum' in obj for obj in objects)
        status_of_drum = yolo_status or presence_status
        data_row = (current_timestamp,status_of_drum,label_present,qr_code_present,text,presence_status,yolo_status, json.dumps(objects))
        self.rawdata_insertion(camera_ip,data_row)
        
        MESSAGE['aligned'] = "Not Aligned"
        if yolo_status and label_present:
            print("Checking label lie in mid 90% of drum... ")
            drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y=None,None,None,None,None,None     
            for obj in objects:
                if 'Grey_Drum' in obj or 'Blue_Drum' in obj or 'Brown_Drum' in obj:
                    drum_x1,drum_y1,drum_x2,drum_y2=float(obj.split('|')[1]),float(obj.split('|')[2]),float(obj.split('|')[3]),float(obj.split('|')[4])
                if 'Label' in obj:
                    label_x = (float(obj.split('|')[1]) + float(obj.split('|')[3])) / 2
                    label_y = (float(obj.split('|')[2]) + float(obj.split('|')[4])) / 2
            
            if self.is_centroid_in_middle_90(drum_x1,drum_y1,drum_x2,drum_y2,label_x,label_y):
                MESSAGE['aligned'] = "Aligned"     
        
        if label_present and qr_code_present:
            print("Checking qr lie in mid 90% of label...")
            label_x1,label_y1,label_x2,label_y2,qr_x,qr_y=None,None,None,None,None,None
            for obj in objects:
                if 'Label' in obj:
                    label_x1,label_y1,label_x2,label_y2=float(obj.split('|')[1]),float(obj.split('|')[2]),float(obj.split('|')[3]),float(obj.split('|')[4])
                if 'QR_Code' in obj:
                    qr_x = (float(obj.split('|')[1]) + float(obj.split('|')[3])) / 2
                    qr_y = (float(obj.split('|')[2]) + float(obj.split('|')[4])) / 2
            if self.is_centroid_in_middle_90(label_x1,label_y1,label_x2,label_y2,qr_x,qr_y):
                MESSAGE['aligned'] = "Aligned"
        
        MESSAGE["camera_ip"] = camera_ip
        self.check_shift()
        MESSAGE["drum_status"]='Present' if status_of_drum else 'Absent'
        MESSAGE["label_status"]='Present' if label_present else 'Absent'
        self.check_pump_code(camera_ip)
        self.check_pump_status(camera_ip)
        
        
        try:
            if datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
                self.interlock = 3
            if product_code != str(self.pump_code):
                self.interlock = 2
            if (product_code != str(self.pump_code) )and datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
                self.interlock = 4
            self.write_plc(camera_ip, product_code, MESSAGE["batch"], MESSAGE["expiry_date"], self.interlock)
            print("Data has been written in mvml PLC")
        except Exception as e:
            print(e)

        
        if camera_ip=="172.168.16.205":
            grey_drum_status["drum_status"],grey_drum_status["alignment"],grey_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
        if camera_ip=="172.168.16.203":
            blue_drum_status["drum_status"],blue_drum_status["alignment"],blue_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
        if camera_ip=="172.168.16.201":
            brown_drum_status["drum_status"],brown_drum_status["alignment"],brown_drum_status["interlock"] = MESSAGE["drum_status"],MESSAGE['aligned'],str(self.interlock)
        
        
        
        self.socket.send_json(MESSAGE) 
        self.run_tower_light()
        print("message sent on zmq: ",MESSAGE)
        for key in MESSAGE:
            MESSAGE[key] = ""
        self.pump_code = None
        self.interlock = 1

        chan.basic_ack(delivery_tag=method_frame.delivery_tag)


def main():
    """Main method."""
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', credentials=credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.queue_declare(queue='myqueue', durable=True)
    channel.basic_qos(prefetch_count=1)

    sealer_instance = SEALER()
    on_message_callback = functools.partial(
        sealer_instance.on_message, userdata='on_message_userdata')
    channel.basic_consume('myqueue', on_message_callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    connection.close()


if __name__ == '__main__':
    main()


