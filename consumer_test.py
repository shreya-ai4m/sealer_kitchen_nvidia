import numpy as np
import sys
import cv2
import tritonclient.grpc as grpcclient
from processing import preprocess, postprocess
from render import render_box, render_filled_box, get_text_size, render_text, RAND_COLORS
from labels import COCOLabels
import os
import base64
import zmq
import time as t
from pyzbar.pyzbar import decode
import json
from PIL import Image
from paddleocr import PaddleOCR
from threading import Thread
from vidgear.gears import VideoGear
import re
from datetime import datetime, timezone, time

# import snap7
output_folder = os.path.join(os.getcwd(), "output_images")
#zmq_address = "tcp://127.0.0.1:5560"
pump_code = None
interlock = 1  # interlock = {1:"OKAY", 2:"CODE-MISMATCH", 3:"EXPIRED", 4:"CODE-MISMATCH & EXPIRED"}
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://localhost:5555")
brown_raw_insert_query = """INSERT INTO sealer_brown(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
blue_raw_insert_query = """INSERT INTO sealer_blue(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
grey_raw_insert_query = """INSERT INTO sealer_grey(timestamp, status_of_drum, label, qrcode, text, presence_status, yolo_status, objects) VALUES %s"""
db_connection = """postgres://postgres:ai4m2024@localhost:5432/mahindra?sslmode=disable"""
connection = psycopg2.connect(db_connection)
plc = snap7.client.Client()
internal_plc = snap7.client.Client()
try:
    plc.connect('192.168.0.70', 0, 0)
    plc.get_connected()
except Exception as e:
    print(e)
try:
    internal_plc.connect('192.168.1.21', 0, 1)
    print("Internal_plc_connected:", internal_plc.get_connected())
except Exception as e:
    print(e)

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
    "weight": ""
}
# Define shift timings
shifts = {
    "I": {"start": time(6, 30), "end": time(15, 10)},
    "II": {"start": time(15, 10), "end": time(23, 50)},
    "III": {"start": time(23, 50), "end": time(6, 30)}
}


# Function to check the current shift
def check_shift():
    try:
        current_shift_time = datetime.now().time()
        if shifts["I"]["start"] < current_shift_time < shifts["I"]["end"]:
            return "I"
        if shifts["II"]["start"] < current_shift_time < shifts["II"]["end"]:
            return "II"
        if (shifts["III"]["start"] <= current_shift_time) or (current_shift_time < shifts["III"]["end"]):
            return "III"
    except Exception as e:
        print(e)
        return "Unknown"


def rawdata_insertion(camera_ip, data):
    try:
        cur = connection.cursor()
        insert_query = None
        if camera_ip == 'rtsp://admin:admin@172.168.16.205:554/unicaststream/2':
            insert_query = grey_raw_insert_query
        if camera_ip == 'rtsp://admin:admin@172.168.16.203:554/unicaststream/2':
            insert_query = blue_raw_insert_query
        if camera_ip == 'rtsp://admin:admin@172.168.16.201:554/unicaststream/2':
            insert_query = brown_raw_insert_query
        cur.execute(insert_query, [data])
        connection.commit()
        print('Raw Data Inserted')

    except Exception as e:
        connection.rollback()
        print("Exception: ", e)


def check_pump_code(camera_ip):
    if camera_ip == "rtsp://admin:admin@172.168.16.205:554/unicaststream/2":
        pump_code = 3097
    if camera_ip == "rtsp://admin:admin@172.168.16.203:554/unicaststream/2":
        pump_code = 3098
    if camera_ip == "rtsp://admin:admin@172.168.16.201:554/unicaststream/2":
        pump_code = 3090


def check_pump_status(cameraip):
    if cameraip == "172.168.16.205":
        data = plc.db_read(8, 12, 1)
        grey_moving = util.get_bool(data, 0, 2)
        grey_ready_to_run = util.get_bool(data, 0, 5)
        if grey_moving == True:
            MESSAGE["pump_status"] = "Move"
        elif grey_ready_to_run == True:
            MESSAGE["pump_status"] = "Ready"
        else:
            MESSAGE["pump_status"] = "Stop"
    if cameraip == "172.168.16.203":
        data = plc.db_read(8, 12, 1)
        blue_moving = util.get_bool(data, 0, 0)
        blue_ready_to_run = util.get_bool(data, 0, 3)
        if blue_moving == True:
            MESSAGE["pump_status"] = "Move"
        elif blue_ready_to_run == True:
            MESSAGE["pump_status"] = "Ready"
        else:
            MESSAGE["pump_status"] = "Stop"
    if cameraip == "172.168.16.201":
        data = plc.db_read(8, 12, 1)
        brown_moving = util.get_bool(data, 0, 1)
        brown_ready_to_run = util.get_bool(data, 0, 4)
        if brown_moving == True:
            MESSAGE["pump_status"] = "Move"
        elif brown_ready_to_run == True:
            MESSAGE["pump_status"] = "Ready"
        else:
            MESSAGE["pump_status"] = "Stop"


def write_ascii_string(data, string, length):
    string_bytes = string.encode('ascii')
    string_bytes += b' ' * (length - len(string_bytes))
    data[:length] = string_bytes


def run_tower_light():
    try:
        data = bytearray(1)
        if (brown_drum_status["drum_status"] == "") or (
                brown_drum_status["drum_status"] == "Present" and brown_drum_status["alignment"] == "Not Aligned"):
            snap7.util.set_bool(data, 0, 5, True)

        if brown_drum_status["alignment"] == "Aligned" and brown_drum_status["interlock"] == "1":
            snap7.util.set_bool(data, 0, 6, True)

        # if brown_drum_status["alignment"]=="Aligned" and (brown_drum_status["interlock"]=="2" or brown_drum_status["interlock"]=="3" or brown_drum_status["interlock"]=="4"):
        # snap7.util.set_bool(data,0,4,True)

        if (blue_drum_status["drum_status"] == "") or (
                blue_drum_status["drum_status"] == "Present" and blue_drum_status["alignment"] == "Not Aligned"):
            snap7.util.set_bool(data, 0, 2, True)

        if blue_drum_status["alignment"] == "Aligned" and blue_drum_status["interlock"] == "1":
            snap7.util.set_bool(data, 0, 3, True)

        # if blue_drum_status["alignment"]=="Aligned" and (blue_drum_status["interlock"]=="2" or blue_drum_status["interlock"]=="3" or blue_drum_status["interlock"]=="4"):
        # snap7.util.set_bool(data,0,1,True)

        if grey_drum_status["alignment"] == "Aligned" and grey_drum_status["interlock"] == "1":
            snap7.util.set_bool(data, 0, 0, True)

        internal_plc.db_write(1, 528, data)
        snap7.util.set_bool(data, 0, 6, False)
        # if grey_drum_status["alignment"]=="Aligned" and (grey_drum_status["interlock"]=="2" or grey_drum_status["interlock"]=="3" or grey_drum_status["interlock"]=="4"):
        # snap7.util.set_bool(data,0,6,True)

        if (grey_drum_status["drum_status"] == "") or (
                grey_drum_status["drum_status"] == "Present" and grey_drum_status["alignment"] == "Not Aligned"):
            snap7.util.set_bool(data, 0, 7, True)

        internal_plc.db_write(1, 527, data)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


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
            print(expiry, buffer)
            plc.db_write(8, 58, buffer)

            data = bytearray(2)
            snap7.util.set_int(data, 0, interlock)
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
            print(expiry, buffer)
            plc.db_write(8, 130, buffer)

            data = bytearray(2)
            snap7.util.set_int(data, 0, interlock)
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
            print(expiry, buffer)
            plc.db_write(8, 196, buffer)

            data = bytearray(2)
            snap7.util.set_int(data, 0, interlock)
            plc.db_write(8, 84, data)
    except Exception as e:
        print(e)


MESSAGE["camera_ip"] = camera_ip
MESSAGE["current_shift"] = check_shift()
MESSAGE["drum_status"] = 'Present' if status_of_drum else 'Absent'
MESSAGE["label_status"] = 'Present' if label_present else 'Absent'
check_pump_code(camera_ip)
check_pump_status(camera_ip)

try:
    if datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
        interlock = 3
    if product_code != str(pump_code):
        interlock = 2
    if (product_code != str(pump_code)) and datetime.strptime(MESSAGE["expiry_date"], "%d/%m/%y") < datetime.today():
        interlock = 4
    write_plc(camera_ip, product_code, MESSAGE["batch"], MESSAGE["expiry_date"], interlock)
    print("Data has been written in mvml PLC")
except Exception as e:
    print(e)

if camera_ip == "172.168.16.205":
    grey_drum_status["drum_status"], grey_drum_status["alignment"], grey_drum_status["interlock"] = MESSAGE[
        "drum_status"], MESSAGE['aligned'], str(interlock)
if camera_ip == "172.168.16.203":
    blue_drum_status["drum_status"], blue_drum_status["alignment"], blue_drum_status["interlock"] = MESSAGE[
        "drum_status"], MESSAGE['aligned'], str(interlock)
if camera_ip == "172.168.16.201":
    brown_drum_status["drum_status"], brown_drum_status["alignment"], brown_drum_status["interlock"] = MESSAGE[
        "drum_status"], MESSAGE['aligned'], str(interlock)


socket.send_json(MESSAGE)
run_tower_light()
print("message sent on zmq: ", MESSAGE)
for key in MESSAGE:
    MESSAGE[key] = ""
    pump_code = None
    interlock = 1
# Initialize QR code detector
qcd = cv2.QRCodeDetector()

# Initialize ZeroMQ context and socket
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind(zmq_address)

INPUT_NAMES = ["images"]
OUTPUT_NAMES = ["num_dets", "det_boxes", "det_scores", "det_classes"]

# Hardcoded values
VIDEO_PATHS = [
    # 'rtsp://admin:admin@172.168.16.201:554/unicaststream/2',
    # 'rtsp://admin:admin@172.168.16.203:554/unicaststream/2',
    # 'rtsp://admin:admin@172.168.16.205:554/unicaststream/2'
    '/home/ubuntu/sealer_kitchen/config_files/007.mp4'
    # '/home/ubuntu/sealer_kitchen/config_files/006.mp4'
    # '/home/ubuntu/sealer_kitchen/config_files/005.mp4'
]
MODEL_NAME = 'sealer_model'
TRITON_URL = 'localhost:8001'
INPUT_WIDTH = 640
INPUT_HEIGHT = 640
TARGET_LABELS = ['DRUM', 'LABEL', 'QRCODE']


def get_class_id_from_label(label):
    for i, c in enumerate(COCOLabels):
        if c.name == label:
            return i
    return None


def process_frame(frame, image_file, video_name):
    """Detect QR codes using both OpenCV and PyZbar, then annotate and send the data."""
    ret_qr, decoded_info, points, _ = qcd.detectAndDecodeMulti(frame)
    qr_detected_by = None

    if ret_qr:
        qr_detected_by = "OpenCV"
        annotate_and_send_qr_data(frame, decoded_info, points, qr_detected_by, image_file, video_name)
    else:
        # Fallback to PyZbar if OpenCV detection fails
        decoded_objects = decode(frame)
        if decoded_objects:
            qr_detected_by = "PyZbar"
            decoded_info = [obj.data.decode("utf-8") for obj in decoded_objects]
            points = [obj.polygon for obj in decoded_objects]
            annotate_and_send_qr_data(frame, decoded_info, points, qr_detected_by, image_file, video_name)


def extract_info(text):
    # Clean up and normalize text
    text = re.sub(r'\s+', ' ', text.replace('.', ' ').strip())

    # Define patterns for extraction
    product_name_pattern = r'(TEROSON\s*TER0S0N\s*PV\s*\d+)'
    weight_pattern = r'(\d+KG)'
    batch_pattern = r'\b(IN\d+|ME\d+)\b'
    date_pattern = r'\d{2}/\d{2}/\d{2}'  # Pattern to match dates in the format dd/mm/yy

    # Extract information using regex
    product_name = re.search(product_name_pattern, text)
    weight = re.search(weight_pattern, text)
    batch = re.search(batch_pattern, text)
    dates = re.findall(date_pattern, text)  # Find all dates

    # Determine the fixed date (e.g., the second date in the list)
    fixed_date = dates[1] if len(dates) > 1 else "Not Found"

    # Extract matched values or set default
    extracted_info = {
        "Product Name": product_name.group(0) if product_name else "Not Found",
        "Weight": weight.group(0) if weight else "Not Found",
        "Batch": batch.group(0) if batch else "Not Found",
        "Expiry Date": fixed_date
    }

    return extracted_info


def OCR_QR(frame, image_file, video_name):
    # Initialize PaddleOCR
    ocr = PaddleOCR()

    # Perform OCR using PaddleOCR on the current frame
    result = ocr.ocr(frame)

    # Check if result is not None and contains text
    if result is not None and isinstance(result, list):
        # Extract text from the results
        detected_text = ' '.join([word[1][0] for line in result for word in line])
        print(f"OCR Text from {video_name}: {detected_text}")
        print("==========================================================================================")

        # Extract relevant info (product name, weight, batch, expiry) from the detected text
        extracted_info = extract_info(detected_text)

        # Print extracted information
        MESSAGE["camera_ip"]={video_name}
        MESSAGE["product_name"]={extracted_info['Product Name']}
        MESSAGE["weight"]={extracted_info['Weight']}
        MESSAGE["batch"]={extracted_info['Batch']}
        MESSAGE["expiry_date"]={extracted_info['Expiry Date']}
        MESSAGE["camera_ip"]=video_name
        print("==========================================================================================")
        MESSAGE["drum_status"] = status_of_drum
        MESSAGE["label_status"] = label_present
        qr_code_present = False
        text = detected_text
        presence_status = True
        yolo_status = True
        current_timestamp = datetime.now()
        current_timestamp = datetime.now()
        (current_timestamp, status_of_drum, label_present, qr_code_present, text, presence_status, yolo_status,
         json.dumps(objects))
        rawdata_insertion(camera_ip, data_row)
    else:
        print(f"No text detected by OCR in {video_name}.")
        qr_code_present = True
        text = ""
        presence_status = False
        yolo_status = False
        current_timestamp = datetime.now()
        current_timestamp = datetime.now()
        (current_timestamp, status_of_drum, label_present, qr_code_present, text, presence_status, yolo_status,
         json.dumps(objects))
        rawdata_insertion(camera_ip, data_row)

def annotate_and_send_qr_data(frame, decoded_info, points, qr_detected_by, image_file, video_name):
    """Annotate the frame with QR code data and send it via ZeroMQ."""
    current_shift = check_shift()  # Check the current shift

    for idx, (s, point) in enumerate(zip(decoded_info, points)):
        if s:
            print(f"QR code {idx} detected by {qr_detected_by}: {s}")
            parts = s.split()
            MESSAGE["product_name"] = ' '.join(parts[:3])
            MESSAGE["weight"] = next((word for word in parts if "KG" in word), None)
            MESSAGE['batch'] = parts[-1][2:12]
            MESSAGE["expiry"] = parts[-1][-8:]
            MESSAGE["camera_ip"] = video_name
            MESSAGE["drum_status"] = status_of_drum
            MESSAGE["label_status"] = label_present
            qr_code_present = True
            text = f"{s}"
            presence_status = True
            yolo_status = True
            current_timestamp = datetime.now()
            (current_timestamp, status_of_drum, label_present, qr_code_present, text, presence_status, yolo_status,
             json.dumps(objects))
            rawdata_insertion(camera_ip, data_row)
            try:
                _, buffer = cv2.imencode('.jpg', frame)
                image_b64 = base64.b64encode(buffer).decode()
            except Exception as e:
                print(f"Error encoding image: {e}")
                continue

            # socket.send_json(json_data)
            # print(json.dumps(json_data, indent=4))

            color = (0, 255, 0)
            text_position = (int(point[0][0]), int(point[0][1]) - 10)
            cv2.putText(frame, s, text_position, cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2)
            if qr_detected_by == "OpenCV":
                frame = cv2.polylines(frame, [np.array(point, dtype=np.int32)], True, color, 8)
            else:  # PyZbar
                xmi = min(point, key=lambda x: x[0])[0]
                xma = max(point, key=lambda x: x[0])[0]
                ymi = min(point, key=lambda x: x[1])[1]
                yma = max(point, key=lambda x: x[1])[1]
                cv2.rectangle(frame, (xmi, ymi), (xma, yma), color, 8)
        else:
            qr_code_present = True
            text = ""
            presence_status = True
            yolo_status = True
            current_timestamp = datetime.now()
            (current_timestamp, status_of_drum, label_present, qr_code_present, text, presence_status, yolo_status,
             json.dumps(objects))
            rawdata_insertion(camera_ip, data_row)


        chan.basic_ack(delivery_tag=method_frame.delivery_tag)

def process_video(video_path):
    """Process a single video file."""
    try:
        triton_client = grpcclient.InferenceServerClient(
            url=TRITON_URL,
            verbose=False)
    except Exception as e:
        print("Context creation failed: " + str(e))
        sys.exit()

    if not triton_client.is_server_ready():
        print("Server is not ready")
        sys.exit(1)

    if not triton_client.is_model_ready(MODEL_NAME):
        print("Model is not ready")
        sys.exit(1)

    # Initialize the VideoGear stream
    stream = VideoGear(source=video_path).start()

    target_class_ids = {get_class_id_from_label(label) for label in TARGET_LABELS}
    if None in target_class_ids:
        print(f"One or more labels from '{TARGET_LABELS}' not found in COCOLabels.")
        sys.exit(1)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    while True:
        frame = stream.read()
        if frame is None:
            break

        input_image_buffer = preprocess(frame, [INPUT_WIDTH, INPUT_HEIGHT])
        input_image_buffer = np.expand_dims(input_image_buffer, axis=0)

        inputs = [grpcclient.InferInput(INPUT_NAMES[0], [1, 3, INPUT_WIDTH, INPUT_HEIGHT], "FP32")]
        inputs[0].set_data_from_numpy(input_image_buffer)

        outputs = [grpcclient.InferRequestedOutput(name) for name in OUTPUT_NAMES]

        results = triton_client.infer(model_name=MODEL_NAME,
                                      inputs=inputs,
                                      outputs=outputs)

        num_dets = results.as_numpy("num_dets")
        det_boxes = results.as_numpy("det_boxes")
        det_scores = results.as_numpy("det_scores")
        det_classes = results.as_numpy("det_classes")

        detected_objects = postprocess(num_dets, det_boxes, det_scores, det_classes, frame.shape[1], frame.shape[0],
                                       [INPUT_WIDTH, INPUT_HEIGHT])
        detected_labels = set()
        MESSAGE["label_status"] = False
        MESSAGE["drum_status"] = False
        for box in detected_objects:
            class_id = box.classID
            print(class_id)
            if class_id in target_class_ids:
                label = COCOLabels(class_id).name
                detected_labels.add(label)
                print(f"Detected: {label}")
                if class_id == 1:
                    MESSAGE["label_status"] = True
                    timestamp = int(t.time())
                    x1, y1, x2, y2 = box.box()
                    cropped_img = frame[int(y1):int(y2), int(x1):int(x2)]
                    # process_frame(cropped_img, label, video_path)
                    OCR_QR(cropped_img, label, video_path)
                if class_id == 2:
                    MESSAGE["drum_status"] = True
                    timestamp = int(t.time())
                    x1, y1, x2, y2 = box.box()
                    cropped_img = frame[int(y1):int(y2), int(x1):int(x2)]
                    process_frame(cropped_img, label, video_path)
            MESSAGE['aligned'] = "Not Aligned"
            if all(label in detected_labels for label in TARGET_LABELS):
                print("Aligned all three labels (DRUM, LABEL, QRCODE) detected in this frame.")
                # process_frame(cropped_img, label, video_path)
                MESSAGE['aligned'] = "Aligned"

            else:
                print("NOT Aligned ")
                MESSAGE['aligned'] = "Not Aligned"
                stream.stop()
                print(f"Processing completed for {video_path}.")
if __name__ == '__main__':
    threads = []
    for video_path in VIDEO_PATHS:
        thread = Thread(target=process_video, args=(video_path,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
        print("All video processing completed.")
