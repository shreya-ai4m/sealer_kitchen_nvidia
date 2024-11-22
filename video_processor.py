import numpy as np
import tritonclient.grpc as grpcclient
from vidgear.gears import VideoGear
from datetime import datetime
from processing import preprocess, postprocess
from labels import COCOLabels
from utils import handle_interlocks, assign_drum_status
from PIL import Image
from config import *

import logging

logging.basicConfig(
    level=logging.INFO,  # Set the log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log format
    handlers=[
        logging.FileHandler("logics.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)


class VideoProcessor:
    def __init__(self, triton_url, model_name, target_labels):
        self.triton_client = grpcclient.InferenceServerClient(url=triton_url, verbose=False)
        self.model_name = model_name
        self.target_labels = target_labels

        if not self.triton_client.is_server_ready() or not self.triton_client.is_model_ready(model_name):
            logging.error("Triton server or model is not ready")
            raise Exception("Triton server or model is not ready")

    def convert_numpy_types(self, data):
        """Converts numpy types to native Python types for JSON serialization."""
        if isinstance(data, dict):
            return {key: self.convert_numpy_types(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_numpy_types(item) for item in data]
        elif isinstance(data, np.integer):
            return int(data)
        elif isinstance(data, np.floating):
            return float(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        else:
            return data
    def is_centroid_in_middle_90(self, drum_x1, drum_y1, drum_x2, drum_y2, centroid_x, centroid_y):
        drum_width = drum_x2 - drum_x1
        drum_height = drum_y2 - drum_y1

        mid_90_x1 = drum_x1 + 0.05 * drum_width
        mid_90_x2 = drum_x2 - 0.05 * drum_width
        mid_90_y1 = drum_y1 + 0.05 * drum_height
        mid_90_y2 = drum_y2 - 0.05 * drum_height

        in_x_range = mid_90_x1 <= centroid_x <= mid_90_x2
        in_y_range = mid_90_y1 <= centroid_y <= mid_90_y2

        return in_x_range and in_y_range

    def process_video(self, video_path, qr_processor, plc_manager, db_manager, message):
        options = {"CAP_PROP_FPS": 1}
        stream = VideoGear(source=video_path, **options).start()
        #stream = VideoGear(source=video_path).start()
        start_time = datetime.now()
        frame_count = 0
        try:
            while True:
                frame = stream.read()
                frame_count += 1

                if frame is None:
                    break
                try:
                    input_image_buffer = preprocess(frame, [INPUT_WIDTH, INPUT_HEIGHT])
                    input_image_buffer = np.expand_dims(input_image_buffer, axis=0)

                    inputs = [grpcclient.InferInput("images", [1, 3, INPUT_WIDTH, INPUT_HEIGHT], "FP32")]
                    inputs[0].set_data_from_numpy(input_image_buffer)
                    outputs = [grpcclient.InferRequestedOutput(name) for name in ["num_dets", "det_boxes", "det_scores", "det_classes"]]

                    results = self.triton_client.infer(model_name=self.model_name, inputs=inputs, outputs=outputs)
                    num_dets = results.as_numpy("num_dets")
                    det_boxes = results.as_numpy("det_boxes")
                    det_scores = results.as_numpy("det_scores")
                    det_classes = results.as_numpy("det_classes")

                    detected_objects = postprocess(num_dets, det_boxes, det_scores, det_classes, frame.shape[1], frame.shape[0], [INPUT_WIDTH, INPUT_HEIGHT])
                    self.handle_detected_objects(detected_objects, frame, video_path, qr_processor, plc_manager, db_manager, message)

                    stream.stop()  # Stop video stream once done
                except Exception as e:
                    logging.error("Error during inference: %s", e)
        except Exception as e:
            logging.error("An error occurred during video processing: %s", e)
    
    def handle_detected_objects(self, detected_objects, frame, video_name, qr_processor, plc_manager, db_manager, message):
        detected_labels = set()
        yolo_status = False  # Initialize yolo_status as False
        message['objects'] = []
        try:
            if not detected_objects:
                logging.info("No objects detected in the stream from %s", video_name)
                message["yolo_status"] = False
                message["drum_status"] = "Absent"
                message["label_status"] = "Absent"
                message["qr_code_present"] = False
                message["presence_status"] = False
                message["aligned"] = "Not Aligned"
                message["product_name"] = "--"
                message["weight"] = "--"
                message['batch'] = "--"
                message["expiry"] = "--"
                message["camera_ip"] = video_name
                message["current_shift"] = ShiftManager.check_shift()
                message["presence_status"] = False
                text = " "
                current_timestamp = datetime.now()

                message["qr_code_present"] = False
                message["text"] = text
                message["timestamp"] = current_timestamp.isoformat()
                message = self.convert_numpy_types(message)

        # Even without detected objects, check pump status and send the message
                plc_manager.check_pump_status(video_name, message)

        # Log the message or insert into the database to record the event
                db_manager.insert_data(video_name, message)
                return

            for box in detected_objects:
                boxes = box.box()
                class_id = box.classID
                label = COCOLabels(class_id).name
                detected_labels.add(label)
                a1, b1, a2, b2 = box.box()
                if a2 > a1 and b2 > b1:
                    mask = np.zeros_like(frame)
                    mask[b1:b2, a1:a2] = frame[b1:b2, a1:a2]
                    cropped_ocr = mask
                    if cropped_ocr is None or cropped_ocr.size == 0:
                        logging.debug("Skipping empty cropped image.")
                        continue  # Skip if the cropped image is empty
                centroid_x = (box.x1 + box.x2) / 2
                centroid_y = (box.y1 + box.y2) / 2

                object_data = {
                    "class": label,
                    "bbox": [box.x1, box.y1, box.x2, box.y2]  # Bounding box
                }
                message['objects'].append(object_data)  # Append detected object info to the message
                if label == "LABEL" : #or label == "QRCODE":
                    height, width, _ = frame.shape
                    y1, y2 = max(0, box.y1), min(height, box.y2)
                    x1, x2 = max(0, box.x1), min(width, box.x2)

    # Crop the image
                    cropped_images = frame[y1:y2, x1:x2]

    # Ensure the cropped image is not empty
                    if cropped_images.shape[0] > 0 and cropped_images.shape[1] > 0:
                         mask = np.zeros_like(frame)
                         mask[y1:y2, x1:x2] = frame[y1:y2, x1:x2]
                         cropped_images = mask
                    else:
                         logging.debug("Empty cropped image. Skipping...")
                if class_id == 0 or class_id == 1 or class_id == 2:  # Assuming 0, 1, 2 are the class IDs for drums, labels, etc.
                    if self.is_centroid_in_middle_90(box.x1, box.y1, box.x2, box.y2, centroid_x, centroid_y):
                        message['aligned'] = "Aligned"
                        logging.info("%s is Aligned.", label)
                    else:
                        message['aligned'] = "Not Aligned"
                        logging.info("%s is Not Aligned.", label)
                qr_detected = False
                try:
                    if label == "LABEL":
                        qr_detected = qr_processor.process_frame(cropped_images, label, video_name, message)
                        #qr_detected = qr_processor.process_frame(cropped_ocr, label, video_name, message)
                        if qr_detected:
                            logging.info("QR code detected.")
                            if message['product_name'] != "--":
                                logging.info("Product name is valid. No further action needed.")
                            else:
                                logging.info("Product name is '--'. Proceeding to OCR.")
                                message["label_status"] = "Present"
                                yolo_status = True  # Set yolo_status True when label is detected
                                #if message['product_name'] == "--":
                                    #qr_processor.process_frame(cropped_ocr, label, video_name, message)
                                    #yolo_status = True  # Set yolo_status True when drum is detected
                                    #print("==============================================================================")

                        else:
                           logging.info("No QR code detected. Proceeding to OCR.")
                           message["label_status"] = "Absent"
                        #if message['product_name'] == "--":
                            #qr_processor.process_frame(cropped_ocr, label, video_name, message)
                            #qr_processor.process_frame(cropped_images, label, video_name, message)
                    else:
                       #if message['product_name'] == "--":                       # Only process LABEL if QR code is not detected or product name is "--"
                       #if not qr_detected and message['product_name'] == "--":
                           #qr_processor.process_frame(cropped_ocr, label, video_name, message)
                           yolo_status = True  # Set yolo_status True when drum is detected
                           print("==============================================================================")
                except Exception as e:
                    logging.error("Error processing QR code: %s", e)

            logging.info("Message after handling detected objects: %s", message)

            if 'timestamp' not in message:
                message['timestamp'] = datetime.now().isoformat()
            message['yolo_status'] = yolo_status
            status_of_drum = 'DRUM' in detected_labels  # Drum presence based on detection
            label_present = 'LABEL' in detected_labels  # Label presence based on detection
            message["drum_status"] = 'Present' if status_of_drum else 'Absent'
            message["label_status"] = 'Present' if label_present else 'Absent'
            try:
               plc_manager.check_pump_status(video_name, message)
               db_manager.insert_data(message["camera_ip"], message)
            except Exception as e:
                    logging.error("Error checking pump status or inserting data: %s", e)
        except Exception as e:
                logging.error("An error occurred while handling detected objects: %s", e)