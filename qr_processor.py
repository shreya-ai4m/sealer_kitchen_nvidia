import cv2
import numpy as np
from pyzbar.pyzbar import decode
from datetime import datetime
from shift_manager import ShiftManager
from PIL import Image
import logging

logging.basicConfig(
    level=logging.INFO,  # Set the log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log format
    handlers=[
        logging.FileHandler("logics.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

class QRProcessor:
    def __init__(self):
        self.qcd = cv2.QRCodeDetector()
    def update_message_no_qr(self, video_name, message):
        message["yolo_status"] = False
        message["drum_status"] = "Absent"
        message["label_status"] = "Absent"
        message["qr_code_present"] = False
        message["aligned"] = "Not Aligned"
        message["product_name"] = "--"
        message["weight"] = "--"
        message["batch"] = "--"
        message["expiry"] = "--"
        message["camera_ip"] = video_name
        message["current_shift"] = ShiftManager.check_shift()
        message["presence_status"] = False
        message["text"] = " "
        message["timestamp"] = datetime.now().isoformat()
        message = self.convert_numpy_types(message)
    def process_frame(self, frame, image_file, video_name, message):
        try:
           if frame is None or frame.size == 0:
              logging.error("Error: Empty frame passed to QR code detection.")
              self.update_message_no_qr(video_name, message)
              message['qr_code_present'] = False
              message['presence_status'] = False
              return False

           ret_qr, decoded_info, points, _ = self.qcd.detectAndDecodeMulti(frame)

           if ret_qr and decoded_info:
               if isinstance(decoded_info, tuple) and all(not info for info in decoded_info):
                    logging.info("QR detected but empty data. Skipping QR processing.")
                    #self.update_message_no_qr(video_name, message)
                    message['qr_code_present'] = False
                    message['presence_status'] = False
                    return False

               decoded_info = decoded_info[0] if isinstance(decoded_info, tuple) else decoded_info

               logging.info(f"QR Detected by OpenCV: {decoded_info}")
               message['qr_code_present'] = True
               message['presence_status'] = True
               self.annotate_and_send_qr_data(frame, decoded_info, points, "OpenCV", image_file, video_name, message)
               return True
           else:
                decoded_objects = decode(frame)
                if decoded_objects:
                    decoded_info = [obj.data.decode("utf-8") for obj in decoded_objects]
                    points = [obj.polygon for obj in decoded_objects]
                    
                    if all(not info for info in decoded_info):
                        logging.info("QR detected by PyZbar but empty data. Skipping QR processing.")
                        message['qr_code_present'] = False
                        message['presence_status'] = False
                        self.update_message_no_qr(video_name, message)
                        return False

                    message['qr_code_present'] = True
                    message['presence_status'] = True
                    self.annotate_and_send_qr_data(frame, decoded_info, points, "PyZbar", image_file, video_name, message)
                    return True
                else:
                    self.update_message_no_qr(video_name, message)
                    message['qr_code_present'] = False
                    message['presence_status'] = False
                    return False

        except Exception as e:
            logging.error(f"Error processing frame: {e}")
            self.update_message_no_qr(video_name, message)
            return False

    def annotate_and_send_qr_data(self, frame, decoded_info, points, qr_detected_by, image_file, video_name, message):
        current_shift = ShiftManager.check_shift()
        try:
            if isinstance(decoded_info, str):
                decoded_info = [decoded_info]

            for idx, (s, point) in enumerate(zip(decoded_info, points)):
                if s:
                    logging.info(f"QR code {idx} detected by {qr_detected_by}: {s}")
                    parts = s.split() if isinstance(s, str) else []

                    message["product_name"] = ' '.join(parts[:3]) if len(parts) >= 3 else "--"
                    message["weight"] = next((word for word in parts if "KG" in word), "--")
                    message['batch'] = parts[-1][:12] if len(parts[-1]) >= 12 else "--"
                    message["expiry"] = parts[-1][-8:] if len(parts[-1]) >= 8 else "--"
                    message["camera_ip"] = video_name
                    message["current_shift"] = current_shift
                    message["presence_status"] = True

                    text = f"{s}"
                    current_timestamp = datetime.now()

                    message["qr_code_present"] = True
                    message["text"] = text
                    message["timestamp"] = current_timestamp.isoformat()

                # Convert any numpy types (e.g., np.int64) to standard Python types
                    message = self.convert_numpy_types(message)
                    color = (0, 255, 0)
                    text_position = (int(point[0][0]), int(point[0][1]) - 10)
                    cv2.putText(frame, s, text_position, cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2)
                    frame = cv2.polylines(frame, [np.array(point, dtype=np.int32)], True, color, 8)

                else:
                    message['qr_code_present'] = False
                    message['presence_status'] = False
                    self.update_message_no_qr(video_name, message)
        except Exception as e:
            logging.error(f"Error annotating and sending QR data: {e}")
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
