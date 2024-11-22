import sys
import os
import logging


sys.path.append(os.path.join(os.path.dirname(__file__), 'ocr'))

from ocr.config import *
from ocr.database_manager import DatabaseManager
from ocr.plc_manager import PLCManager
from ocr.qr_processor import QRProcessor
from ocr.video_processor import VideoProcessor
from ocr.shift_manager import ShiftManager
from ocr.utils import handle_interlocks, assign_drum_status  # Add this import
import signal
import threading
from threading import Thread
import time

logging.basicConfig(
    level=logging.INFO,  # Set the log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log format
    handlers=[
        logging.FileHandler("main.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)


class SealerApplication:
    def __init__(self):
        logging.info("SealerApplication initialized.")
        self.db_manager = DatabaseManager(DB_CONNECTION)
        self.plc_manager = PLCManager(PLC_IP, 0, 0)
        self.qr_processor = QRProcessor()
        self.video_processor = VideoProcessor(TRITON_URL, MODEL_NAME, TARGET_LABELS)
        self.message = {
            "camera_ip": "",
            "current_shift": "",
            "drum_status": "",   # Drum status field
            "label_status": "",  # Label status field
            "pump_status": "",   # Pump status field
            "aligned": "",       # Alignment status field
            "drum_loaded": "",
            "volume_transfer": "",
            "drum_level": "",
            "aligned": "",
            "product_name": "",
            "expiry": "",
            "batch": "",
            "weight": "",
            "station": "",
            "yolo_status": False,  # Initialize yolo_status as False in the message
            "objects": []  # Initialize objects as an empty list in the message
        }
        self.running = True  # Control flag for graceful exit
        self.processed_streams = 0  # Counter for processed streams
        self.total_streams = len(VIDEO_PATHS)  # Total number of streams

        # Lock for thread-safe updates to processed_streams
        self.lock = threading.Lock()

    def run(self):
        self.message["current_shift"] = ShiftManager.check_shift()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        threads = []

        for video_path in VIDEO_PATHS[:3]:
            self.message["camera_ip"] = video_path
            logging.info(f"Starting thread to process video: {video_path}")
            thread = Thread(target=self.process_video_thread, args=(video_path,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        logging.info("All video processing completed.")

    def process_video_thread(self, video_path):
        while self.running:
            # Call the PLCManager's method to update the pump status in the message
            self.plc_manager.check_pump_status(video_path, self.message)
            # Handle interlocks based on the camera and message data
            interlock = handle_interlocks(video_path, self.message)

            # Assign drum status after interlock handling
            assign_drum_status(video_path, interlock, self.message)
            # Continue processing video after pump status is updated
            self.video_processor.process_video(video_path, self.qr_processor, self.plc_manager, self.db_manager, self.message)
            logging.info(f"Updated message after processing video from {video_path}: {self.message}")

            #break
    def signal_handler(self, sig, frame):
        logging.info("Graceful shutdown initiated...")
        self.running = False

        # Give some time to threads to complete
        time.sleep(5)
        sys.exit(0)
if __name__ == '__main__':
    app = SealerApplication()
    app.run()

