import snap7
from snap7.util import set_string, set_int
import time
from config import INTERNAL_PLC_IP
from utils import write_plc, check_pump_code  # Add this import
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logics.log'),
        logging.StreamHandler()
    ]
)

class PLCManager:
    def __init__(self, plc_ip, rack, slot, max_retries=5, retry_delay=1):
        self.plc = snap7.client.Client()
        self.plc_ip = plc_ip
        self.rack = rack
        self.slot = slot
        self.max_retries = max_retries  # Max retry attempts
        self.retry_delay = retry_delay  # Delay between retries in seconds
        self.internal_plc = snap7.client.Client()
        self.connect()

    def connect(self):
        try:
            self.plc.connect(self.plc_ip, self.rack, self.slot)
            logging.info(f"PLC connected: {self.plc.get_connected()}")
        except Exception as e:
            logging.error(f"Error connecting to PLC: {e}")
        try:
            self.internal_plc.connect(INTERNAL_PLC_IP, 0, 1)
            logging.info(f"Internal PLC connected: {self.internal_plc.get_connected()}")
        except Exception as e:
            logging.error(f"Error connecting to internal PLC: {e}")

    def read_data(self, db_number, start, size):
        """Retry mechanism to read PLC data with retries and delays."""
        attempt = 0
        while attempt < self.max_retries:
            try:
                data = self.plc.db_read(db_number, start, size)
                if data is not None:
                    return data
                else:
                    logging.warning(f"Attempt {attempt + 1}: No data returned from PLC read.")
            except Exception as e:
                logging.error(f"Error reading from PLC (attempt {attempt + 1}): {e}")
                if "Job pending" in str(e):
                    logging.info(f"Job pending. Retrying after {self.retry_delay} seconds...")
                else:
                    logging.error(f"Unhandled error: {e}")
                attempt += 1
                time.sleep(self.retry_delay)

        logging.error(f"Max retries reached ({self.max_retries}). Aborting PLC read.")
        return None

    def check_pump_status(self, camera_ip, message):
        data = self.read_data(8, 12, 1)  # Read the necessary PLC data

        if data is None:
            logging.warning(f"Warning: No data received from PLC for {camera_ip}. Skipping pump status check.")
            message["pump_status"] = "Unknown"
            return message["pump_status"]

        if camera_ip == "rtsp://admin:admin@172.168.16.205:554/unicaststream/1":  # Grey drum
            grey_moving = snap7.util.get_bool(data, 0, 2)
            grey_ready_to_run = snap7.util.get_bool(data, 0, 5)
            if grey_moving:
                message["pump_status"] = "Move"
            elif grey_ready_to_run:
                message["pump_status"] = "Ready"
            else:
                message["pump_status"] = "Stop"

        elif camera_ip == "rtsp://admin:admin@172.168.16.203:554/unicaststream/1":  # Blue drum
            blue_moving = snap7.util.get_bool(data, 0, 0)
            blue_ready_to_run = snap7.util.get_bool(data, 0, 3)
            if blue_moving:
                message["pump_status"] = "Move"
            elif blue_ready_to_run:
                message["pump_status"] = "Ready"
            else:
                message["pump_status"] = "Stop"

        elif camera_ip == "rtsp://admin:admin@172.168.16.201:554/unicaststream/1":  # Brown drum
            brown_moving = snap7.util.get_bool(data, 0, 1)
            brown_ready_to_run = snap7.util.get_bool(data, 0, 4)
            if brown_moving:
                message["pump_status"] = "Move"
            elif brown_ready_to_run:
                message["pump_status"] = "Ready"
            else:
                message["pump_status"] = "Stop"

        return message["pump_status"]