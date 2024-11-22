import os
from datetime import datetime
import logging
from PIL import Image

#logging.getLogger('ppocr').setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

file_handler = logging.FileHandler('logics.log')
stream_handler = logging.StreamHandler()

logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(stream_handler)
# Configuration constants
TRITON_URL = 'localhost:8001'
MODEL_NAME = 'sealer_model'
INPUT_WIDTH = 640
INPUT_HEIGHT = 640
TARGET_LABELS = ['DRUM', 'LABEL', 'QRCODE']
VIDEO_PATHS = [
    'rtsp://admin:admin@172.168.16.201:554/unicaststream/1',
    'rtsp://admin:admin@172.168.16.203:554/unicaststream/1',
    'rtsp://admin:admin@172.168.16.205:554/unicaststream/1'
]
OUTPUT_FOLDER = os.path.join(os.getcwd(), "output_images")
DB_CONNECTION = """postgres://postgres:ai4m2024@localhost:5432/mahindra?sslmode=disable"""
PLC_IP = '192.168.0.70'
INTERNAL_PLC_IP = '192.168.1.21'
TIMEOUT_MINUTES = 30
MAX_FRAMES = 10000

# Status dictionaries
grey_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}
blue_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}
brown_drum_status = {"drum_status": "", "alignment": "", "interlock": ""}

# Shift timings
shifts = {
    "I": {
        "start": datetime.strptime("06:40:00", "%H:%M:%S").time(),
        "end": datetime.strptime("15:10:00", "%H:%M:%S").time()
    },
    "II": {
        "start": datetime.strptime("15:10:00", "%H:%M:%S").time(),
        "end": datetime.strptime("23:50:00", "%H:%M:%S").time()
    },
    "III": {
        "start": datetime.strptime("23:50:00", "%H:%M:%S").time(),
        "end": datetime.strptime("06:40:00", "%H:%M:%S").time()
    }
}
