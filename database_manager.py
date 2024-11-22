import psycopg2
import json
import logging
from datetime import datetime
import numpy as np
from queries import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logics.log'),
        logging.StreamHandler()
    ]
) 

class DatabaseManager:
    def __init__(self, db_connection_string):
        try:
            self.connection = psycopg2.connect(db_connection_string)
            self.cursor = self.connection.cursor()
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
            print(f"Error connecting to the database: {e}")
            self.connection = None

    def insert_data(self, camera_ip, data):
        insert_query = None
        status_insert_query = None

        # Determine which table to insert into based on camera IP
        if camera_ip == 'rtsp://admin:admin@172.168.16.205:554/unicaststream/1':  # Grey Drum
            insert_query = grey_raw_insert_query
            status_insert_query = grey_status_insert_query
        elif camera_ip == 'rtsp://admin:admin@172.168.16.203:554/unicaststream/1':  # Blue Drum
            insert_query = blue_raw_insert_query
            status_insert_query = blue_status_insert_query
        elif camera_ip == 'rtsp://admin:admin@172.168.16.201:554/unicaststream/1':  # Brown Drum
            insert_query = brown_raw_insert_query
            status_insert_query = brown_status_insert_query
        #self.open_connection()

        try:
            # Convert numpy types to standard Python types
            data = self.convert_numpy_types(data)

            # Ensure 'presence_status' field is always present
            data['presence_status'] = data.get('presence_status', False)

            # Ensure 'qr_code_present' and 'timestamp' fields are set
            data['qr_code_present'] = data.get('qr_code_present', False)
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().isoformat()

            # Ensure 'yolo_status' and 'objects' are present in the data
            data['yolo_status'] = data.get('yolo_status', False)
            data['objects'] = data.get('objects', [])

            logging.debug(f"Data to be inserted: {data}")

            # Insert into the primary table (without 'aligned')
            self.cursor.execute(insert_query, (
                data['timestamp'], data['drum_status'], data['label_status'],
                data['qr_code_present'], data['presence_status'], json.dumps(data['objects']),
                data.get('product_name', ''), data.get('weight', ''),
                data.get('batch', ''), data.get('expiry', '')
            ))

            # Insert into the status table (with 'aligned')
            self.cursor.execute(status_insert_query, (
                data.get('station', ''), data.get('current_shift', ''), data['timestamp'],
                data['drum_status'], data['label_status'], data['pump_status'], data['aligned']
            ))

            # Commit both insertions
            self.connection.commit()
            logging.info("Data inserted into both tables successfully.")
        except Exception as e:
            self.connection.rollback()
            print(f"Error inserting data: {e}")
            error_message = f"Error inserting data: {e} | Data: {data}"
            logging.error(error_message)
            print(error_message)
        '''finally:
            # Close the connection to ensure proper cleanup
            try:
                #self.cursor.close()
                self.connection.close()
                print("Database connection closed.")
            except Exception as e:
                logging.error(f"Error closing database connection: {e}")
                print(f"Error closing database connection: {e}")'''

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


"""class ShiftManager:
    @staticmethod
    def check_shift(self):
        try:
            current_time = datetime.now().time()

            if self.shifts["I"]["start"] <= current_time <= self.shifts["I"]["end"]:
                self.message["current_shift"] = "I"
            elif self.shifts["II"]["start"] <= current_time <= self.shifts["II"]["end"]:
                self.message["current_shift"] = "II"
            elif (self.shifts["III"]["start"] <= current_time) or (current_time <= self.shifts["III"]["end"]):
                self.message["current_shift"] = "III"
            else:
                self.message["current_shift"] = "Unknown"
        except Exception as e:
            print(f"Error checking shift: {e}")
"""