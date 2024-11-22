# Database insert queries for the main data tables
brown_raw_insert_query = """INSERT INTO sealer_brown (timestamp, status_of_drum, label, qrcode, presence_status, objects, product_name, weight, batch_number, expiry_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
blue_raw_insert_query = """INSERT INTO sealer_blue (timestamp, status_of_drum, label, qrcode, presence_status, objects, product_name, weight, batch_number, expiry_date)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
grey_raw_insert_query = """INSERT INTO sealer_grey (timestamp, status_of_drum, label, qrcode, presence_status, objects, product_name, weight, batch_number, expiry_date)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

# Database insert queries for the status tables
brown_status_insert_query = """INSERT INTO sealer_brown_status (station, current_shift, timestamp, drum_status, label_status, pump_status, aligned)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)"""
blue_status_insert_query = """INSERT INTO sealer_blue_status (station, current_shift, timestamp, drum_status, label_status, pump_status, aligned)
                              VALUES (%s, %s, %s, %s, %s, %s, %s)"""
grey_status_insert_query = """INSERT INTO sealer_grey_status (station, current_shift, timestamp, drum_status, label_status, pump_status, aligned)
                              VALUES (%s, %s, %s, %s, %s, %s, %s)"""