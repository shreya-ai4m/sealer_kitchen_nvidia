import snap7
from snap7 import util
from datetime import datetime

plc = snap7.client.Client()

plc_ip, rack, slot = '192.168.0.70', 0, 0
db_number = 8

product_code = "3090"
batch = "INME41138540"
expiry= "10/06/24"
#interlock = {1:"OKAY", 2:"CODE-MISMATCH", 3:"EXPIRED"}
interlock = 1

plc.connect(plc_ip,rack,slot)
print("Plc connected:",plc.get_connected())

def read_plc():

    data = plc.db_read(db_number, 0, 4)
    seam_flow_rate = util.get_real(data,0)
    data = plc.db_read(db_number, 4, 4)
    ubc_flow_rate = util.get_real(data,0)
    data = plc.db_read(db_number, 8, 4)
    hem_flow_rate = util.get_real(data,0)

    data = plc.db_read(db_number,12, 1)
    seam_actually_moving = util.get_bool(data, 0, 0)
    ubc_actually_moving = util.get_bool(data, 0, 1)
    hem_actually_moving = util.get_bool(data, 0, 2)

    data = plc.db_read(db_number,12, 1)
    seam_ready_to_run = util.get_bool(data, 0, 3)
    ubc_ready_to_run = util.get_bool(data, 0, 4)
    hem_ready_to_run = util.get_bool(data, 0, 5)

    data = plc.db_read(db_number, 12, 1)
    seam_drum_loaded = util.get_bool(data, 0, 6)
    ubc_drum_loaded = util.get_bool(data, 0, 7)
    data = plc.db_read(db_number, 13, 1)
    hem_drum_loaded = util.get_bool(data, 0, 0)

    print("seam_actually_moving:",seam_actually_moving)
    print("ubc_actually_moving:",ubc_actually_moving)
    print("hem_actually_moving:",hem_actually_moving)
    print("seam_ready_to_run:",seam_ready_to_run)
    print("ubc_ready_to_run:",ubc_ready_to_run)
    print("hem_ready_to_run:",hem_ready_to_run)
    print("seam_drum_loaded:",seam_drum_loaded)
    print("ubc_drum_loaded:",ubc_drum_loaded)
    print("hem_drum_loaded:",hem_drum_loaded)
    print("seam_flow_rate:",seam_flow_rate)
    print("ubc_flow_rate:",ubc_flow_rate)
    print("hem_flow_rate:",hem_flow_rate)

def write_ascii_string(data, string, length):
    string_bytes = string.encode('ascii')
    string_bytes += b' ' * (length - len(string_bytes))
    data[:length] = string_bytes

def write_plc():
    data = bytearray(4)
    write_ascii_string(data, product_code, 4)
    plc.db_write(db_number, 14, data)

    data = bytearray(12)
    write_ascii_string(data, batch, 12)
    plc.db_write(db_number, 36, data)

    data = bytearray(8)
    write_ascii_string(data, expiry, 8)
    plc.db_write(db_number, 58, data)

def check_if_written():
    print("product_code", plc.db_read(db_number,14,4).decode())
    print("batch", plc.db_read(db_number,36,12).decode())
    print("expiry", plc.db_read(db_number,58,8).decode())
    data = plc.db_read(8, 80, 2)
    print("seam interlock:",util.get_int(data, 0))
    data = plc.db_read(8, 82, 2)
    print("UBC interlock:",util.get_int(data, 0))
    data = plc.db_read(8, 84, 2)
    print("Hem interlock:",util.get_int(data, 0))

read_plc()
#write_plc()
check_if_written()
