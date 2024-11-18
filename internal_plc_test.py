import snap7
from snap7 import util
plc = snap7.client.Client()
db_number = 1

plc.connect('192.168.1.21', 0, 1)
#print(plc.get_connected())

a=1
b=2
c=3
data = bytearray(1)
#if a==1:
#    snap7.util.set_bool(data,0,0,False)
if b==2:
    snap7.util.set_bool(data,0,5,False)
#if c==3:
#    snap7.util.set_bool(data,0,6,False)
#plc.db_write(1,528,data)

data = plc.db_read(db_number, 528, 1)
print("Brown_drum (Red)",util.get_bool(data, 0, 4))

data = plc.db_read(db_number, 528, 1)
print("Brown_drum (Yellow)",util.get_bool(data, 0, 5))

data = plc.db_read(db_number, 528, 1)
print("Brown_drum (Green)",util.get_bool(data, 0, 6))

data = plc.db_read(db_number, 528, 1)
print("Blue_drum (Red",util.get_bool(data, 0, 1))

data = plc.db_read(db_number, 528, 1)
print("Blue_drum (Yellow)",util.get_bool(data, 0, 2))

data = plc.db_read(db_number, 528, 1)
print("Blue_drum (Green)",util.get_bool(data, 0, 3))

data = plc.db_read(db_number, 527, 1)
print("Grey_drum (Red)",util.get_bool(data, 0, 6))

data = plc.db_read(db_number, 527, 1)
print("Grey_drum (Yellow)",util.get_bool(data, 0, 7))

data = plc.db_read(db_number, 528, 1)
print("Grey_drum (Green)",util.get_bool(data, 0, 0))

