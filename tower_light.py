def run_tower_light(self, camera_ip, drum_status, alignment, interlock):
    try:
        data = bytearray(1)
        if camera_ip == '192.168.1.32':
            if drum_status=='Absent' or (drum_status=='Present' and alignment=='Not Aligned'):
                snap7.util.set_bool(data,0,5,True)
            if alignment=='Aligned'and interlock == 1:
                snap7.util.set_bool(data,0,6,True)         
            #if alignment=="Aligned" and (interlock=="2" or interlock=="3" or interlock=="4"):
                #snap7.util.set_bool(data,0,4,True)

        if camera_ip == '192.168.1.31':
            if drum_status=='Absent' or (drum_status=='Present' and alignment=='Not Aligned'):
                snap7.util.set_bool(data,0,2,True)
            if alignment=='Aligned'and interlock == 1:
                snap7.util.set_bool(data,0,3,True)
            #if alignment=="Aligned" and (interlock=="2" or interlock=="3" or interlock=="4"):
                #snap7.util.set_bool(data,0,1,True)

        if camera_ip == '192.168.1.30':
            if drum_status=='Absent' or (drum_status=='Present' and alignment=='Not Aligned'):
                snap7.util.set_bool(data,0,7,True)
            if alignment=='Aligned'and interlock == 1:
                snap7.util.set_bool(data,0,0,True)
            #if alignment=="Aligned" and (interlock=="2" or interlock=="3" or interlock=="4"):
                #snap7.util.set_bool(data,0,6,True)

        self.internal_plc.db_write(1,528,data)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

