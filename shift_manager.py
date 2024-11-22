from datetime import datetime, time as dt_time

# Shift data
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

class ShiftManager:
    @staticmethod
    def check_shift():
        current_time = datetime.now().time()
        for shift, timings in shifts.items():
            start_time = timings["start"]
            end_time = timings["end"]
            if start_time < end_time:
                # Shift does not span midnight
                if start_time <= current_time <= end_time:
                    return shift
            else:
                # Shift spans midnight
                if current_time >= start_time or current_time <= end_time:
                    return shift
            #if timings["start"] <= current_time <= timings["end"]:
                #return shift
        return "Unknown"