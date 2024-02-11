import os
import logging
import json
from enum import Enum

logging.basicConfig(level=logging.DEBUG)

from database.mongodb import VehicleDB
script_dir = os.path.dirname(os.path.abspath(__file__))

class ObjectEvent(Enum):

    VEHICLE_ID = "vehicle_id"
    EVENT_TYPE = "event_type"
    TIMESTAMP = "time_stamp"

class VehicleStatus(Enum):

    VEHICLE_ID = "vehicle_id"
    EVENT_TYPE = "event_type"
    TIMESTAMP = "time_stamp"

class StorageFiler():
    def __init__(self) -> None:
        self.vehicle_db = VehicleDB()
        self.vehicle_files_pass = os.path.join(script_dir,"vehicle_files")
        self.object_file_prefix = "objects_detection"
        self.vehicle_status_prefix = "vehicle_status"

    def write_object_events(self, data):
        events = []
        for event, event_data in data.items():
            
            event_dict = {}
            if event.startswith("event_"):
                for enum in ObjectEvent:
                    event_dict[enum.value] = event_data.get(enum.value)
        events.append(event_dict)
        self.vehicle_db.insert_object_events(events)

    def collect_events(self):
        files = os.listdir(self.vehicle_files_pass)
        object_detection_files = [file for file in files if file.startswith('objects_detection')]
        if object_detection_files != []:
            logging.debug("fount object detection files: %s", object_detection_files)
            for file_name in object_detection_files:

                file_path = os.path.join(self.vehicle_files_pass, file_name)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    self.write_object_events(data)
                    


if __name__ == '__main__':

    pass
