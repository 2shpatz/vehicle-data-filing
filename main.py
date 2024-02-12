import os
import time
import json
import random
import string
from datetime import datetime

from storage_filer.storage_filer import StorageFiler

def generate_objects_random_dict(data_id, timestamp):
    dictionary = {}
    dictionary[f"vehicle_{data_id}1"] = {"vehicle_id" : ''.join(random.choices(string.ascii_letters, k=5)), "event_type": ''.join(random.choices(string.ascii_letters, k=5)), "time_stamp": timestamp}
    dictionary[f"vehicle_{data_id}2"] = {"vehicle_id" : ''.join(random.choices(string.ascii_letters, k=5)), "event_type": ''.join(random.choices(string.ascii_letters, k=5)), "time_stamp": timestamp}
    return dictionary

def generate_status_random_dict(data_id, timestamp):
    dictionary = {}
    dictionary[f"vehicle_{data_id}1"] = {"vehicle_id" : ''.join(random.choices(string.ascii_letters, k=5)), "status": ''.join(random.choices(string.ascii_letters, k=5)), "time_stamp": timestamp}
    dictionary[f"vehicle_{data_id}2"] = {"vehicle_id" : ''.join(random.choices(string.ascii_letters, k=5)), "status": ''.join(random.choices(string.ascii_letters, k=5)), "time_stamp": timestamp}
    return dictionary

def create_some_files_for_simulation(dir_path):
    # simulates some file creation for testing the system
    for i in range(10):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        objects_file_path = os.path.join(dir_path, f"objects_detection_{timestamp}.json")
        status_file_path = os.path.join(dir_path, f"vehicles_status_{timestamp}.json")
        with open(objects_file_path, "w") as file:
            json.dump(generate_objects_random_dict(i, timestamp), file, indent=4)
        with open(status_file_path, "w") as file:
            json.dump(generate_status_random_dict(i, timestamp), file, indent=4)
        time.sleep(2)

def main():
    file_storage = StorageFiler()
    file_storage.start_storage_filer_process()
    create_some_files_for_simulation(file_storage.vehicle_files_dir)


if __name__ == '__main__':
    main()
