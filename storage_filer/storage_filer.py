import os
import logging
import shutil
import json
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

logging.basicConfig(level=logging.INFO)

from database.mongodb import VehicleDB
script_dir = os.path.dirname(os.path.abspath(__file__))
    

    

class StorageFiler():
    def __init__(self) -> None:
        self.vehicle_db = VehicleDB()
        self.files_watchdog = self.create_event_handler()
        self.set_handler_events()
        self.vehicle_files_dir = os.path.join(script_dir,"vehicle_files")
        self.processed_dir = os.path.join(self.vehicle_files_dir, "processed")
        

    def create_event_handler(self):
        patterns = [collection+'_*' for collection in self.vehicle_db.get_collections_names()]
        ignore_patterns = None
        ignore_directories = True
        case_sensitive = True
        return PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    
    
    ####################### events #######################

    def set_handler_events(self):
        self.files_watchdog.on_created = self.on_created

    def on_created(self, event):
        logging.info("new files found in vehicle files directory")
        self._collect_data_from_files()

    ##################### events end #####################

    def _collect_data_from_files(self):
        files = os.listdir(self.vehicle_files_dir)
        if files != []:
            logging.debug("found files: %s", files)
            for file_name in files:
                for collection_name in self.vehicle_db.get_collections_names():
                    try:
                        if file_name.startswith(collection_name):
                            file_path = os.path.join(self.vehicle_files_dir, file_name)
                            with open(file_path, 'r') as f:
                                data = json.load(f)
                                listings = [listing for _, listing in data.items()]
                            self.vehicle_db.insert_collection_listings(collection_name, listings)
                            self._move_to_processed(file_path)
                    except Exception as err:
                        print(err)

    def _create_processed(self):
        if not os.path.exists(self.processed_dir):
            os.makedirs(self.processed_dir)

    def _move_to_processed(self, file):
        if not os.path.exists(file):
            logging.error("file %s doesn't exist", file)
            return
        try:
            shutil.move(file, self.processed_dir)
            logging.debug("moving %s to %s.", file, self.processed_dir)
        except Exception as err:
            logging.error("Error occurred: %s", err)
        
    def start_storage_filer_process(self):
        self._create_processed()
        if os.listdir(self.vehicle_files_dir):
            self._collect_data_from_files()
        observer = Observer()
        observer.schedule(self.files_watchdog, self.vehicle_files_dir, recursive=False)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        finally:
            observer.join()


if __name__ == '__main__':

    pass
