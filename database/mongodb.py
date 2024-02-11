
import os
import sys
import logging
from pymongo import MongoClient,DESCENDING
from pymongo import errors as mongo_error
import pandas as pd

MONGOBD_SERVER = "localhost"
MONGOBD_PORT = 27017

class MongoDB(MongoClient):
    def __init__(self, host=MONGOBD_SERVER, port=MONGOBD_PORT, **kwargs):
        try:
            super().__init__(host, port, **kwargs)
        except [mongo_error.ConnectionFailure, mongo_error.ConfigurationError] as err:
            exc_type, _, exc_tb = sys.exc_info()
            error = f"[{os.path.basename(__file__)}]Exception: {err} {exc_type} Error line: {exc_tb.tb_lineno}"
            raise

    def create_collection(self, database_name, collection_name):
        db = self.get_database(database_name)
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)

    def get_database(self, database_name):
        return self[database_name]

    def get_collection(self, database_name, collection_name):
        database = self.get_database(database_name)
        return database[collection_name]

    # def get_collection(self, database_name, collection_name):
    #     # Retrieves mongodb collection from database by name
    #     database = self.get_database(database_name)
    #     if collection_name not in database.list_collection_names():
    #         return database[collection_name]
    #     else:
    #         print("error")


class VehicleDB(MongoClient):
    # This class accessing the mongodb collection
    def __init__(self, host=MONGOBD_SERVER, port=MONGOBD_PORT, **kwargs):
        super().__init__(host, port, **kwargs)
        self.db = self.get_database("vehicles")
        self.object_events_collection = self.get_object_events_collection()
        # self.vehicle_status_collection = self.get_vehicle_status_collection()
        print(self.object_events_collection)

    def get_object_events_collection(self):
        collection_name = "object_events"
        if collection_name in self.db.list_collection_names():
            return self.db[collection_name]

    def get_vehicle_status_collection(self):
        collection_name = "vehicle_status"
        if collection_name in self.db.list_collection_names():
            return self.db[collection_name]

    
    def insert_object_events(self, data):
        try:
            self.object_events_collection.insert_many(data)
        except Exception as err:
            print(err)


    def insert_vehicle_status(self, data):
        self.vehicle_status_collection.insert_one(data)

    def get_database_name(self):
        return self.database_name

    def get_collection_name(self):
        return self.collection_name

    def get_aggregated_dataframe(self, pipeline:list) -> pd.DataFrame:
        return pd.DataFrame(list(self.collection.aggregate(pipeline)))

    def distinct(self, field_name:str) -> list:
        # Retrieves list of all the values of a given field name
        return self.collection.distinct(field_name)

    def get_all_documents_cursor(self):
        return self.collection.find({})

    def find_one(self, field, value):
        # retrieves the last document that found in the collection
        return self.collection.find_one({field : value})
    
    def find_last(self, field, value):
        # retrieves the first document that found in the collection
        return self.collection.find_one({field : value}, sort=[('_id', DESCENDING)])

if __name__ == '__main__':

    pass
