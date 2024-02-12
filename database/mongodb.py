
import logging
from pymongo import MongoClient,DESCENDING
import pandas as pd

MONGOBD_SERVER = "localhost"
MONGOBD_PORT = 27017


class VehicleDB(MongoClient):
    # This class accessing the mongodb collection
    def __init__(self, host=MONGOBD_SERVER, port=MONGOBD_PORT, **kwargs):
        super().__init__(host, port, **kwargs)
        self.db_name = "vehicles"
        self.db = self.get_database(self.db_name)

    def create_collection(self, collection_name:str):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)

    def get_collection(self, collection_name:str):
        if collection_name in self.db.list_collection_names():
            return self.db[collection_name]

    def get_collections_names(self):
        return self.db.list_collection_names()
    
    def insert_collection_listings(self, collection_name:str, listings:list[dict]):
        collection = self.get_collection(collection_name)
        result = collection.insert_many(listings)
        return str(result)

    def get_database_name(self):
        return self.db_name

    def get_aggregated_dataframe(self, collection_name:str, pipeline:list) -> pd.DataFrame:
        collection = self.get_collection(collection_name) 
        return pd.DataFrame(list(collection.aggregate(pipeline)))

    def distinct(self, collection_name:str, field_name:str) -> list:
        # Retrieves list of all the values of a given field name
        collection = self.get_collection(collection_name) 
        return collection.distinct(field_name)

    def get_all_documents_cursor(self, collection_name:str):
        collection = self.get_collection(collection_name) 
        return collection.find({})

    def find_one(self, collection_name:str, field:str, value:str):
        # retrieves the last document that found in the collection
        collection = self.get_collection(collection_name)
        return collection.find_one({field : value})
    
    def find_last(self, collection_name:str, field:str, value:str):
        # retrieves the first document that found in the collection
        collection = self.get_collection(collection_name) 
        return collection.find_one({field : value}, sort=[('_id', DESCENDING)])

if __name__ == '__main__':

    pass
