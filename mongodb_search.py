# mongodb_search.py
import pymongo
from pymongo import MongoClient
import re

class SearchInMongoDB:
    def __init__(self, uri, collection_name):
        self.client = MongoClient(uri)
        self.db = self.client["twitter_database"]
        self.collection = self.db[collection_name]

    def search_by_name(self, name, search_type='fuzzy'):
        if search_type == 'fuzzy':
            query = {"$or": [
                {"name": {"$regex": re.escape(name), "$options": "i"}},
                {"screen_name": {"$regex": re.escape(name), "$options": "i"}}
            ]}
        else:  # exact search
            query = {"$or": [
                {"name": name},
                {"screen_name": name}
            ]}
        return list(self.collection.find(query))

    def search_by_id_str(self, id_str):
        return list(self.collection.find({"id_str": id_str}))

    def search_by_favourites(self, min_favourites, max_favourites=None):
        if max_favourites:
            query = {"favourites_count": {"$gte": min_favourites, "$lte": max_favourites}}
        else:
            query = {"favourites_count": {"$gte": min_favourites}}
        return list(self.collection.find(query).sort("favourites_count", -1))

    def search_by_location(self, location):
        query = {"location": {"$regex": re.escape(location), "$options": "i"}}
        return list(self.collection.find(query))

    def search_by_followers_count(self, min_followers):
        return list(self.collection.find({"followers_count": {"$gte": min_followers}}))

    def close_connection(self):
        self.client.close()
