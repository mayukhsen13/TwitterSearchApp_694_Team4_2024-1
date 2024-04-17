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


############################################################################################################################################################################
class SearchCache:
    def __init__(self, max_size=35):
        self.max_size = max_size
        self.heap = []
        self.cache = {}

    def store_search(self, search_input, search_result):
        # If cache is full, remove the oldest search
        if len(self.cache) >= self.max_size:
            oldest_search = heapq.heappop(self.heap)[1]
            del self.cache[oldest_search]

        # Add the new search input and result
        self.cache[search_input] = search_result
        heapq.heappush(self.heap, (time.time(), search_input))

    def get_search_result(self, search_input):
        # If search input is found in cache, return the result
        if search_input in self.cache:
            return self.cache[search_input]
        else:
            return None

    def save_cache(self, filename):
        with open(filename, "wb") as file:
            pickle.dump(self.cache, file)

    def load_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                self.cache = pickle.load(file)
                # Reconstruct the heap from the loaded cache
                self.heap = [(time.time(), key) for key in self.cache]
                heapq.heapify(self.heap)
        except FileNotFoundError:
            print("Cache file not found. Starting with an empty cache.")
