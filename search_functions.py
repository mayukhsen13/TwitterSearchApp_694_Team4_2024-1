# mongodb_search.py
import pymongo
from pymongo import MongoClient
import re
import heapq
import time
import pickle

class SearchInMongoDB:  ##Mayukh Sen 
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
class SearchCache: ##Divya Shah
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

############################################################################################################################################################################

class SearchTweets: ## Max Jacobs
    def string_search(words, choice_and_or, date_and_or, range_choice, time_and_or, time_choice):
    #Search By Text
        if choice_and_or == "AND":
            text_where_clause = "AND ".join([f"text LIKE '% {word} %'" for word in words])
        elif choice_and_or == "OR":
            text_where_clause = "OR ".join([f"text LIKE '% {word} %'" for word in words])
        else: 
            text_where_clause = (f"text LIKE '% {words} %'")
        
        if date_and_or == "AND RANGE" or date_and_or == "SINGLE RANGE":
            date_where_clause = "AND ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        elif date_and_or == "OR RANGE":
            date_where_clause = "OR ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        else: 
            date_where_clause = f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {range_choice}"

        if time_and_or == "AND RANGE" or time_and_or == "SINGLE RANGE":
            time_where_clause = "AND ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        elif time_and_or == "OR RANGE":
            time_where_clause = "OR ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        else: 
            time_where_clause = f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {time_choice}"


        where_clauses = [text_where_clause]

        if range_choice:
            where_clauses.append(date_where_clause)

        if time_choice:
            where_clauses.append(time_where_clause)

        if len(where_clauses) > 1:
            where_clause = " AND ".join(where_clauses)
        else:
            where_clause = text_where_clause


        sql_query = f"""
        SELECT 
            text, 
            retweet_count, 
            DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS date,
            TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS time,
            id_str_user, 
            id_str_tweet
        FROM `msds-417117.Tweets.Tweets`
        WHERE {where_clause}
        ORDER BY retweet_count DESC
        """

        query = client.query(sql_query)
        result = query.to_dataframe()

        return result

    ## Search Tweets by Hashtags
    def hashtag_search(hashtags, choice_and_or, date_and_or, range_choice, time_and_or, time_choice):
    
        if choice_and_or == "AND":
            hashtag_where_clause = "AND ".join([f"text LIKE '% {hashtag} %'" for hashtag in hashtags])
        elif choice_and_or == "OR":
            hashtag_where_clause = "OR ".join([f"text LIKE '% {hashtag} %'" for hashtag in hashtags])
        else: 
            hashtag_where_clause = (f"text LIKE '% {hashtags} %'")
        
        if date_and_or == "AND RANGE" or date_and_or == "SINGLE RANGE":
            date_where_clause = "AND ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        elif date_and_or == "OR RANGE":
            date_where_clause = "OR ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        else: 
            date_where_clause = f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {range_choice}"

        if time_and_or == "AND RANGE" or time_and_or == "SINGLE RANGE":
            time_where_clause = "AND ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        elif time_and_or == "OR RANGE":
            time_where_clause = "OR ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        else: 
            time_where_clause = f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {time_choice}"


        where_clauses = [hashtag_where_clause]

        if range_choice:
            where_clauses.append(date_where_clause)

        if time_choice:
            where_clauses.append(time_where_clause)

        if len(where_clauses) > 1:
            where_clause = " AND ".join(where_clauses)
        else:
            where_clause = hashtag_where_clause


        sql_query = f"""
        SELECT 
            text, 
            retweet_count, 
            DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS date,
            TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS time,
            id_str_user, 
            id_str_tweet
        FROM `msds-417117.Tweets.Tweets`
        WHERE {where_clause}
        ORDER BY retweet_count DESC
        """

        query = client.query(sql_query)
        result = query.to_dataframe()

        return result
    
    ## Search Tweets by Number of retweets 
    def retweet_search(retweets, choice_and_or, date_and_or, range_choice, time_and_or, time_choice):
    
        if choice_and_or == "AND":
            retweet_where_clause = "AND ".join([f"retweet_count {retweet} " for retweet in retweets])
        elif choice_and_or == "OR":
            retweet_where_clause = "OR ".join([f"retweet_count {retweet}" for retweet in retweets])
        else: 
            retweet_where_clause = (f"retweet_count {retweets}")
        
        if date_and_or == "AND RANGE" or date_and_or == "SINGLE RANGE":
            date_where_clause = "AND ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        elif date_and_or == "OR RANGE":
            date_where_clause = "OR ".join([f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in range_choice])
        else: 
            date_where_clause = f"DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {range_choice}"

        if time_and_or == "AND RANGE" or time_and_or == "SINGLE RANGE":
            time_where_clause = "AND ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        elif time_and_or == "OR RANGE":
            time_where_clause = "OR ".join([f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {condition}" for condition in time_choice])
        else: 
            time_where_clause = f"TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) {time_choice}"


        where_clauses = [retweet_where_clause]

        if range_choice:
            where_clauses.append(date_where_clause)

        if time_choice:
            where_clauses.append(time_where_clause)

        if len(where_clauses) > 1:
            where_clause = " AND ".join(where_clauses)
        else:
            where_clause = retweet_where_clause


        sql_query = f"""
        SELECT 
            text, 
            retweet_count, 
            DATE(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS date,
            TIME(PARSE_TIMESTAMP('%a %b %d %T %z %Y', created_at)) AS time,
            id_str_user, 
            id_str_tweet
        FROM `msds-417117.Tweets.Tweets`
        WHERE {where_clause}
        ORDER BY retweet_count DESC
        """

        query = client.query(sql_query)
        result = query.to_dataframe()

        return result


