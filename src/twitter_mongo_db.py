import pandas as pd
from pymongo import MongoClient
import certifi

class TwitterMongoInserter:
    def __init__(self, file_path, mongo_connection_string, database_name, collection_name):
        self.file_path = file_path
        self.mongo_connection_string = mongo_connection_string
        self.database_name = database_name
        self.collection_name = collection_name

    def load_data(self):
        try:
            twitter_data_df = pd.read_json(self.file_path, lines=True)
            print("Data loaded successfully into DataFrame")
            return twitter_data_df
        except ValueError as err:
            print(f"Error loading JSON: {err}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    def connect_to_mongodb(self):
        try:
            client = MongoClient(self.mongo_connection_string, tlsCAFile=certifi.where())
            db = client[self.database_name]
            tweets_collection = db[self.collection_name]
            print("Connected to MongoDB")
            return tweets_collection
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return None

    def insert_data(self):
        twitter_data_df = self.load_data()
        if twitter_data_df.empty:
            print("No data to insert.")
            return
        
        tweets_collection = self.connect_to_mongodb()
        if tweets_collection is None:
            print("MongoDB connection not established.")
            return

        for index, row in twitter_data_df.iterrows():
            tweet_document = {
                'created_at': row.get('created_at'),
                'id': row.get('id'),
                'id_str': row.get('id_str'),
                'text': row.get('text'),
                'source': row.get('source'),
                'user': row.get('user'),
                'place': row.get('place'),
                'retweeted_status': row.get('retweeted_status'),
                'quoted_status': row.get('quoted_status'),
                'quote_count': row.get('quote_count'),
                'reply_count': row.get('reply_count'),
                'retweet_count': row.get('retweet_count'),
                'favorite_count': row.get('favorite_count'),
                'lang': row.get('lang')
            }
            tweets_collection.insert_one(tweet_document)

        print("Data inserted successfully into MongoDB")

# Usage example
#file_path = 'C://Users//Deep.ai//Downloads//corona-out-2//corona-out-2'
#mongo_connection_string = 'mongodb+srv://bhaveshsharma7895:12345@cluster0.wdgelhd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
#database_name = 'tweet'
#collection_name = 'Tweet_database'

#inserter = TwitterMongoInserter(file_path, mongo_connection_string, database_name, collection_name)
#inserter.insert_data()
