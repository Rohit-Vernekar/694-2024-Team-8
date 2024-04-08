from src.cache import Cache
import time
from src.tweet_data_processor import TweetDataProcessor
from src.twitter_mongo_db import TwitterMongoInserter
test_cache = Cache(cache_path="/Users/rohitvernekar/Desktop/cache.pkl")

for i in range(200):
    test_cache.put(i, i**5)
    time.sleep(1)
    for j in range(i):
        print(test_cache.get(j))

# for key, value in test_cache._data.items():
#     print(key, value)

# Example usage
processor = TweetDataProcessor("databaseteam8.c1coikyuqtk8.us-east-1.rds.amazonaws.com", "admin", "admin123", "databaseteam8")
file_path = "C:/Users/obero/Desktop/corona-out-2"  # set file path
user_data, tweet_data = processor.data_processing(file_path)
#processor.insert_in_sql(tweet_data, user_data)

file_path = 'C://Users//Deep.ai//Downloads//corona-out-2//corona-out-2'
mongo_connection_string = 'mongodb+srv://bhaveshsharma7895:12345@cluster0.wdgelhd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
database_name = 'tweet'
collection_name = 'Tweet_database'

mongo_processor = TwitterMongoInserter(file_path, mongo_connection_string, database_name, collection_name)
#mongo_processor.insert_data()