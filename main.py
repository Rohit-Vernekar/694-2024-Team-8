from src.cache import Cache
import time
from src.tweet_data_processor import TweetDataProcessor

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
