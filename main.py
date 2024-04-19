from src.tweet_data_processor import TweetDataProcessor
from src.twitter_queries import TwitterQueries

#obj = TweetDataProcessor()
#obj.process_data(file_path="/Users/rohitvernekar/My Drive/Rutgers/DBMS/Project/corona-out-2")


mysql_config = {
    'host': 'databaseteam8.c1coikyuqtk8.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'admin123',
    'database': 'twitter'
}
mongo_uri = 'cluster0.wdgelhd.mongodb.net'
mongo_db_name = 'Database_team_8'

#initiate
twitter_queries = TwitterQueries()

'''search_name = 'NUFF'
sort_metric = 'timestamp'  # Could be 'timestamp', 'retweet', 'favorite', 'engagement'
sort_order = -1  # -1 for descending, 1 for ascending
df = twitter_queries.create_aggregated_username(search_name, sort_metric, sort_order)
print(f"Aggregated Tweet Data for '{search_name}':")
print(df)'''

'''# Fetch and display user data sorted by followers count
df_users_by_followers_desc = twitter_queries.search_and_sort_users('bob', sort_by='followers_count', order='desc')
print("Users sorted by followers count (descending):")
print(df_users_by_followers_desc)'''

user_name = "NUFF"
user_info = twitter_queries.get_user_ids_by_username(user_name)

# Search for tweets by the user from the last week
tweets = twitter_queries.search_tweets_username(user_info, '1week')
print(tweets)