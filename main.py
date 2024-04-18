from src.tweet_data_processor import TweetDataProcessor
from src.twitter_queries import TwitterQueries

obj = TweetDataProcessor()
obj.process_data(file_path="/Users/rohitvernekar/My Drive/Rutgers/DBMS/Project/corona-out-2")


mysql_config = {
    'host': 'databaseteam8.c1coikyuqtk8.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'admin123',
    'database': 'twitter'
}
mongo_uri = 'cluster0.wdgelhd.mongodb.net'
mongo_db_name = 'Database_team_8'


twitter_queries = TwitterQueries()
    # Search and display tweets by hashtag within the last 1 day
twitter_queries.fetch_and_display_tweets("#example", search_type='hashtag', time_frame='1day', sort_fields={'created_at': 'desc'})

# Search and display tweets by username sorted by creation date and filtered for the last week
twitter_queries.fetch_and_display_tweets("john_doe", search_type='username', sort_fields={'created_at': 'desc'}, time_frame='1week')  
    
#Search users and sort them using followers count or last_post_timestamp
users = twitter_queries.search_and_sort_users("john", sort_by='followers_count')
for user in users:
    print(f"User ID: {user['user_id']}, Name: {user['name']}, Followers: {user['followers_count']}, Last Posted: {user['last_post_timestamp']}")