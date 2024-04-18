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


# Assume twitter_queries is an instance of TwitterQueries
twitter_queries = TwitterQueries()

# Fetch and display tweets by hashtag
hashtag = "#technology"
twitter_queries.fetch_and_display_tweets(hashtag, search_type='hashtag', sort_fields={'retweet_count': 'desc'}, time_frame='1day')


# Fetch and display tweets by username
username = "alice"
twitter_queries.fetch_and_display_tweets(username, search_type='username', sort_fields={'created_at': 'desc'}, time_frame='1week')


# Search users by name and sort by followers count
search_term = "bob"
sort_by = 'followers_count'  # Could also use 'last_post_timestamp'
users = twitter_queries.search_and_sort_users(search_term, sort_by)

# Display the sorted users
print("Sorted users:")
for user in users:
    print(f"User ID: {user['user_id']}, Name: {user['name']}, Followers: {user['followers_count']}, Last Posted: {user['last_post_timestamp']}")
