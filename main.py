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



# Create an instance of TwitterQueries
twitter_queries = TwitterQueries()

# Define the username you want to search for
username = "NUFF"

# Define sorting and time frame parameters
sort_fields = {'order': 'latest'}  # Sorting tweets by the latest
time_frame = '1week'  # Filtering tweets from the last week

# Call the method to search tweets by username
tweets_by_username = twitter_queries.search_tweets_by_username(username, sort_fields, time_frame)

# Check if any tweets were found and print the results
if tweets_by_username:
    print(f"Tweets found for username '{username}':")
    for tweet in tweets_by_username:
        print(tweet)  # Print each tweet's details
else:
    print(f"No tweets found for username '{username}'.")

