import json
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class TweetDataProcessor:
    def __init__(self, host_name, user_name, user_password, db_name, port=3306):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.db_name = db_name
        self.port = port
        self.connection = self.connect_to_mysql()

    def connect_to_mysql(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host_name,
                user=self.user_name,
                passwd=self.user_password,
                database=self.db_name,
                port=self.port
            )
            print("Connection to MySQL DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection

    def fetch_user(self, user_data):
        user = {
            "user_id": user_data["id"],
            "name": user_data["name"],
            "screen_name": user_data["screen_name"],
            "location": user_data.get("location", ""),
            "description": user_data.get("description", ""),
            "verified": user_data["verified"],
            "followers_count": user_data["followers_count"],
            "friends_count": user_data["friends_count"],
            "listed_count": user_data["listed_count"],
            "favourites_count": user_data["favourites_count"],
            "statuses_count": user_data["statuses_count"],
            "created_at": self.parse_datetime(user_data["created_at"]),
            "geo_enabled": user_data["geo_enabled"],
            "profile_picture": user_data["profile_image_url_https"]
        }
        return user

    def fetch_tweet(self, tweet_data):
        tweet = {
            "tweet_id": tweet_data["id"],
            "user_id": tweet_data["user"]["id"],
            "created_at": tweet_data["created_at"],
        }
        return tweet

    def parse_datetime(self, timestamp_str):
        return datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y')

    def data_processing(self, file_path):
        user_data = []
        tweet_data = []
        with open(file_path, 'r') as file:
            for line in file:
                if line != '\n':
                    data = json.loads(line)
                    user = self.fetch_user(data['user'])
                    tweet = self.fetch_tweet(data)
                    
                    user_data.append(user)  # Add the user of the tweet
                    
                    if 'retweeted_status' in data:
                        # Handling retweet
                        try:
                            user_data_rt = self.fetch_user(data['retweeted_status']['user'])
                            tweet_data_rt = self.fetch_tweet(data['retweeted_status'])
                            tweet_data_rt["retweet_id"] = None  # Indicate this tweet is a retweet
                            user_data.append(user_data_rt)  # Add the user of the retweet
                            tweet_data.append(tweet_data_rt)  # Add the retweeted tweet
                        except Exception as e:
                            print(f"Error processing retweet: {e}")
                    else:
                        tweet["retweet_id"] = None
                    
                    tweet_data.append(tweet)  # Always add the original tweet

        return user_data, tweet_data

    def insert_in_sql(self, tweet_data, user_data):
        try:
            engine = create_engine(f'mysql+mysqlconnector://{self.user_name}:{self.user_password}@{self.host_name}/{self.db_name}')
            
            # Convert user_data to DataFrame and remove duplicates
            df_user = pd.DataFrame(user_data).drop_duplicates(['user_id'])
            
            # Convert tweet_data to DataFrame, remove duplicates and drop 'retweet_id' column
            df_tweet = pd.DataFrame(tweet_data).drop_duplicates(['tweet_id']).drop('retweet_id', axis=1)
            
            # Insert Users first to avoid foreign key constraint failure
            if not df_user.empty:
                df_user.to_sql('users', con=engine, if_exists='append', index=False)
                print("Users inserted successfully.")
            else:
                print("No user data to insert.")
            
            # Then insert Tweets
            if not df_tweet.empty:
                df_tweet.to_sql('tweets', con=engine, if_exists='append', index=False)
                print("Tweets inserted successfully.")
            else:
                print("No tweet data to insert.")
                
        except Exception as e:
            print(f"An error occurred: {e}")

