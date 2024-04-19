import pymongo
import mysql.connector
from datetime import datetime, timedelta
from mysql.connector import Error
import pandas as pd
from datetime import datetime, timedelta
import pytz
from .connections import get_mysql_conn, get_mongodb_conn

class TwitterQueries:
    def __init__(self):
        # Initialize MySQL connection
        self.mysql_connection = get_mysql_conn()
        # Initialize MongoDB connection
        try:
            self.mongo_db = get_mongodb_conn(collection="tweet_data")
        except Exception as e:
            print("Failed to connect to MongoDB:", e)
            self.mongo_db = None

    def ensure_text_index(self):
        if self.mongo_db is not None:
            if 'text' not in self.mongo_db.index_information():
                self.mongo_db.create_index([('text', pymongo.TEXT)], default_language='english')
                print("Text index created on the 'text' field.")
            else:
                print("Text index already exists.")

    def get_user_ids_by_username(self, user_name):
        # Handle MySQL connection issues
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        try:
            query = "SELECT id_str, name FROM users WHERE name LIKE CONCAT('%', %s, '%');"
            with self.mysql_connection.cursor() as cursor:
                cursor.execute(query, (user_name,))
                results = cursor.fetchall()
                return {row[0]: row[1] for row in results}
        except Error as e:
            print(f"Error fetching user IDs: {e}")
            return []

    # Search tweets by username
    def search_tweets_username(self, user_info, time_frame=None):
        if self.mongo_db is None:
            print("MongoDB connection is not established.")
            return pd.DataFrame()

        # Get the time limit from the utility function, ensuring it's timezone-aware
        time_limit = self.get_time_limit(time_frame) if time_frame else None

        # Build the query with a time limit if specified
        query = {"user": {"$in": list(user_info.keys())}}
        if time_limit:
            query["created_at"] = {"$gte": time_limit.isoformat()}  # Use isoformat for MongoDB compatibility

        # Specify the projection to fetch necessary fields
        projection = {
            "text": 1, 
            "user": 1, 
            "lang": 1, 
            "is_retweeted_status": 1,    
            "retweet_count": 1, 
            "favorite_count": 1, 
            "reply_count": 1,
            "created_at": 1,
            "is_quote_status": 1
        }

        results = list(self.mongo_db.find(query, projection))
        if not results:
            print("No tweets found.")
            return pd.DataFrame()

        # Post-process results to add custom fields based on available data
        for result in results:
            # Determine if the tweet is a retweet
            result['is_retweet_status'] = True if result.get('is_retweet_status', False) else False
            result['is_quote_status'] = True if result.get('is_quote_status', False) else False
            result['user_name'] = user_info.get(result['user'], 'Unknown')
            df = pd.DataFrame(results)
            # Reorder and select columns as required
            column_order = ['user_name', 'text', 'lang', 'is_retweet_status', 'is_quote_status',
                            'reply_count', 'retweet_count', 'favorite_count', 'created_at']
            df = df[column_order]

            return df

    # aggregates the searched tweet 
    def create_aggregated_username(self, search_name, sort_metric="created_at", sort_order=1):  # 1 for ascending, -1 for descending
        user_info = self.get_user_ids_by_username(search_name)
        tweets = self.search_tweets_username(user_info)

        data = [{
            'Name': user_info[tweet['user']],
            'User ID': tweet['user'],
            'Tweet Text': tweet['text'],
            'language':tweet['lang'],
            'Retweet Count': tweet.get('retweet_count', 0),
            'Favorite Count': tweet.get('favorite_count', 0),
            'Reply Count': tweet.get('reply_count', 0),
            'Timestamp': tweet.get('created_at', ''),
            'Quoted Status' :tweet.get('is_quoted_status',False),
            'Retweet Status':tweet.get('is_retweet_status',False)
        } for tweet in tweets if tweet['user'] in user_info]

        df = pd.DataFrame(data)
        # Calculate total engagement as the sum of retweets, favorites, and replies
        df['Total Engagement'] = df['Retweet Count'] + df['Favorite Count'] + df['Reply Count']
        sort_column = {'timestamp': 'Timestamp', 'retweet': 'Retweet Count', 'favorite': 'Favorite Count', 'engagement': 'Total Engagement'}[sort_metric]
        return df.sort_values(by=[sort_column], ascending=(sort_order == 1))
    
    
    # search and sort users based on followers count or last posted timestamp
    def search_and_sort_users(self, search_term, sort_by='followers_count', order='desc'):
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return pd.DataFrame()  # Return an empty DataFrame if no connection
        
        order_by = "DESC" if order == 'desc' else "ASC"
        query = f"""
        SELECT id_str as `User ID`, 
               name as `Name`, 
               followers_count as `Followers Count`, 
               last_post_timestamp as `Last Post Timestamp`
        FROM users 
        WHERE name LIKE CONCAT('%', %s, '%') 
        ORDER BY {sort_by} {order_by};
        """
        cursor = self.mysql_connection.cursor()
        try:
            cursor.execute(query, (search_term,))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            return df
        except mysql.connector.Error as e:
            print(f"Error fetching user data: {e}")
            return pd.DataFrame()
        finally:
            cursor.close()   
            
              
     # Function to get the time range 
    def get_time_limit(self, time_frame):
        """ Calculate the starting date and time for a given time frame with timezone-aware datetime objects. """
        utc = pytz.utc
        now = datetime.now(utc)  # Get the current time in UTC
        if time_frame == '1day':
            return now - timedelta(days=1)
        elif time_frame == '1week':
            return now - timedelta(weeks=1)
        elif time_frame == '1month':
            return now - timedelta(days=30)
        else:
            return None    
        