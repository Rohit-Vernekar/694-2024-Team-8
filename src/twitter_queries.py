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
        
         # Define the new method to search tweets by hashtag
    def search_tweets_by_hashtag(self, hashtag, time_frame=None):
        tweet_ids = self.fetch_tweet_ids_from_mysql(hashtag)
        if not tweet_ids:
            print("No tweet IDs found for hashtag:", hashtag)
            return pd.DataFrame()  # Return an empty DataFrame if no IDs are found

        time_limit = self.get_time_limit(time_frame) if time_frame else None
        query = {"id_str": {"$in": tweet_ids}}
        if time_limit:
            query["created_at"] = {"$gte": time_limit.isoformat()}

        tweets = self.fetch_tweets_from_mongodb(tweet_ids)  # Make sure to pass tweet_ids here, not query
        if not tweets:
            print("No tweets found in MongoDB matching the tweet IDs from MySQL.")
            return pd.DataFrame()

        data = [{
            'User ID': tweet.get('user', 'Unknown'),
            'Tweet Text': tweet.get('text', ''),
            'Retweet Count': tweet.get('retweet_count', 0),
            'Favorite Count': tweet.get('favorite_count', 0),
            'Reply Count': tweet.get('reply_count', 0),
            'Timestamp': tweet.get('created_at', '')
        } for tweet in tweets]

        return pd.DataFrame(data)

    def fetch_tweet_ids_from_mysql(self, hashtag):
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        try:
            cursor = self.mysql_connection.cursor()
            query = "SELECT tweet_id FROM hashtags WHERE hashtag = %s"
            cursor.execute(query, (hashtag,))
            tweet_ids = [item[0] for item in cursor.fetchall()]
            cursor.close()
            return tweet_ids
        except Error as e:
            print(f"Error fetching tweet IDs: {e}")
            return []

    def fetch_tweets_from_mongodb(self, tweet_ids):
        if self.mongo_db is None:
            print("MongoDB connection is not established.")
            return []

        # Check if tweet_ids list is empty to avoid MongoDB errors
        if not tweet_ids:
            print("No tweet IDs provided to fetch from MongoDB.")
            return []

        # Proceed with fetching tweets if there are valid tweet IDs
        return list(self.mongo_db.find({"id_str": {"$in": tweet_ids}}, {
            "text": 1, 
            "user": 1, 
            "retweet_count": 1, 
            "favorite_count": 1, 
            "reply_count": 1,
            "created_at": 1
        }))
        
        
           
    #Popular tweets based on engagement metrics(Top 10)
    def search_popular_tweets_based_on_engagement(self, time_frame=None):
        if self.mongo_db is None:
            print("MongoDB connection is not established.")
            return pd.DataFrame()

        time_limit = self.get_time_limit(time_frame) if time_frame else None
        pipeline = [
            {
                '$match': {"created_at": {"$gte": time_limit.isoformat()}} if time_limit else {}
            },
            {
                '$project': {
                    'text': 1,
                    'user': 1,
                    'quote_count': {'$ifNull': ['$quote_count', 0]},
                    'reply_count': {'$ifNull': ['$reply_count', 0]},
                    'retweet_count': {'$ifNull': ['$retweet_count', 0]},
                    'favorite_count': {'$ifNull': ['$favorite_count', 0]},
                    'total_engagement': {
                        '$add': [
                            '$quote_count', '$reply_count', '$retweet_count', '$favorite_count'
                        ]
                    }
                }
            },
            {
                '$sort': {'total_engagement': -1}
            },
            {
                '$limit': 10
            }
        ]

        try:
            results = list(self.mongo_db.aggregate(pipeline))
            if not results:
                print("No tweets found with high engagement.")
                return pd.DataFrame()

            return pd.DataFrame(results)
        except pymongo.errors.OperationFailure as e:
            print(f"Error fetching tweets based on engagement: {e}")
            return pd.DataFrame()   
        
        