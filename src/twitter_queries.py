import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient

class TwitterQueries:
    def __init__(self, mysql_config, mongo_uri, mongo_db_name):
        # Initialize MySQL and MongoDB connections when the class instance is created
        self.mysql_connection = self._create_mysql_connection(mysql_config)
        self.mongo_db = self._create_mongo_connection(mongo_uri, mongo_db_name)

    
    # the connection codes can be called from the other class
    
    
    def _create_mysql_connection(self, config):
        # Private method to create a MySQL connection
        try:
            connection = mysql.connector.connect(**config)
            return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None

    def _create_mongo_connection(self, uri, db_name):
        # Private method to create a MongoDB connection
        client = MongoClient(uri)
        return client[db_name]


    # the below function helps fetch the tweets by users.

    def get_user_ids_by_name(self, user_name):
        """
        Fetch user IDs from MySQL based on a user's name, allowing for partial matches and treating special
        characters like underscores as literals.

        :param user_name: String, the part of the name of the user to search for.
        :return: List of user IDs or an empty list if no user is found.
        """
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        try:
            # Prepare the username for the query
            modified_name = user_name.strip().replace(' ', '%').replace('_', '\\_')
            query = "SELECT user_id FROM users WHERE name LIKE %s ESCAPE '\\';"
            cursor = self.mysql_connection.cursor()
            search_pattern = f"%{modified_name}%"
            cursor.execute(query, (search_pattern,))
            results = cursor.fetchall()
            cursor.close()
            return [result[0] for result in results] if results else []
        except Error as e:
            print(f"Error fetching user IDs: {e}")
            return []

    def fetch_tweets_by_user_ids(self, user_ids, order='latest'):
        """
        Fetch tweets for specific user IDs from MongoDB, sorted by created date.

        :param user_ids: List of user IDs (already in string format).
        :param order: String, sorting order of tweets ('latest' or 'oldest').
        :return: List of tweets or empty list if none found.
        """
        if not self.mongo_db:
            print("MongoDB connection is not established.")
            return []
        query = {"user": {"$in": user_ids}}
        sort_order = -1 if order == 'latest' else 1
        tweets = list(self.mongo_db.tweet_data.find(query).sort("created_at", sort_order))
        return tweets


    # the below function fetches the tweets by hashtags


    def fetch_tweet_ids_by_hashtag(self, hashtag_query):
        """
        Fetch tweet IDs based on a hashtag from MySQL.

        :param hashtag_query: String, the hashtag to search for.
        :return: List of tweet IDs or an empty list if none found.
        """
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        try:
            query = "SELECT tweet_id FROM hashtags WHERE hashtag = %s;"
            cursor = self.mysql_connection.cursor()
            cursor.execute(query, (hashtag_query,))
            results = cursor.fetchall()
            cursor.close()
            return [result[0] for result in results] if results else []
        except Error as e:
            print(f"Error fetching tweet IDs: {e}")
            return []

    def fetch_tweets_by_tweet_ids(self, tweet_ids):
        """
        Fetch tweets from MongoDB using tweet IDs.

        :param tweet_ids: List of tweet IDs to fetch.
        :return: List of tweets or empty list if none found.
        """
        if not self.mongo_db:
            print("MongoDB connection is not established.")
            return []
        query = {"id_str": {"$in": tweet_ids}}
        tweets = list(self.mongo_db.tweet_data.find(query))
        return tweets

    def search_tweets_by_hashtag(self, hashtag_query):
        """
        Main function to search tweets by hashtag.

        :param hashtag_query: String, the hashtag to search for.
        :return: List of tweets found for the hashtag.
        """
        tweet_ids = self.fetch_tweet_ids_by_hashtag(hashtag_query)
        if not tweet_ids:
            print("No tweets found for hashtag:", hashtag_query)
            return []
        return self.fetch_tweets_by_tweet_ids(tweet_ids)

    
    
    # this function fetches the results of the above functions and displays them ,a variable search_type has been added to handle code redundancy 
    
    def fetch_and_display_tweets(self, search_term, search_type='hashtag'):
        """
        Fetch tweet IDs from MySQL and corresponding tweet data from MongoDB based on a search term,
        which can be a hashtag or a user name, and display the results.

        :param search_term: String, the hashtag or user name to search for.
        :param search_type: String, type of search ('hashtag' or 'username').
        """
        if search_type == 'hashtag':
            tweet_ids = self.fetch_tweet_ids_by_hashtag(search_term)
            if not tweet_ids:
                print(f"No tweets found for hashtag: {search_term}")
                return
        elif search_type == 'username':
            user_ids = self.get_user_ids_by_name(search_term)
            if not user_ids:
                print(f"No users found for the name: {search_term}")
                return
            tweet_ids = user_ids  # Assume mapping user_id to tweet_id
        else:
            print(f"Invalid search type specified: {search_type}")
            return
        tweets = self.fetch_tweets_by_tweet_ids(tweet_ids)
        if tweets:
            print(f"Tweets for {search_type} '{search_term}':")
            for tweet in tweets:
                print(tweet)
        else:
            print(f"No tweets found for {search_type} '{search_term}'.")
