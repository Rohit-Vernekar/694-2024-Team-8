import pymongo
import mysql.connector
from datetime import datetime, timedelta
from mysql.connector import Error
from .connections import get_mysql_conn, get_mongodb_conn

class TwitterQueries:
    def __init__(self):
        self.mysql_connection = get_mysql_conn()
        try:
            self.mongo_db = get_mongodb_conn(collection="tweets")
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

    def get_user_ids_by_name(self, user_name):
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        try:
            query = "SELECT id_str FROM users WHERE name LIKE CONCAT('%', %s, '%');"
            with self.mysql_connection.cursor() as cursor:
                cursor.execute(query, (user_name,))
                results = cursor.fetchall()
                return [row[0] for row in results]
        except mysql.connector.Error as e:
            print(f"Error fetching user IDs: {e}")
            return []

    def fetch_tweets_by_criteria(self, mongo_query, sort_fields=None, time_frame=None, projection=None):
        if self.mongo_db is None:
            print("MongoDB connection is not established.")
            return []

        if time_frame:
            now = datetime.now()
            time_delta = {
                '1day': now - timedelta(days=1),
                '1week': now - timedelta(weeks=1),
                '1month': now - timedelta(days=30)
            }.get(time_frame, now)
            mongo_query["created_at"] = {"$gte": time_delta}

        sort_order = pymongo.DESCENDING if sort_fields and sort_fields.get('order', 'latest') == 'latest' else pymongo.ASCENDING
        results = list(self.mongo_db.find(mongo_query, projection if projection else {}).sort("created_at", sort_order))
        if not results:
            print("No tweets found for the criteria. Query was:", mongo_query)
        return results

    def search_tweets_by_username(self, username, sort_fields=None, time_frame=None):
        user_ids = self.get_user_ids_by_name(username)
        if not user_ids:
            print(f"No users found for the name: {username}")
            return []
        mongo_query = {"user": {"$in": user_ids}}
        projection = {"text": 1, "created_at": 1, "user": 1}
        return self.fetch_tweets_by_criteria(mongo_query, sort_fields, time_frame, projection)

    def search_tweets_by_hashtag(self, hashtag_query, sort_fields=None, time_frame=None):
        tweet_ids = self.fetch_tweet_ids_by_hashtag(hashtag_query)
        if not tweet_ids:
            print("No tweets found for hashtag:", hashtag_query)
            return []
        mongo_query = {"id_str": {"$in": tweet_ids}}
        projection = {"text": 1, "created_at": 1, "user": 1}
        return self.fetch_tweets_by_criteria(mongo_query, sort_fields, time_frame, projection)

    def search_and_sort_users(self, search_term, sort_by='followers_count', order='desc'):
        if not self.mysql_connection:
            print("MySQL connection is not established.")
            return []
        order_by = "DESC" if order == 'desc' else "ASC"
        query = f"SELECT id_str, name, followers_count, last_post_timestamp FROM users WHERE name LIKE CONCAT('%', %s, '%') ORDER BY {sort_by} {order_by};"
        with self.mysql_connection.cursor() as cursor:
            cursor.execute(query, (search_term,))
            results = cursor.fetchall()
            return [{'user_id': row[0], 'name': row[1], 'followers_count': row[2], 'last_post_timestamp': row[3]} for row in results]
