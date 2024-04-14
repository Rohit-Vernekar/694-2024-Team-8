import mysql.connector
from mysql.connector import Error
import pymongo
from pymongo import MongoClient
from datetime import datetime


class TwitterQueries:
    def search_tweets_by_string(db, text_query, start_date, end_date):
        """ Search for tweets containing a specific string within a time range. """
    query = {
        "text": {"$regex": text_query, "$options": "i"},
        "created_at": {"$gte": start_date, "$lte": end_date}
    }
    results = db.tweet_data.find(query)
    return list(results)
   
  