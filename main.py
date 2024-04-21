from src.tweet_data_processor import TweetDataProcessor
from src.twitter_queries import TwitterQueries

# obj = TweetDataProcessor()
# obj.process_data(file_path="/Users/rohitvernekar/My Drive/Rutgers/DBMS/Project/corona-out-2")


twitter_queries = TwitterQueries()
#
# print("Testing for tweets by username")
# user_name = "NUFF"
# user_info = twitter_queries.get_user_data_by_username(user_name)
#
# # Search for tweets by the user from the last week
# tweets = twitter_queries.search_tweets_username(user_info, '1week')
# print(tweets)
#
# print("Testing for searching users")
# df_users_by_followers_desc = twitter_queries.search_and_sort_users('bob', sort_by='followers_count', order='desc')
# print("Users sorted by followers count (descending):")
# print(df_users_by_followers_desc)
#
# print("Testing for Hashtag")
# df_hashtag_tweets = twitter_queries.search_tweets_by_hashtag('covid19')
# print(df_hashtag_tweets)
#
# print("Testing for engagement tweets")
# df_popular_tweets = twitter_queries.search_popular_tweets_based_on_engagement('1month')
# print(df_popular_tweets)
#
# print("testing for search by keywords and sorting by timerange parameter")
# print(twitter_queries.search_tweets_by_keyword("death", '1week'))
#
# print(twitter_queries.get_trending_hashtags())
