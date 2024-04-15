import json
from datetime import datetime
import logging.config

from connections import get_mongodb_conn, get_mysql_conn

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

class TweetDataProcessor:
    def __init__(self):
        self.mysql_conn = get_mysql_conn()
        self.create_user_tb_mysql()
        self.tweet_collection = get_mongodb_conn(collection="tweet_data")
        self.user_collection = get_mongodb_conn(collection="user_data")

    def fetch_user(self, user_data, post_creation):
        user = {"id_str": user_data["id_str"], "name": user_data["name"].replace("'", "\\'").replace('"', '\\"'),
            "screen_name": user_data["screen_name"].replace("'", "\\'").replace('"', '\\"'),
            "protected": user_data["protected"], "verified": user_data["verified"],
            "followers_count": user_data["followers_count"], "friends_count": user_data["friends_count"],
            "listed_count": user_data["listed_count"], "favourites_count": user_data["favourites_count"],
            "statuses_count": user_data["statuses_count"], "created_at": self.parse_datetime(user_data["created_at"]),
            "last_post_timestamp": self.parse_datetime(post_creation)}
        return user

    def create_user_tb_mysql(self):
        sql_setup = """CREATE TABLE IF NOT EXISTS users (
                        id_str VARCHAR(255) NOT NULL,
                        name VARCHAR(255),
                        screen_name VARCHAR(255),
                        protected BOOLEAN,
                        verified BOOLEAN,
                        followers_count INT,
                        friends_count INT,
                        listed_count INT,
                        favourites_count INT,
                        statuses_count INT,
                        created_at TIMESTAMP,
                        last_post_timestamp TIMESTAMP,
                        PRIMARY KEY (id_str),
                        INDEX (name),
                        INDEX (screen_name)
                        );
                    """
        try:
            self.mysql_conn.cursor().execute(sql_setup)
            self.mysql_conn.commit()
        except Exception as e:
            print(f"The error '{e}' occurred while creating table userdata in MySQL DB.")

    def insert_into_user_tb_mysql(self, user):

        # Create table userdata if it does not exist yet
        self.create_user_tb_mysql()

        # Insert or update userdata records using user object
        sql_insertion = f"""
        REPLACE INTO users 
        (
            id_str,
            name,
            screen_name,
            protected,
            verified,
            followers_count,
            friends_count,
            listed_count,
            favourites_count,
            statuses_count,
            created_at,
            last_post_timestamp
        ) VALUES
        (
            '{user["id_str"]}',
            '{user["name"]}',
            '{user["screen_name"]}',
            {user['protected']},
            {user['verified']},
            {user['followers_count']},
            {user['friends_count']},
            {user['listed_count']},
            {user['favourites_count']},
            {user['statuses_count']},
            TIMESTAMP('{user["created_at"]}'),
            GREATEST(COALESCE(last_post_timestamp,'1000-01-01 00:00:00'), TIMESTAMP('{user["last_post_timestamp"]}'))
        );
        """
        try:
            self.mysql_conn.cursor().execute(sql_insertion)
            self.mysql_conn.commit()
        except Exception as e:
            print(f"The error '{e}' occurred while inserting a record into table userdata in MySQL DB.")

    @staticmethod
    def parse_datetime(timestamp_str):
        return datetime.strftime(datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y'), '%Y-%m-%d %H:%M:%S')

    def process_hashtag(self, hashtags: list, tweet_id: str, user_id: str):
        """
        Function to save hashtags in MySQL db
        Args:
            hashtags: List of hashtags to be saved
            tweet_id: Tweet in which the hashtag was mentioned
            user_id: User who used the hashtag

        Returns:

        """
        for hashtag in hashtags:
            logger.info(f"Saving hashtag: {hashtag['text']}")
            self.mysql_conn.cursor().execute(f"""
            INSERT INTO hashtags (hashtag, user_id, tweet_id) VALUES ('{hashtag["text"]}', '{user_id}', '{tweet_id}')
            """)
        self.mysql_conn.commit()

    def process_tweet(self, tweet_data: dict) -> None:
        """
        Function to process and save tweet to MongoDB
        Args:
            tweet_data: Dict have tweet data
        Returns: None
        """
        logger.info(f"Processing Tweet: {tweet_data['id_str']}")
        keys_to_be_dropped = ["id", "geo", "favorited", "retweeted", "filter_level", "quoted_status_id"]
        for key in keys_to_be_dropped:
            tweet_data.pop(key, None)

        tweet_data["user"] = tweet_data["user"]["id_str"]

        hashtags = tweet_data.get("entities", {}).get("hashtags")
        if hashtags:
            self.process_hashtag(hashtags=hashtags, tweet_id=tweet_data["id_str"], user_id=tweet_data["user"])
        self.tweet_collection.insert_one(tweet_data)

    def process_reply_user_mysql(self, tweet_data: dict) -> None:
        """
        Function to process and save user in_reply_to_user_id_str information into MySQL
        Args:
            tweet_data: Dict have tweet data
        Returns: None
        """
        logger.info(f"Processing User into MySQL: {tweet_data['in_reply_to_user_id_str']}")
        
        ## For reply users not inserted in database yet, name will be equal to screen_name 
        sql_insertion = f"""
        REPLACE INTO users 
        (id_str, name, screen_name, protected, verified, followers_count,
        friends_count, listed_count, favourites_count, statuses_count,
        created_at, last_post_timestamp
        ) VALUES (
        '{tweet_data['in_reply_to_user_id_str']}', 
        '{tweet_data['in_reply_to_screen_name']}', 
        '{tweet_data['in_reply_to_screen_name']}', 
        null, null, null, null, null, null, null, null, null
        );
        """
        self.mysql_conn.cursor().execute(sql_insertion)
        self.mysql_conn.commit()

    def process_user_mysql(self, tweet_data: dict, user_data: dict) -> None:
        """
        Function to process and save user information into MySQL
        Args:
            tweet_data: Dict have tweet data
            user_data: Dict have user data
        Returns: None
        """
        logger.info(f"Processing User into MySQL: {user_data['id_str']}")

        user_data['name'] = user_data['name'].replace("'", "\\'").replace('"', '\\"')
        user_data['screen_name'] = user_data['screen_name'].replace("'", "\\'").replace('"', '\\"')
        
        sql_insertion = f"""
        REPLACE INTO users 
        (id_str, 
        name, 
        screen_name, 
        protected, 
        verified, 
        followers_count,
        friends_count, 
        listed_count, 
        favourites_count, 
        statuses_count,
        created_at, 
        last_post_timestamp
        ) VALUES (
        '{user_data['id_str']}', 
        '{user_data['name']}',
        '{user_data['screen_name']}',
        {user_data['protected']}, 
        {user_data['verified']}, 
        {user_data['followers_count']},
        {user_data['friends_count']}, 
        {user_data['listed_count']}, 
        {user_data['favourites_count']},
        {user_data['statuses_count']}, 
        TIMESTAMP('{self.parse_datetime(user_data['created_at'])}'),
        GREATEST(COALESCE(last_post_timestamp,'1000-01-01 00:00:00'), TIMESTAMP('{self.parse_datetime(tweet_data['created_at'])}'))
        );
        """
        self.mysql_conn.cursor().execute(sql_insertion)
        self.mysql_conn.commit()

    def process_user_mongodb(self, user_data: dict) -> None:
        """
        Function to process and save user information into MongoDB
        Args:
            user_data: Dict have user data
        Returns: None
        """
        logger.info(f"Processing User into MongoDB: {user_data['id_str']}")

        user = {}
        user['id_str'] = user_data['id_str']
        user['reply_users'] = []
        user['quoted_users'] = []
        user['retweeted_users'] = []
        user['replied_by_users'] = []
        user['quoted_by_users'] = []
        user['retweeted_by_users'] = []

        self.user_collection.update_one({'id_str': user['id_str']}, 
                                        {'$setOnInsert': user}, upsert=True) 
        
    def set_relationship_mongodb(self, id_A, id_B, field_A, field_B) -> None:
        """
        Function to add users id into relationship lists in MongoDB
        Args:
            id_A: string, user id A
            id_B: string, user id B
            field_A: string, relationship list field name in user id A
            field_B: string, relationship list field name in user id B
        Returns: None
        """
        self.user_collection.update_one({'id_str': id_A}, 
                {'$addToSet': { field_A: id_B } })
        
        self.user_collection.update_one({'id_str': id_B }, 
                {'$addToSet': { field_B: id_A } })       

    def process_data(self, file_path: str):

        with open(file_path, 'r') as file:
            for line in file:
                if line != '\n':
                    
                    data = json.loads(line)

                    # Process User into MySQL
                    self.process_user_mysql(tweet_data=data, user_data=data['user'])

                    # Process User into MongoDB
                    self.process_user_mongodb(user_data=data['user'])

                    data["is_retweet_status"] = False

                    if data['in_reply_to_user_id_str'] is not None:
                        
                        # Process User into MySQL
                        self.process_reply_user_mysql(data)

                        # Process User into MongoDB
                        self.process_user_mongodb(user_data={'id_str': data['in_reply_to_user_id_str']})

                        # Add user ids to relationship lists
                        self.set_relationship_mongodb(id_A = data['user']['id_str'],
                                                      id_B = data['in_reply_to_user_id_str'],
                                                      field_A = 'reply_users',
                                                      field_B = 'replied_by_users')

                    if 'retweeted_status' in data:

                        try:
                            # Process User into MySQL
                            self.process_user_mysql(tweet_data=data['retweeted_status'], 
                                                    user_data=data['retweeted_status']['user'])
                            
                            # Process User into MongoDB
                            self.process_user_mongodb(user_data=data['retweeted_status']['user'])

                            # Add user ids to relationship lists
                            self.set_relationship_mongodb(id_A = data['user']['id_str'],
                                                        id_B = data['retweeted_status']['user']['id_str'],
                                                        field_A = 'retweeted_users',
                                                        field_B = 'retweeted_by_users')                            

                            # Process Tweet
                            self.process_tweet(tweet_data=data["retweeted_status"])
                            data["is_retweet_status"] = True
                            data["retweeted_status_id_str"] = data["retweeted_status"]["id_str"]
                            data.pop("retweeted_status")

                        except Exception as e:
                            print(f"Error processing retweet: {e}")

                    if 'quoted_status' in data:

                        try:
                            # Process User into MySQL
                            self.process_user_mysql(tweet_data=data['quoted_status'], 
                                                    user_data=data['quoted_status']['user'])
                            
                            # Process User into MongoDB
                            self.process_user_mongodb(user_data=data['quoted_status']['user'])

                            # Add user ids to relationship lists
                            self.set_relationship_mongodb(id_A = data['user']['id_str'],
                                                        id_B = data['quoted_status']['user']['id_str'],
                                                        field_A = 'quoted_users',
                                                        field_B = 'quoted_by_users')

                            # Process Tweet
                            self.process_tweet(tweet_data=data["quoted_status"])
                            data.pop("quoted_status")

                        except Exception as e:
                            print(f"Error processing quoted tweet: {e}")

                    self.process_tweet(tweet_data=data)
