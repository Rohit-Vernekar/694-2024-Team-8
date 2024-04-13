import json
import connections
from datetime import datetime

class TweetDataProcessor:
    def __init__(self):
        self.my_sqlconnection = connections.get_mysql_conn()

    def fetch_user(self, user_data, post_creation):
        user = {
            "id_str": user_data["id_str"],
            "name": user_data["name"].replace("'", "\\'").replace('"', '\\"'),
            "screen_name": user_data["screen_name"].replace("'", "\\'").replace('"', '\\"'),
            "protected": user_data["protected"],
            "verified": user_data["verified"],
            "followers_count": user_data["followers_count"],
            "friends_count": user_data["friends_count"],
            "listed_count": user_data["listed_count"],
            "favourites_count": user_data["favourites_count"],
            "statuses_count": user_data["statuses_count"],
            "created_at": self.parse_datetime(user_data["created_at"]),
            "last_post_timestamp": self.parse_datetime(post_creation)
        }
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
            self.my_sqlconnection.cursor().execute(sql_setup)
            self.my_sqlconnection.commit()
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
            "{user['id_str']}",
            "{user['name']}",
            "{user['screen_name']}",
            {user['protected']},
            {user['verified']},
            {user['followers_count']},
            {user['friends_count']},
            {user['listed_count']},
            {user['favourites_count']},
            {user['statuses_count']},
            TIMESTAMP("{user['created_at']}"),
            GREATEST(COALESCE(last_post_timestamp,'1000-01-01 00:00:00'), TIMESTAMP("{user['last_post_timestamp']}"))
        );
        """
        try:
            self.my_sqlconnection.cursor().execute(sql_insertion)
            self.my_sqlconnection.commit()
        except Exception as e:
            print(f"The error '{e}' occurred while inserting a record into table userdata in MySQL DB.")


    def fetch_tweet(self, tweet_data):
        tweet = {
            "tweet_id": tweet_data["id"],
            "user_id": tweet_data["user"]["id"],
            "created_at": tweet_data["created_at"],
        }
        return tweet

    def parse_datetime(self, timestamp_str):
        return datetime.strftime(datetime.strptime(timestamp_str, 
                                '%a %b %d %H:%M:%S %z %Y'),
                                '%Y-%m-%d %H:%M:%S')

    def data_processing(self, file_path):


        user_data = []
        tweet_data = []
        
        with open(file_path, 'r') as file:

            for line in file:

                if line != '\n':

                    data = json.loads(line)
                    
                    # Insert user record into MySQL
                    user = self.fetch_user(data['user'], data['created_at'])
                    self.insert_into_user_tb_mysql(user)

                    #tweet = self.fetch_tweet(data)
                    
                    if 'retweeted_status' in data:
                          
                        # Handling retweet
                        try:
                            # Insert original retweet user record into MySQL
                            user = self.fetch_user(data['retweeted_status']['user'], data['retweeted_status']['created_at'])
                            self.insert_into_user_tb_mysql(user)

                            #tweet_data_rt = self.fetch_tweet(data['retweeted_status'])
                            #tweet_data_rt["retweet_id"] = None  # Indicate this tweet is a retweet
                            #tweet_data.append(tweet_data_rt)  # Add the retweeted tweet

                        except Exception as e:
                            print(f"Error processing retweet: {e}")

                    else:
                        #tweet["retweet_id"] = None
                        pass

                    if 'quoted_status' in data:
                        
                        # Handling quoted tweet

                        try:

                            # Insert original quote tweet user record into MySQL
                            user = self.fetch_user(data['quoted_status']['user'], data['quoted_status']['created_at'])
                            self.insert_into_user_tb_mysql(user)

                            # Do something about quote tweets here
                            ###############################

                        except Exception as e:
                            print(f"Error processing quoted tweet: {e}")

                    else:
                        #tweet["quoted_tweet_id"] = None
                        pass
                    
                    

        return user_data, tweet_data



