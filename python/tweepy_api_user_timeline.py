# Imports
import tweepy
import json
from datetime import datetime
import config

# Config file variables
data_directory = config.data_directory["folder_path"]
consumer_key = config.twitter_auth["consumer_key"]
consumer_secret = config.twitter_auth["consumer_secret"]
access_token = config.twitter_auth["access_token"]
access_token_secret = config.twitter_auth["access_token_secret"]

# Other variables
now = datetime.today().strftime('%Y%m%d')

# Populate the following variable with your desired Twitter ID. For example, 44196397 = @elonmusk
twitter_id = 44196397

# Authenticate into Tweepy
auth = tweepy.OAuthHandler(
    consumer_key,
    consumer_secret)
auth.set_access_token(
    access_token,
    access_token_secret)
api = tweepy.API(auth)

# Define Tweepy method(s)
user_tweets = api.user_timeline(
    id=twitter_id, 
    count=100, 
    include_rts = True)
screen_name = api.get_user(id=twitter_id).screen_name.lower()

# Print raw tweets to file
with open('{}rawtweets_{}_{}.json'.format(data_directory, screen_name, now), 'w', encoding='utf-8') as f:
    for tweet in user_tweets:
        data = tweet._json
        json.dump(data, f,ensure_ascii=False, indent=4)