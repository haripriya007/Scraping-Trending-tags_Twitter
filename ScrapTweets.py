# __author__ = 'Harimsv007'
#fetch tweets using twitter search API#

import tweepy #pip install tweepy
import json # pip install json
import pymongo # pip install pymongo
import pandas as pd # pip install pandas
import datetime #pip install DateTime
import time
from dateutil import parser #pip install parser
from datetime import date,timedelta #pip install datedelta
import os
from datetime import datetime,tzinfo,timedelta #pip install pytz
class Zone(tzinfo):
    def __init__(self,offset,isdst,name):
        self.offset = offset
        self.isdst = isdst
        self.name = name
    def utcoffset(self, dt):
        return timedelta(hours=self.offset) + self.dst(dt)
    def dst(self, dt):
            return timedelta(hours=1) if self.isdst else timedelta(0)
    def tzname(self,dt):
         return self.name
def get_Full_Tweet (results ,collection,df):
    DateList = []
    Tweets = []
    Hashtags = []
    print(len(results))
    for tweet_info in results:
        if "retweeted_status" in dir(tweet_info):
            tweet=tweet_info.retweeted_status.full_text
            print(tweet)
        else:
            tweet=tweet_info.full_text
            print(tweet)
        # URL = getURL(tweet)
        # ProcessedTweet = processTweet(tweet)
        # print(tweet_info)
        ent = tweet_info.entities
        print(ent)
        hashtags = ent["hashtags"]
        tagList = []
        for hashtag in hashtags:
            print(hashtag['text'])
            tagList.append(hashtag['text'])
        tags = ",".join(tagList)
        print(tags)
        DateList.append(tweet_info.created_at)
        RetweetCount = tweet_info.retweet_count

        FavouriteCount = tweet_info.favorite_count
        Tweets.append(tweet)
        Hashtags.append(tags)
        dataToWrite = {'Date':tweet_info.created_at,'Tweets':tweet,'hashtags':tags,'RetweetCount' : str(RetweetCount) , 'FavouriteCount' : str(FavouriteCount)}

        parseTime = parser.parse(str(tweet_info.created_at))
        ISO_timeformat = parseTime.replace(tzinfo=GMT)
        document = {"TWEET_TIMESTAMP": ISO_timeformat, "TWEET_TEXT": tweet, "TWEET_HASHTAGS": tags,
                    "TWEET_RETWEET_COUNT": RetweetCount, "TWEET_FAVOURITE_COUNT": FavouriteCount}
        if(InsertToDB):
            collection.update(document, document, upsert=True)
        else:
            df = df.append(document, ignore_index=True)

    return df
client = pymongo.MongoClient("")#connection address
database = client["Tweets"]#Database Collections
collection = database["Trending_Tweets"]#Collection Name of Database
#diseaseMaster = database["Prophetico_Health_DieseaseMaster"]
GMT = Zone(0,False,'GMT')
maxTweets = 10000000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
#collect tweets from since date to till date
# untill = date.today()
# since = untill - timedelta(1)
untill = " "    #"2019-09-15" Enter the date
since = " "   #"2019-09-16"
InsertToDB = 0
localPath = "C:\Users\admin\Downloads" #Set Your Local Path
print(untill)
print(since)
#Twitter API keys and tokens
consumer_key = "" #Twitter API Keys like "trAv36dtK0aMZfOyLCP6khLg9OdeMMTXBJagjYM8Ik1fz"
consumer_secret = "" #Twitter API Keys
access_token = "" #Twitter API Keys
access_token_secret = "" #Twitter API Keys
# Creating and setting the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
lang = "en"
# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None
# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -100000
tweetCount = 0
hashtags = []
hashtags = ["blockchain"]
for hashtag in hashtags:
    df = pd.DataFrame()
    max_id = 0
    searchQuery = hashtag.lower()
    print(hashtag)
    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                if (not sinceId):
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry,since=str(since), until=str(untill),tweet_mode="extended",lang = "en")
                else:
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry, since_id=sinceId,since=str(since), until=str(untill),tweet_mode="extended",lang = "en")
            else:
                if (not sinceId):
                    print(max_id)
                    print(max_id - 1)
                    new_tweets = api.search(q=searchQuery, lang="en", count=tweetsPerQry, since=str(since),
                                        until=str(untill), tweet_mode="extended",
                                        max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=searchQuery, lang="en", count=tweetsPerQry,
                                        max_id=str(max_id - 1), since=str(since), until=str(untill),
                                        tweet_mode="extended",
                                        since_id=sinceId)
            if not new_tweets:
                len(list(new_tweets))
                print("No more tweets found")
                # continue
                break

            # write full tweets
            df = get_Full_Tweet(new_tweets, collection,df)
            tweetCount += len(list(new_tweets))

            print("Downloaded {0} tweets".format(tweetCount))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            print("waiting for API response")
            time.sleep(60 * 15)
            continue
            print("some error : " + str(e))
        # break
    directory = "C:\Users\admin\Downloads"+ since+"-"+untill+"//" #CSV file Location
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(directory+hashtag+".csv")
    print("Downloaded {0} tweets, Saved to {1}".format(tweetCount, hashtag))
