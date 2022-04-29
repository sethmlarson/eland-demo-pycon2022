import datetime
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import tweepy
from dateutil import tz

TWITTER_BEARER_TOKEN = os.environ["TWITTER_BEARER_TOKEN"]
CLOUD_ID = os.environ["CLOUD_ID"]
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(USERNAME, PASSWORD)
)

tw = tweepy.Client(
    bearer_token=TWITTER_BEARER_TOKEN
)

def tweets_to_document() -> dict:
    global tw

    tweets: tweepy.Response
    next_token = None
    tweets_fetched = 0

    while tweets_fetched < 1000:
        tweets = tw.search_recent_tweets(
            query="(#pycon OR #pyconus OR #pyconus2020 OR \"PyCon\") -is:retweet",
            expansions=["author_id"],
            tweet_fields=["created_at", "lang", "public_metrics", "possibly_sensitive"],
            sort_order="recency",
            max_results=100,
            start_time=(datetime.datetime.now(tz=tz.UTC) - datetime.timedelta(days=1)).isoformat(),
            next_token=next_token
        )
        tweets_fetched += tweets.meta["result_count"]
        next_token = tweets.meta.get("next_token", None)

        # Create a mapping of ID -> User for use in expanding author_id -> handle
        users_by_id = {str(user["id"]): user for user in tweets.includes["users"]}

        for tweet in tweets.data:

            # Skip any tweets that have a chance of being sensitive.
            if tweet.possibly_sensitive:
                continue

            # Transform the author_id into an richer object from 'includes.users'
            author = users_by_id[str(tweet.author_id)]

            # Yield the index action to bulk
            yield {
                "_id": str(tweet.id),
                "tweet_id": str(tweet.id),
                "@timestamp": tweet.created_at,
                "content": tweet.text,
                "author": author.username,
                "language": tweet.lang,
                "retweets": tweet.public_metrics["retweet_count"],
                "replies": tweet.public_metrics["reply_count"],
                "likes": tweet.public_metrics["like_count"],
            }

        # There were no results after this, we stop here.
        if next_token is None:
            break

    print(f"Indexed {tweets_fetched} tweets")

# Grab tweets and turn them into bulk index actions
actions = tweets_to_document()
bulk(client=es, actions=actions, index="tweets")
