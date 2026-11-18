import logging
import re
from urllib.parse import quote

import requests

from core.crawler import Crawler

logger = logging.getLogger(__name__)
API_URL = "https://api.x.com/2"


def clean_tweet(tweet: str) -> str:
    without_mentions = re.sub(r"@\w+", "", tweet)
    return re.sub(r"\s+", " ", without_mentions).strip()


def _get_json(session, path, params):
    response = session.get(f"{API_URL}/{path}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_tweets(session, query, num_tweets):
    params = {
        "query": query,
        "tweet.fields": "created_at,public_metrics,author_id,lang",
        "expansions": "author_id",
        "user.fields": "username",
    }
    remaining = num_tweets
    while remaining > 0:
        params["max_results"] = max(10, min(50, remaining))
        page = _get_json(session, "tweets/search/recent", params.copy())
        authors = {
            user["id"]: user["username"]
            for user in page.get("includes", {}).get("users", [])
        }
        tweets = page.get("data", [])[:remaining]
        for tweet in tweets:
            yield tweet, authors.get(tweet.get("author_id"))
        remaining -= len(tweets)
        next_token = page.get("meta", {}).get("next_token")
        if not next_token:
            break
        params["next_token"] = next_token


class TwitterCrawler(Crawler):
    def crawl(self) -> None:
        config = self.cfg.twitter_crawler
        num_tweets = config.num_tweets
        userhandles = config.userhandles
        clean_tweets = config.get("clean_tweets", True)

        with requests.Session() as session:
            session.headers["Authorization"] = f"Bearer {config.twitter_bearer_token}"
            for username in userhandles:
                profile = _get_json(
                    session,
                    f"users/by/username/{quote(username, safe='')}",
                    {"user.fields": "created_at,public_metrics,description,location,verified"},
                ).get("data")
                if not profile:
                    logger.warning("Could not look up Twitter profile %s", username)
                    continue

                metrics = profile.get("public_metrics") or {}
                doc = {
                    "id": "tweets-" + username,
                    "title": f"top {num_tweets} tweets of {username}",
                    "metadata": {
                        "url": f"https://twitter.com/{username}",
                        "source": "twitter",
                        "username": username,
                        "account_created_at": profile.get("created_at"),
                        "followers_count": metrics.get("followers_count"),
                        "following_count": metrics.get("following_count"),
                        "tweet_count": metrics.get("tweet_count"),
                        "description": profile.get("description"),
                        "location": profile.get("location"),
                        "verified": profile.get("verified"),
                    },
                    "sections": [],
                }
                query = f"@{username} -is:retweet"
                for tweet, author in fetch_tweets(session, query, num_tweets):
                    text = tweet["text"]
                    metrics = tweet.get("public_metrics") or {}
                    doc["sections"].append({
                        "text": clean_tweet(text) if clean_tweets else text,
                        "metadata": {
                            "author": author,
                            "created_at": tweet.get("created_at"),
                            "retweet_count": metrics.get("retweet_count"),
                            "reply_count": metrics.get("reply_count"),
                            "like_count": metrics.get("like_count"),
                            "quote_count": metrics.get("quote_count"),
                            "lang": tweet.get("lang"),
                        },
                    })
                if self.indexer.index_document(doc):
                    logger.info("Indexed tweets for %s", username)
                else:
                    logger.warning("Error indexing tweets for %s", username)
        logger.info("Finished indexing all users (total=%s)", len(userhandles))
