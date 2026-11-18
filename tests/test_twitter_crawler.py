import unittest
from unittest.mock import Mock, patch

from omegaconf import OmegaConf

from crawlers.twitter_crawler import TwitterCrawler, fetch_tweets


def json_response(payload):
    response = Mock()
    response.json.return_value = payload
    return response


class TestTwitterCrawler(unittest.TestCase):
    def test_fetch_tweets_paginates_and_honors_the_requested_limit(self):
        session = Mock()
        session.get.side_effect = [
            json_response({"data": [{"id": "1"}], "meta": {"next_token": "page-2"}}),
            json_response({
                "data": [{"id": "2", "author_id": "a"}, {"id": "3"}],
                "includes": {"users": [{"id": "a", "username": "writer"}]},
            }),
        ]

        tweets = list(fetch_tweets(session, "@reader -is:retweet", 2))

        self.assertEqual(tweets, [({"id": "1"}, None), ({"id": "2", "author_id": "a"}, "writer")])
        first, second = [call.kwargs["params"] for call in session.get.call_args_list]
        self.assertEqual(first["max_results"], 10)
        self.assertNotIn("next_token", first)
        self.assertEqual(second["next_token"], "page-2")

    def test_empty_search_returns_no_tweets(self):
        session = Mock()
        session.get.return_value = json_response({"meta": {"result_count": 0}})

        self.assertEqual(list(fetch_tweets(session, "@reader", 2)), [])

    @patch("crawlers.twitter_crawler.requests.Session")
    def test_crawl_indexes_account_and_author_metadata(self, session_factory):
        session = session_factory.return_value.__enter__.return_value
        session.headers = {}
        session.get.side_effect = [
            json_response({"data": {"public_metrics": {"followers_count": 5}}}),
            json_response({
                "data": [{
                    "text": "@reader Hello",
                    "author_id": "a",
                    "public_metrics": {"like_count": 2},
                }],
                "includes": {"users": [{"id": "a", "username": "writer"}]},
            }),
        ]
        crawler = TwitterCrawler.__new__(TwitterCrawler)
        crawler.cfg = OmegaConf.create({
            "twitter_crawler": {
                "twitter_bearer_token": "test-token",
                "num_tweets": 1,
                "userhandles": ["reader"],
            },
        })
        crawler.indexer = Mock()

        crawler.crawl()

        self.assertEqual(session.headers["Authorization"], "Bearer test-token")
        profile_call, search_call = session.get.call_args_list
        self.assertTrue(profile_call.args[0].endswith("/users/by/username/reader"))
        self.assertEqual(search_call.kwargs["params"]["query"], "@reader -is:retweet")
        doc = crawler.indexer.index_document.call_args.args[0]
        self.assertEqual(doc["id"], "tweets-reader")
        self.assertEqual(doc["metadata"]["followers_count"], 5)
        self.assertEqual(doc["sections"][0]["text"], "Hello")
        self.assertEqual(doc["sections"][0]["metadata"]["author"], "writer")
        self.assertEqual(doc["sections"][0]["metadata"]["like_count"], 2)
