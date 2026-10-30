"""SEC-005 regression tests for legacy fork RSS fault tolerance."""

import logging
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

from crawlers.rss_feed_utils import extract_feed_entries


class RssFeedFallbackTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 31, 8, 0)
        self.cutoff = self.now - timedelta(days=7)

    def test_malformed_feed_does_not_drop_valid_items(self):
        feed = {"bozo": True, "entries": [
            {"title": "No URL", "published_parsed": None},
            {"link": "https://example.org/one"},
            {"link": "https://example.org/two", "title": "Two"},
        ]}
        with self.assertLogs("crawlers.rss_feed_utils", logging.WARNING) as logs:
            entries = extract_feed_entries(feed, "https://feed.example", self.cutoff, self.now)
        self.assertEqual([(row[0], row[1]) for row in entries], [
            ("https://example.org/one", "No Title"), ("https://example.org/two", "Two")
        ])
        self.assertTrue(any("Malformed" in message for message in logs.output))
        self.assertTrue(any("without link" in message for message in logs.output))

    def test_invalid_date_falls_back_to_undated(self):
        feed = SimpleNamespace(entries=[
            SimpleNamespace(link="https://example.org/a", title="A",
                            published_parsed="not-a-date")
        ])
        with self.assertLogs("crawlers.rss_feed_utils", logging.WARNING):
            result = extract_feed_entries(feed, "rss", self.cutoff, self.now)
        self.assertEqual(result, [("https://example.org/a", "A", None)])

    def test_outside_window_is_ignored(self):
        old = (self.now - timedelta(days=15)).timetuple()
        fresh = (self.now - timedelta(days=2)).timetuple()
        feed = {"entries": [
            {"link": "https://example.org/old", "published_parsed": old},
            {"link": "https://example.org/new", "published_parsed": fresh},
        ]}
        entries = extract_feed_entries(feed, "rss", self.cutoff, self.now)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0][0], "https://example.org/new")

    def test_broken_entry_does_not_stop_following_entries(self):
        class BrokenEntry:
            def __getattr__(self, name):
                raise RuntimeError("corrupt entry")
        feed = {"entries": [BrokenEntry(), {"link": "https://example.org/good"}]}
        with self.assertLogs("crawlers.rss_feed_utils", logging.ERROR):
            entries = extract_feed_entries(feed, "rss", self.cutoff, self.now)
        self.assertEqual(entries, [("https://example.org/good", "No Title", None)])


if __name__ == "__main__":
    unittest.main()
