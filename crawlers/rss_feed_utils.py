"""Defensive RSS entry extraction with no Ray or Vectara runtime dependency."""

import logging
from datetime import datetime
from time import mktime

logger = logging.getLogger(__name__)


def _field(entry, name, default=None):
    # feedparser's FeedParserDict supports both mapping and attribute access.
    if hasattr(entry, "get"):
        return entry.get(name, default)
    return getattr(entry, name, default)


def extract_feed_entries(feed, url, cutoff, now):
    """Return eligible (URL, title, published_date) tuples from one RSS feed.

    A malformed feed or individual entry must not abort processing other feeds.
    Missing dates are represented as None to preserve upstream incremental
    prefilter behaviour (which fails open and fetches undated entries).
    """
    if _field(feed, "bozo", False):
        logger.warning("Malformed RSS feed detected: %s", url)

    eligible = []
    for entry in _field(feed, "entries", ()) or ():
        try:
            link = _field(entry, "link")
            if not link:
                logger.warning("Skipping RSS entry without link from %s", url)
                continue

            title = _field(entry, "title") or "No Title"
            published = _field(entry, "published_parsed")
            if published is None:
                eligible.append((link, title, None))
                continue

            try:
                published_at = datetime.fromtimestamp(mktime(published))
            except (TypeError, ValueError, OverflowError):
                logger.warning("Invalid RSS publication date for %s in %s", link, url)
                eligible.append((link, title, None))
                continue

            if cutoff <= published_at <= now:
                eligible.append((link, title, published_at))
        except Exception:
            logger.exception("Unable to process RSS entry from %s", url)

    return eligible
