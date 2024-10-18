import logging
import time
from core.crawler import Crawler
import feedparser
from datetime import datetime, timedelta
from time import mktime

class RssCrawler(Crawler):

    def crawl(self) -> None:
        """
        Crawl RSS feeds and upload to Vectara.
        """
        rss_pages = self.cfg.rss_crawler.rss_pages
        source = self.cfg.rss_crawler.source
        if isinstance(rss_pages, str):
            rss_pages = [rss_pages]
        delay_in_secs = self.cfg.rss_crawler.delay
        days_past = self.cfg.rss_crawler.days_past

        today = datetime.now().replace(microsecond=0)
        cutoff_date = today - timedelta(days=days_past)

        logging.info(f"Starting RSS crawl for the last {days_past} days from source: {source}")

        # Collect all URLs from the RSS feeds
        urls = []
        for rss_page in rss_pages:
            logging.debug(f"Parsing RSS feed: {rss_page}")
            try:
                feed = feedparser.parse(rss_page)
                if feed.bozo:
                    logging.warning(f"Malformed RSS feed detected: {rss_page}")
                for entry in feed.entries:
                    try:
                        entry_link = getattr(entry, 'link', None)
                        entry_title = getattr(entry, 'title', "No Title")
                        entry_published_parsed = getattr(entry, 'published_parsed', None)

                        if not entry_link:
                            logging.warning(f"Entry missing 'link' attribute in feed {rss_page}. Skipping entry: {entry}")
                            continue

                        if entry_published_parsed:
                            entry_date = datetime.fromtimestamp(mktime(entry_published_parsed))
                        else:
                            logging.warning(f"Entry missing 'published' date in feed {rss_page}. Using current time as publish date.")
                            entry_date = datetime.now()

                        if cutoff_date <= entry_date <= today:
                            urls.append([entry_link, entry_title, entry_date])
                            logging.debug(f"Added URL: {entry_link}, Title: {entry_title}, Date: {entry_date}")
                        else:
                            logging.debug(f"Skipping URL: {entry_link}, Date: {entry_date} outside the cutoff range.")

                    except Exception as e:
                        logging.error(f"Error processing entry in feed {rss_page}: {e}")
            except Exception as e:
                logging.error(f"Error parsing RSS feed {rss_page}: {e}")

        logging.info(f"Found {len(urls)} URLs to index from the last {days_past} days ({source})")

        crawled_urls = set()  # To avoid duplications
        for url, title, pub_date in urls:
            if url in crawled_urls:
                logging.info(f"Skipping duplicate URL: {url}")
                continue

            # Index document into Vectara
            try:
                if pub_date:
                    pub_date_int = int(pub_date.timestamp())
                else:
                    pub_date_int = 0  # Unknown published date
                    pub_date = 'unknown'

                crawl_date_int = int(today.timestamp())

                metadata = {
                    'source': source,
                    'url': url,
                    'title': title,
                    'pub_date': str(pub_date),
                    'pub_date_int': pub_date_int,
                    'crawl_date': str(today),
                    'crawl_date_int': crawl_date_int
                }

                logging.debug(f"Indexing URL: {url} with metadata: {metadata}")
                succeeded = self.indexer.index_url(url, metadata=metadata)
                if succeeded:
                    logging.info(f"Successfully indexed {url}")
                    crawled_urls.add(url)
                else:
                    logging.warning(f"Indexing failed for {url}")

            except Exception as e:
                logging.error(f"Error while indexing {url}: {e}")

            time.sleep(delay_in_secs)

        logging.info("RSS crawl completed successfully.")
        return

