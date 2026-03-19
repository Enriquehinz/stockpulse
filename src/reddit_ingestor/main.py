from __future__ import annotations

import logging

from reddit_ingestor.config import load_settings
from reddit_ingestor.db import PostgresStore
from reddit_ingestor.reddit_client import RedditClient
from reddit_ingestor.ticker_extractor import TickerExtractor


LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )


def main() -> None:
    configure_logging()
    settings = load_settings()

    reddit_client = RedditClient(settings)
    ticker_extractor = TickerExtractor()
    store = PostgresStore(settings)

    store.ensure_schema()
    posts = reddit_client.fetch_latest_posts()
    post_result = store.save_posts(posts)
    ticker_matches = ticker_extractor.extract_for_posts(posts)
    ticker_result = store.save_ticker_matches(
        ticker_matches,
        post_result.post_ids_by_reddit_id,
    )
    posts_with_tickers = sum(1 for match in ticker_matches if match.tickers)

    LOGGER.info(
        f"Fetched {len(posts)} posts from {len(settings.subreddits)} subreddits. "
        f"Inserted {post_result.inserted_count} new rows into PostgreSQL."
    )
    LOGGER.info(
        f"Detected {ticker_result.detected_relations} ticker matches across "
        f"{posts_with_tickers} posts using {ticker_result.unique_detected_tickers} "
        f"unique tickers."
    )
    LOGGER.info(
        f"Stored {ticker_result.inserted_tickers} new tickers and "
        f"{ticker_result.inserted_relations} new post-ticker relations."
    )


if __name__ == "__main__":
    main()
