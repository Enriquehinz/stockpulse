from __future__ import annotations

import logging
from datetime import datetime, timezone

from reddit_ingestor.comment_signal_service import CommentTickerSignalService
from reddit_ingestor.config import load_settings
from reddit_ingestor.db import PostgresStore
from reddit_ingestor.market_data_provider import MarketDataProvider
from reddit_ingestor.outcome_service import SignalOutcomeService
from reddit_ingestor.reddit_client import RedditClient
from reddit_ingestor.snapshot_service import SnapshotPlanner
from reddit_ingestor.ticker_extractor import TickerExtractor
from reddit_ingestor.yfinance_provider import YFinanceMarketDataProvider


LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )


def evaluate_market_outcomes(
    store: PostgresStore,
    outcome_service: SignalOutcomeService,
    market_data_provider: MarketDataProvider,
    market_as_of: datetime,
) -> tuple[int, int, int, int]:
    signals = store.list_post_ticker_signals()
    evaluated_pairs = len(signals)
    prices_fetched = 0
    outcomes_stored = 0
    skipped_pairs = 0

    for signal in signals:
        requests = outcome_service.build_requests(signal)
        existing_points = store.get_market_price_points(requests)
        due_missing_requests = outcome_service.filter_due_missing_requests(
            requests=requests,
            existing_points=existing_points,
            as_of=market_as_of,
        )
        requests_by_symbol: dict[str, list] = {}

        for request in due_missing_requests:
            requests_by_symbol.setdefault(request.ticker_symbol, []).append(request)

        fetched_points = []
        for ticker_symbol, symbol_requests in requests_by_symbol.items():
            fetched_points.extend(
                market_data_provider.fetch_price_points(
                    ticker_symbol=ticker_symbol,
                    requests=symbol_requests,
                    as_of=market_as_of,
                )
            )

        prices_fetched += store.save_market_price_points(fetched_points)
        merged_points = outcome_service.merge_price_points(
            existing_points=existing_points.values(),
            new_points=fetched_points,
        )
        outcome = outcome_service.build_outcome(signal, merged_points)

        if outcome is None:
            skipped_pairs += 1
            continue

        outcomes_stored += store.save_signal_outcome(outcome)

    return evaluated_pairs, prices_fetched, outcomes_stored, skipped_pairs


def main() -> None:
    configure_logging()
    snapshot_at = datetime.now(timezone.utc)
    settings = load_settings()

    reddit_client = RedditClient(settings)
    snapshot_planner = SnapshotPlanner()
    ticker_extractor = TickerExtractor()
    comment_signal_service = CommentTickerSignalService(ticker_extractor)
    outcome_service = SignalOutcomeService()
    market_data_provider = YFinanceMarketDataProvider()
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

    stored_posts = store.list_posts_for_snapshot_check()
    snapshot_plans = snapshot_planner.plan_due_snapshots(
        posts=stored_posts,
        reference_time=snapshot_at,
    )
    due_reddit_post_ids = [plan.reddit_post_id for plan in snapshot_plans]
    post_states = reddit_client.fetch_post_states(due_reddit_post_ids)
    snapshot_result = store.save_post_snapshots(
        plans=snapshot_plans,
        post_states=post_states,
        snapshot_at=snapshot_at,
    )

    LOGGER.info(f"Checked {len(stored_posts)} posts for snapshots.")
    LOGGER.info(
        f"Found {snapshot_result.snapshots_due} due snapshots. "
        f"Stored {snapshot_result.snapshots_stored}. "
        f"Skipped {snapshot_result.snapshots_skipped}."
    )

    stored_post_references = store.list_stored_post_references()
    comments = reddit_client.fetch_top_level_comments(stored_post_references)
    stored_comments_result = store.save_comments(comments)
    comment_mentions = comment_signal_service.extract_mentions(comments)
    stored_comment_mentions_result = store.save_comment_ticker_mentions(
        comment_mentions,
        stored_comments_result.comment_ids_by_reddit_id,
    )

    LOGGER.info(f"Fetched top-level comments for {len(stored_post_references)} posts.")
    LOGGER.info(f"Stored {stored_comments_result.stored_comments} comments.")
    LOGGER.info(
        f"Stored {stored_comment_mentions_result.stored_mentions} comment ticker mentions."
    )

    evaluated_pairs, prices_fetched, outcomes_stored, skipped_pairs = (
        evaluate_market_outcomes(
            store=store,
            outcome_service=outcome_service,
            market_data_provider=market_data_provider,
            market_as_of=snapshot_at,
        )
    )

    LOGGER.info(f"Evaluated {evaluated_pairs} post-ticker pairs for market outcomes.")
    LOGGER.info(f"Fetched {prices_fetched} market price points.")
    LOGGER.info(f"Stored {outcomes_stored} signal outcomes.")
    LOGGER.info(f"Skipped {skipped_pairs} post-ticker pairs.")


if __name__ == "__main__":
    main()
