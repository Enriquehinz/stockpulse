from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime

import psycopg

from reddit_ingestor.config import Settings
from reddit_ingestor.models import (
    CommentSignalStorageResult,
    CommentTickerMention,
    MarketPricePoint,
    PostTickerMatch,
    PostTickerSignal,
    PostSnapshotPlan,
    RedditComment,
    RedditPost,
    RedditPostState,
    SignalOutcome,
    SnapshotStorageResult,
    MarketPriceRequest,
    StoredCommentsResult,
    StoredPostReference,
    StoredPostsResult,
    StoredRedditPost,
    TickerStorageResult,
)


CREATE_REDDIT_POSTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reddit_posts (
    id BIGSERIAL PRIMARY KEY,
    reddit_post_id TEXT NOT NULL UNIQUE,
    subreddit TEXT NOT NULL,
    title TEXT NOT NULL,
    selftext TEXT NOT NULL,
    author TEXT,
    upvotes INTEGER NOT NULL,
    number_of_comments INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CREATE_TICKERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tickers (
    symbol TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CREATE_POST_TICKERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS post_tickers (
    post_id BIGINT NOT NULL REFERENCES reddit_posts(id) ON DELETE CASCADE,
    ticker_symbol TEXT NOT NULL REFERENCES tickers(symbol) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, ticker_symbol)
);
"""

CREATE_POST_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS post_snapshots (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES reddit_posts(id) ON DELETE CASCADE,
    snapshot_type TEXT NOT NULL CHECK (snapshot_type IN ('1h', '3h', '24h')),
    snapshot_at TIMESTAMPTZ NOT NULL,
    upvotes INTEGER NOT NULL,
    comments_count INTEGER NOT NULL,
    UNIQUE (post_id, snapshot_type)
);
"""

CREATE_COMMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS comments (
    id BIGSERIAL PRIMARY KEY,
    reddit_comment_id TEXT NOT NULL UNIQUE,
    post_id BIGINT NOT NULL REFERENCES reddit_posts(id) ON DELETE CASCADE,
    author TEXT,
    body TEXT NOT NULL,
    score INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
"""

CREATE_COMMENT_TICKER_MENTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS comment_ticker_mentions (
    id BIGSERIAL PRIMARY KEY,
    comment_id BIGINT NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
    ticker_symbol TEXT NOT NULL REFERENCES tickers(symbol) ON DELETE CASCADE,
    mention_count INTEGER NOT NULL,
    UNIQUE (comment_id, ticker_symbol)
);
"""

CREATE_MARKET_PRICE_POINTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_price_points (
    id BIGSERIAL PRIMARY KEY,
    ticker_symbol TEXT NOT NULL,
    price_type TEXT NOT NULL,
    target_at TIMESTAMPTZ NOT NULL,
    actual_market_at TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    UNIQUE (ticker_symbol, price_type, target_at)
);
"""

CREATE_SIGNAL_OUTCOMES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS signal_outcomes (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES reddit_posts(id) ON DELETE CASCADE,
    ticker_symbol TEXT NOT NULL REFERENCES tickers(symbol) ON DELETE CASCADE,
    entry_price_0h DOUBLE PRECISION,
    entry_price_1h DOUBLE PRECISION,
    entry_price_3h DOUBLE PRECISION,
    entry_price_6h DOUBLE PRECISION,
    price_7d DOUBLE PRECISION,
    price_30d DOUBLE PRECISION,
    price_90d DOUBLE PRECISION,
    spy_entry_0h DOUBLE PRECISION,
    spy_entry_1h DOUBLE PRECISION,
    spy_entry_3h DOUBLE PRECISION,
    spy_entry_6h DOUBLE PRECISION,
    spy_7d DOUBLE PRECISION,
    spy_30d DOUBLE PRECISION,
    spy_90d DOUBLE PRECISION,
    return_7d DOUBLE PRECISION,
    return_30d DOUBLE PRECISION,
    return_90d DOUBLE PRECISION,
    alpha_7d DOUBLE PRECISION,
    alpha_30d DOUBLE PRECISION,
    alpha_90d DOUBLE PRECISION,
    UNIQUE (post_id, ticker_symbol)
);
"""

INSERT_POST_SQL = """
INSERT INTO reddit_posts (
    reddit_post_id,
    subreddit,
    title,
    selftext,
    author,
    upvotes,
    number_of_comments,
    created_at
)
VALUES (
    %(reddit_post_id)s,
    %(subreddit)s,
    %(title)s,
    %(selftext)s,
    %(author)s,
    %(upvotes)s,
    %(number_of_comments)s,
    %(created_at)s
)
ON CONFLICT (reddit_post_id) DO NOTHING
RETURNING id;
"""

SELECT_POST_ID_SQL = """
SELECT id
FROM reddit_posts
WHERE reddit_post_id = %(reddit_post_id)s;
"""

INSERT_TICKER_SQL = """
INSERT INTO tickers (symbol)
VALUES (%s)
ON CONFLICT (symbol) DO NOTHING
RETURNING symbol;
"""

INSERT_POST_TICKER_SQL = """
INSERT INTO post_tickers (post_id, ticker_symbol)
VALUES (%s, %s)
ON CONFLICT (post_id, ticker_symbol) DO NOTHING
RETURNING post_id;
"""

SELECT_COMMENT_ID_SQL = """
SELECT id
FROM comments
WHERE reddit_comment_id = %(reddit_comment_id)s;
"""

INSERT_COMMENT_SQL = """
INSERT INTO comments (
    reddit_comment_id,
    post_id,
    author,
    body,
    score,
    created_at
)
VALUES (
    %(reddit_comment_id)s,
    %(post_id)s,
    %(author)s,
    %(body)s,
    %(score)s,
    %(created_at)s
)
ON CONFLICT (reddit_comment_id) DO NOTHING
RETURNING id;
"""

INSERT_COMMENT_TICKER_MENTION_SQL = """
INSERT INTO comment_ticker_mentions (
    comment_id,
    ticker_symbol,
    mention_count
)
VALUES (%s, %s, %s)
ON CONFLICT (comment_id, ticker_symbol) DO NOTHING
RETURNING id;
"""

SELECT_POSTS_FOR_SNAPSHOTS_SQL = """
SELECT
    p.id,
    p.reddit_post_id,
    p.created_at,
    ps.snapshot_type
FROM reddit_posts p
LEFT JOIN post_snapshots ps
    ON ps.post_id = p.id
ORDER BY p.id;
"""

SELECT_STORED_POST_REFERENCES_SQL = """
SELECT id, reddit_post_id
FROM reddit_posts
ORDER BY id;
"""

SELECT_POST_TICKER_SIGNALS_SQL = """
SELECT
    pt.post_id,
    pt.ticker_symbol,
    rp.created_at
FROM post_tickers pt
JOIN reddit_posts rp
    ON rp.id = pt.post_id
ORDER BY pt.post_id, pt.ticker_symbol;
"""

INSERT_POST_SNAPSHOT_SQL = """
INSERT INTO post_snapshots (
    post_id,
    snapshot_type,
    snapshot_at,
    upvotes,
    comments_count
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (post_id, snapshot_type) DO NOTHING
RETURNING id;
"""

SELECT_MARKET_PRICE_POINTS_SQL = """
SELECT
    ticker_symbol,
    price_type,
    target_at,
    actual_market_at,
    price,
    source
FROM market_price_points
WHERE ticker_symbol = ANY(%s)
  AND price_type = ANY(%s)
  AND target_at = ANY(%s);
"""

INSERT_MARKET_PRICE_POINT_SQL = """
INSERT INTO market_price_points (
    ticker_symbol,
    price_type,
    target_at,
    actual_market_at,
    price,
    source
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker_symbol, price_type, target_at) DO NOTHING
RETURNING id;
"""

UPSERT_SIGNAL_OUTCOME_SQL = """
INSERT INTO signal_outcomes (
    post_id,
    ticker_symbol,
    entry_price_0h,
    entry_price_1h,
    entry_price_3h,
    entry_price_6h,
    price_7d,
    price_30d,
    price_90d,
    spy_entry_0h,
    spy_entry_1h,
    spy_entry_3h,
    spy_entry_6h,
    spy_7d,
    spy_30d,
    spy_90d,
    return_7d,
    return_30d,
    return_90d,
    alpha_7d,
    alpha_30d,
    alpha_90d
)
VALUES (
    %(post_id)s,
    %(ticker_symbol)s,
    %(entry_price_0h)s,
    %(entry_price_1h)s,
    %(entry_price_3h)s,
    %(entry_price_6h)s,
    %(price_7d)s,
    %(price_30d)s,
    %(price_90d)s,
    %(spy_entry_0h)s,
    %(spy_entry_1h)s,
    %(spy_entry_3h)s,
    %(spy_entry_6h)s,
    %(spy_7d)s,
    %(spy_30d)s,
    %(spy_90d)s,
    %(return_7d)s,
    %(return_30d)s,
    %(return_90d)s,
    %(alpha_7d)s,
    %(alpha_30d)s,
    %(alpha_90d)s
)
ON CONFLICT (post_id, ticker_symbol) DO UPDATE SET
    entry_price_0h = EXCLUDED.entry_price_0h,
    entry_price_1h = EXCLUDED.entry_price_1h,
    entry_price_3h = EXCLUDED.entry_price_3h,
    entry_price_6h = EXCLUDED.entry_price_6h,
    price_7d = EXCLUDED.price_7d,
    price_30d = EXCLUDED.price_30d,
    price_90d = EXCLUDED.price_90d,
    spy_entry_0h = EXCLUDED.spy_entry_0h,
    spy_entry_1h = EXCLUDED.spy_entry_1h,
    spy_entry_3h = EXCLUDED.spy_entry_3h,
    spy_entry_6h = EXCLUDED.spy_entry_6h,
    spy_7d = EXCLUDED.spy_7d,
    spy_30d = EXCLUDED.spy_30d,
    spy_90d = EXCLUDED.spy_90d,
    return_7d = EXCLUDED.return_7d,
    return_30d = EXCLUDED.return_30d,
    return_90d = EXCLUDED.return_90d,
    alpha_7d = EXCLUDED.alpha_7d,
    alpha_30d = EXCLUDED.alpha_30d,
    alpha_90d = EXCLUDED.alpha_90d
RETURNING id;
"""


class PostgresStore:
    def __init__(self, settings: Settings) -> None:
        self._database_url = settings.resolved_database_url

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._database_url) as conn:
            yield conn

    def ensure_schema(self) -> None:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(CREATE_REDDIT_POSTS_TABLE_SQL)
            cur.execute(CREATE_TICKERS_TABLE_SQL)
            cur.execute(CREATE_POST_TICKERS_TABLE_SQL)
            cur.execute(CREATE_POST_SNAPSHOTS_TABLE_SQL)
            cur.execute(CREATE_COMMENTS_TABLE_SQL)
            cur.execute(CREATE_COMMENT_TICKER_MENTIONS_TABLE_SQL)
            cur.execute(CREATE_MARKET_PRICE_POINTS_TABLE_SQL)
            cur.execute(CREATE_SIGNAL_OUTCOMES_TABLE_SQL)
            conn.commit()

    def save_posts(self, posts: Iterable[RedditPost]) -> StoredPostsResult:
        inserted_count = 0
        post_ids_by_reddit_id: dict[str, int] = {}

        with self.connection() as conn, conn.cursor() as cur:
            for post in posts:
                post_id, inserted = self._save_post(cur, post)
                post_ids_by_reddit_id[post.reddit_post_id] = post_id
                inserted_count += int(inserted)
            conn.commit()

        return StoredPostsResult(
            inserted_count=inserted_count,
            post_ids_by_reddit_id=post_ids_by_reddit_id,
        )

    def save_ticker_matches(
        self,
        matches: Iterable[PostTickerMatch],
        post_ids_by_reddit_id: dict[str, int],
    ) -> TickerStorageResult:
        match_list = list(matches)
        detected_relations = sum(len(match.tickers) for match in match_list)
        unique_detected_tickers = len(
            {
                ticker
                for match in match_list
                for ticker in match.tickers
            }
        )

        if not match_list:
            return TickerStorageResult(
                detected_relations=0,
                unique_detected_tickers=0,
                inserted_tickers=0,
                inserted_relations=0,
            )

        inserted_tickers = 0
        inserted_relations = 0
        unique_tickers = sorted(
            {
                ticker
                for match in match_list
                for ticker in match.tickers
            }
        )

        with self.connection() as conn, conn.cursor() as cur:
            for ticker in unique_tickers:
                cur.execute(INSERT_TICKER_SQL, (ticker,))
                inserted_tickers += int(cur.fetchone() is not None)

            for match in match_list:
                post_id = post_ids_by_reddit_id.get(match.reddit_post_id)
                if post_id is None:
                    continue

                for ticker in match.tickers:
                    cur.execute(INSERT_POST_TICKER_SQL, (post_id, ticker))
                    inserted_relations += int(cur.fetchone() is not None)

            conn.commit()

        return TickerStorageResult(
            detected_relations=detected_relations,
            unique_detected_tickers=unique_detected_tickers,
            inserted_tickers=inserted_tickers,
            inserted_relations=inserted_relations,
        )

    def list_stored_post_references(self) -> list[StoredPostReference]:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(SELECT_STORED_POST_REFERENCES_SQL)
            return [
                StoredPostReference(
                    id=int(row[0]),
                    reddit_post_id=str(row[1]),
                )
                for row in cur.fetchall()
            ]

    def list_post_ticker_signals(self) -> list[PostTickerSignal]:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(SELECT_POST_TICKER_SIGNALS_SQL)
            return [
                PostTickerSignal(
                    post_id=int(row[0]),
                    ticker_symbol=str(row[1]),
                    created_at=row[2],
                )
                for row in cur.fetchall()
            ]

    def get_market_price_points(
        self,
        requests: Iterable[MarketPriceRequest],
    ) -> dict[tuple[str, str, datetime], MarketPricePoint]:
        request_list = list(requests)
        if not request_list:
            return {}

        ticker_symbols = sorted({request.ticker_symbol for request in request_list})
        price_types = sorted({request.price_type for request in request_list})
        target_ats = sorted({request.target_at for request in request_list})

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(
                SELECT_MARKET_PRICE_POINTS_SQL,
                (ticker_symbols, price_types, target_ats),
            )
            rows = cur.fetchall()

        return {
            (str(row[0]), str(row[1]), row[2]): MarketPricePoint(
                ticker_symbol=str(row[0]),
                price_type=str(row[1]),
                target_at=row[2],
                actual_market_at=row[3],
                price=float(row[4]),
                source=str(row[5]),
            )
            for row in rows
        }

    def list_posts_for_snapshot_check(self) -> list[StoredRedditPost]:
        posts_by_id: dict[int, StoredRedditPost] = {}

        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(SELECT_POSTS_FOR_SNAPSHOTS_SQL)

            for row in cur.fetchall():
                post_id = int(row[0])
                reddit_post_id = str(row[1])
                created_at = row[2]
                snapshot_type = row[3]

                existing = posts_by_id.get(post_id)
                if existing is None:
                    snapshot_types = ()
                else:
                    snapshot_types = existing.snapshot_types

                if snapshot_type is not None and snapshot_type not in snapshot_types:
                    snapshot_types = (*snapshot_types, str(snapshot_type))

                posts_by_id[post_id] = StoredRedditPost(
                    id=post_id,
                    reddit_post_id=reddit_post_id,
                    created_at=created_at,
                    snapshot_types=snapshot_types,
                )

        return list(posts_by_id.values())

    def save_post_snapshots(
        self,
        plans: Iterable[PostSnapshotPlan],
        post_states: dict[str, RedditPostState],
        snapshot_at: datetime,
    ) -> SnapshotStorageResult:
        plan_list = list(plans)
        snapshots_due = sum(len(plan.snapshot_types) for plan in plan_list)
        snapshots_stored = 0

        if not plan_list:
            return SnapshotStorageResult(
                snapshots_due=0,
                snapshots_stored=0,
                snapshots_skipped=0,
            )

        with self.connection() as conn, conn.cursor() as cur:
            for plan in plan_list:
                post_state = post_states.get(plan.reddit_post_id)
                if post_state is None:
                    continue

                for snapshot_type in plan.snapshot_types:
                    cur.execute(
                        INSERT_POST_SNAPSHOT_SQL,
                        (
                            plan.post_id,
                            snapshot_type,
                            snapshot_at,
                            post_state.upvotes,
                            post_state.comments_count,
                        ),
                    )
                    snapshots_stored += int(cur.fetchone() is not None)

            conn.commit()

        return SnapshotStorageResult(
            snapshots_due=snapshots_due,
            snapshots_stored=snapshots_stored,
            snapshots_skipped=snapshots_due - snapshots_stored,
        )

    def save_comments(
        self,
        comments: Iterable[RedditComment],
    ) -> StoredCommentsResult:
        stored_comments = 0
        comment_ids_by_reddit_id: dict[str, int] = {}

        with self.connection() as conn, conn.cursor() as cur:
            for comment in comments:
                comment_id, inserted = self._save_comment(cur, comment)
                comment_ids_by_reddit_id[comment.reddit_comment_id] = comment_id
                stored_comments += int(inserted)
            conn.commit()

        return StoredCommentsResult(
            stored_comments=stored_comments,
            comment_ids_by_reddit_id=comment_ids_by_reddit_id,
        )

    def save_comment_ticker_mentions(
        self,
        mentions: Iterable[CommentTickerMention],
        comment_ids_by_reddit_id: dict[str, int],
    ) -> CommentSignalStorageResult:
        mention_list = list(mentions)
        stored_mentions = 0
        unique_tickers = sorted({mention.ticker_symbol for mention in mention_list})

        if not mention_list:
            return CommentSignalStorageResult(stored_mentions=0)

        with self.connection() as conn, conn.cursor() as cur:
            for ticker_symbol in unique_tickers:
                cur.execute(INSERT_TICKER_SQL, (ticker_symbol,))

            for mention in mention_list:
                comment_id = comment_ids_by_reddit_id.get(mention.reddit_comment_id)
                if comment_id is None:
                    continue

                cur.execute(
                    INSERT_COMMENT_TICKER_MENTION_SQL,
                    (comment_id, mention.ticker_symbol, mention.mention_count),
                )
                stored_mentions += int(cur.fetchone() is not None)

            conn.commit()

        return CommentSignalStorageResult(stored_mentions=stored_mentions)

    def save_market_price_points(
        self,
        points: Iterable[MarketPricePoint],
    ) -> int:
        point_list = list(points)
        if not point_list:
            return 0

        stored_points = 0

        with self.connection() as conn, conn.cursor() as cur:
            for point in point_list:
                cur.execute(
                    INSERT_MARKET_PRICE_POINT_SQL,
                    (
                        point.ticker_symbol,
                        point.price_type,
                        point.target_at,
                        point.actual_market_at,
                        point.price,
                        point.source,
                    ),
                )
                stored_points += int(cur.fetchone() is not None)

            conn.commit()

        return stored_points

    def save_signal_outcome(self, outcome: SignalOutcome) -> int:
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(UPSERT_SIGNAL_OUTCOME_SQL, self._serialize_signal_outcome(outcome))
            stored = int(cur.fetchone() is not None)
            conn.commit()
            return stored

    def _save_post(self, cur: psycopg.Cursor, post: RedditPost) -> tuple[int, bool]:
        cur.execute(INSERT_POST_SQL, self._serialize_post(post))
        inserted_row = cur.fetchone()
        if inserted_row is not None:
            return int(inserted_row[0]), True

        cur.execute(SELECT_POST_ID_SQL, {"reddit_post_id": post.reddit_post_id})
        existing_row = cur.fetchone()
        if existing_row is None:
            raise ValueError(f"Could not resolve post id for {post.reddit_post_id}")

        return int(existing_row[0]), False

    def _save_comment(
        self,
        cur: psycopg.Cursor,
        comment: RedditComment,
    ) -> tuple[int, bool]:
        cur.execute(INSERT_COMMENT_SQL, self._serialize_comment(comment))
        inserted_row = cur.fetchone()
        if inserted_row is not None:
            return int(inserted_row[0]), True

        cur.execute(
            SELECT_COMMENT_ID_SQL,
            {"reddit_comment_id": comment.reddit_comment_id},
        )
        existing_row = cur.fetchone()
        if existing_row is None:
            raise ValueError(
                f"Could not resolve comment id for {comment.reddit_comment_id}",
            )

        return int(existing_row[0]), False

    @staticmethod
    def _serialize_post(post: RedditPost) -> dict[str, object]:
        return {
            "reddit_post_id": post.reddit_post_id,
            "subreddit": post.subreddit,
            "title": post.title,
            "selftext": post.selftext,
            "author": post.author,
            "upvotes": post.upvotes,
            "number_of_comments": post.number_of_comments,
            "created_at": post.created_at,
        }

    @staticmethod
    def _serialize_comment(comment: RedditComment) -> dict[str, object]:
        return {
            "reddit_comment_id": comment.reddit_comment_id,
            "post_id": comment.post_id,
            "author": comment.author,
            "body": comment.body,
            "score": comment.score,
            "created_at": comment.created_at,
        }

    @staticmethod
    def _serialize_signal_outcome(outcome: SignalOutcome) -> dict[str, object]:
        return {
            "post_id": outcome.post_id,
            "ticker_symbol": outcome.ticker_symbol,
            "entry_price_0h": outcome.entry_price_0h,
            "entry_price_1h": outcome.entry_price_1h,
            "entry_price_3h": outcome.entry_price_3h,
            "entry_price_6h": outcome.entry_price_6h,
            "price_7d": outcome.price_7d,
            "price_30d": outcome.price_30d,
            "price_90d": outcome.price_90d,
            "spy_entry_0h": outcome.spy_entry_0h,
            "spy_entry_1h": outcome.spy_entry_1h,
            "spy_entry_3h": outcome.spy_entry_3h,
            "spy_entry_6h": outcome.spy_entry_6h,
            "spy_7d": outcome.spy_7d,
            "spy_30d": outcome.spy_30d,
            "spy_90d": outcome.spy_90d,
            "return_7d": outcome.return_7d,
            "return_30d": outcome.return_30d,
            "return_90d": outcome.return_90d,
            "alpha_7d": outcome.alpha_7d,
            "alpha_30d": outcome.alpha_30d,
            "alpha_90d": outcome.alpha_90d,
        }
