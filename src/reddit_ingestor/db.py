from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import psycopg

from reddit_ingestor.config import Settings
from reddit_ingestor.models import (
    PostTickerMatch,
    RedditPost,
    StoredPostsResult,
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
