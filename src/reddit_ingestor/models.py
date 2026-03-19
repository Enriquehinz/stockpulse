from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class RedditPost:
    reddit_post_id: str
    subreddit: str
    title: str
    selftext: str
    author: str | None
    upvotes: int
    number_of_comments: int
    created_at: datetime

    @classmethod
    def from_submission(cls, submission) -> "RedditPost":
        author_name = submission.author.name if submission.author else None

        return cls(
            reddit_post_id=submission.id,
            subreddit=submission.subreddit.display_name,
            title=submission.title,
            selftext=submission.selftext or "",
            author=author_name,
            upvotes=int(submission.score),
            number_of_comments=int(submission.num_comments),
            created_at=datetime.fromtimestamp(
                submission.created_utc,
                tz=timezone.utc,
            ),
        )


@dataclass(frozen=True)
class PostTickerMatch:
    reddit_post_id: str
    tickers: tuple[str, ...]


@dataclass(frozen=True)
class StoredPostsResult:
    inserted_count: int
    post_ids_by_reddit_id: dict[str, int]


@dataclass(frozen=True)
class TickerStorageResult:
    detected_relations: int
    unique_detected_tickers: int
    inserted_tickers: int
    inserted_relations: int


@dataclass(frozen=True)
class StoredRedditPost:
    id: int
    reddit_post_id: str
    created_at: datetime
    snapshot_types: tuple[str, ...]


@dataclass(frozen=True)
class StoredPostReference:
    id: int
    reddit_post_id: str


@dataclass(frozen=True)
class PostSnapshotPlan:
    post_id: int
    reddit_post_id: str
    snapshot_types: tuple[str, ...]


@dataclass(frozen=True)
class RedditPostState:
    reddit_post_id: str
    upvotes: int
    comments_count: int


@dataclass(frozen=True)
class SnapshotStorageResult:
    snapshots_due: int
    snapshots_stored: int
    snapshots_skipped: int


@dataclass(frozen=True)
class RedditComment:
    reddit_comment_id: str
    post_id: int
    author: str | None
    body: str
    score: int
    created_at: datetime


@dataclass(frozen=True)
class CommentTickerMention:
    reddit_comment_id: str
    ticker_symbol: str
    mention_count: int


@dataclass(frozen=True)
class StoredCommentsResult:
    stored_comments: int
    comment_ids_by_reddit_id: dict[str, int]


@dataclass(frozen=True)
class CommentSignalStorageResult:
    stored_mentions: int


@dataclass(frozen=True)
class PostTickerSignal:
    post_id: int
    ticker_symbol: str
    created_at: datetime


@dataclass(frozen=True)
class MarketPriceRequest:
    ticker_symbol: str
    price_type: str
    target_at: datetime


@dataclass(frozen=True)
class MarketPricePoint:
    ticker_symbol: str
    price_type: str
    target_at: datetime
    actual_market_at: datetime
    price: float
    source: str


@dataclass(frozen=True)
class SignalOutcome:
    post_id: int
    ticker_symbol: str
    entry_price_0h: float | None
    entry_price_1h: float | None
    entry_price_3h: float | None
    entry_price_6h: float | None
    price_7d: float | None
    price_30d: float | None
    price_90d: float | None
    spy_entry_0h: float | None
    spy_entry_1h: float | None
    spy_entry_3h: float | None
    spy_entry_6h: float | None
    spy_7d: float | None
    spy_30d: float | None
    spy_90d: float | None
    return_7d: float | None
    return_30d: float | None
    return_90d: float | None
    alpha_7d: float | None
    alpha_30d: float | None
    alpha_90d: float | None


@dataclass(frozen=True)
class MarketEvaluationResult:
    evaluated_pairs: int
    prices_fetched: int
    outcomes_stored: int
    skipped_pairs: int
