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
