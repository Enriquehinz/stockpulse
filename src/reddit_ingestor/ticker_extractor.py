from __future__ import annotations

import re
from collections.abc import Iterable

from reddit_ingestor.models import PostTickerMatch, RedditPost
from reddit_ingestor.ticker_catalog import FALSE_POSITIVES, load_us_tickers


TICKER_CANDIDATE_PATTERN = re.compile(r"\b[A-Z]{1,5}\b")


class TickerExtractor:
    def __init__(self) -> None:
        self._valid_tickers = load_us_tickers()
        self._excluded_tickers = FALSE_POSITIVES

    def extract_for_post(self, post: RedditPost) -> PostTickerMatch:
        text = f"{post.title}\n{post.selftext}"
        seen: set[str] = set()
        tickers: list[str] = []

        for candidate in TICKER_CANDIDATE_PATTERN.findall(text):
            if candidate in seen:
                continue
            if candidate in self._excluded_tickers:
                continue
            if candidate not in self._valid_tickers:
                continue
            seen.add(candidate)
            tickers.append(candidate)

        return PostTickerMatch(
            reddit_post_id=post.reddit_post_id,
            tickers=tuple(tickers),
        )

    def extract_for_posts(self, posts: Iterable[RedditPost]) -> list[PostTickerMatch]:
        return [self.extract_for_post(post) for post in posts]
