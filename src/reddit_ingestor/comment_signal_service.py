from __future__ import annotations

from collections.abc import Iterable

from reddit_ingestor.models import CommentTickerMention, RedditComment
from reddit_ingestor.ticker_extractor import TickerExtractor


class CommentTickerSignalService:
    def __init__(self, ticker_extractor: TickerExtractor) -> None:
        self._ticker_extractor = ticker_extractor

    def extract_mentions(
        self,
        comments: Iterable[RedditComment],
    ) -> list[CommentTickerMention]:
        mentions: list[CommentTickerMention] = []

        for comment in comments:
            ticker_counts = self._ticker_extractor.extract_ticker_counts_from_text(
                comment.body,
            )
            for ticker_symbol, mention_count in ticker_counts.items():
                mentions.append(
                    CommentTickerMention(
                        reddit_comment_id=comment.reddit_comment_id,
                        ticker_symbol=ticker_symbol,
                        mention_count=mention_count,
                    )
                )

        return mentions
