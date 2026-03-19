from __future__ import annotations

from collections.abc import Iterable

import praw

from reddit_ingestor.config import Settings
from reddit_ingestor.models import RedditPost


class RedditClient:
    def __init__(self, settings: Settings) -> None:
        self._client = praw.Reddit(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent,
        )
        self._subreddits = settings.subreddits
        self._post_limit = settings.reddit_post_limit

    def fetch_latest_posts(self) -> list[RedditPost]:
        posts: list[RedditPost] = []

        for subreddit_name in self._subreddits:
            subreddit = self._client.subreddit(subreddit_name)
            submissions = subreddit.new(limit=self._post_limit)
            posts.extend(self._to_posts(submissions))

        return posts

    @staticmethod
    def _to_posts(submissions: Iterable) -> list[RedditPost]:
        return [RedditPost.from_submission(submission) for submission in submissions]
