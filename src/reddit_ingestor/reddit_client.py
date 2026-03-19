from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone

import praw

from reddit_ingestor.config import Settings
from reddit_ingestor.models import RedditComment, RedditPost, RedditPostState, StoredPostReference


class RedditClient:
    def __init__(self, settings: Settings) -> None:
        self._client = praw.Reddit(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent,
        )
        self._subreddits = settings.subreddits
        self._post_limit = settings.reddit_post_limit
        self._comment_limit = settings.reddit_comment_limit

    def fetch_latest_posts(self) -> list[RedditPost]:
        posts: list[RedditPost] = []

        for subreddit_name in self._subreddits:
            subreddit = self._client.subreddit(subreddit_name)
            submissions = subreddit.new(limit=self._post_limit)
            posts.extend(self._to_posts(submissions))

        return posts

    def fetch_post_states(
        self,
        reddit_post_ids: Iterable[str],
    ) -> dict[str, RedditPostState]:
        post_states: dict[str, RedditPostState] = {}

        for reddit_post_id in reddit_post_ids:
            submission = self._client.submission(id=reddit_post_id)
            post_states[reddit_post_id] = RedditPostState(
                reddit_post_id=reddit_post_id,
                upvotes=int(submission.score),
                comments_count=int(submission.num_comments),
            )

        return post_states

    def fetch_top_level_comments(
        self,
        posts: Iterable[StoredPostReference],
    ) -> list[RedditComment]:
        comments: list[RedditComment] = []

        for post in posts:
            submission = self._client.submission(id=post.reddit_post_id)
            submission.comments.replace_more(limit=0)

            for comment in list(submission.comments)[: self._comment_limit]:
                comments.append(
                    RedditComment(
                        reddit_comment_id=comment.id,
                        post_id=post.id,
                        author=comment.author.name if comment.author else None,
                        body=comment.body or "",
                        score=int(comment.score),
                        created_at=datetime.fromtimestamp(
                            comment.created_utc,
                            tz=timezone.utc,
                        ),
                    )
                )

        return comments

    @staticmethod
    def _to_posts(submissions: Iterable) -> list[RedditPost]:
        return [RedditPost.from_submission(submission) for submission in submissions]
