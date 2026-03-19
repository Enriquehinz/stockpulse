from __future__ import annotations

from datetime import datetime, timedelta, timezone

from reddit_ingestor.models import PostSnapshotPlan, StoredRedditPost


SNAPSHOT_WINDOWS: tuple[tuple[str, timedelta], ...] = (
    ("1h", timedelta(hours=1)),
    ("3h", timedelta(hours=3)),
    ("24h", timedelta(hours=24)),
)


class SnapshotPlanner:
    def plan_due_snapshots(
        self,
        posts: list[StoredRedditPost],
        reference_time: datetime | None = None,
    ) -> list[PostSnapshotPlan]:
        effective_reference_time = reference_time or datetime.now(timezone.utc)
        plans: list[PostSnapshotPlan] = []

        for post in posts:
            due_snapshot_types = self._get_due_snapshot_types(
                post=post,
                reference_time=effective_reference_time,
            )
            if not due_snapshot_types:
                continue

            plans.append(
                PostSnapshotPlan(
                    post_id=post.id,
                    reddit_post_id=post.reddit_post_id,
                    snapshot_types=due_snapshot_types,
                )
            )

        return plans

    def _get_due_snapshot_types(
        self,
        post: StoredRedditPost,
        reference_time: datetime,
    ) -> tuple[str, ...]:
        existing_snapshot_types = set(post.snapshot_types)
        due_snapshot_types: list[str] = []

        for snapshot_type, window in SNAPSHOT_WINDOWS:
            if snapshot_type in existing_snapshot_types:
                continue
            if reference_time < post.created_at + window:
                continue
            due_snapshot_types.append(snapshot_type)

        return tuple(due_snapshot_types)
