from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


SUBREDDITS = ("wallstreetbets", "stocks", "investing")


@dataclass(frozen=True)
class Settings:
    reddit_client_id: str
    reddit_client_secret: str
    reddit_user_agent: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    database_url: str | None
    subreddits: tuple[str, ...] = SUBREDDITS
    reddit_post_limit: int = 25

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url

        return (
            "postgresql://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def load_settings() -> Settings:
    load_dotenv()

    return Settings(
        reddit_client_id=_get_required_env("REDDIT_CLIENT_ID"),
        reddit_client_secret=_get_required_env("REDDIT_CLIENT_SECRET"),
        reddit_user_agent=_get_required_env("REDDIT_USER_AGENT"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "stockpulse"),
        postgres_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database_url=os.getenv("DATABASE_URL"),
        reddit_post_limit=int(os.getenv("REDDIT_POST_LIMIT", "25")),
    )


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
