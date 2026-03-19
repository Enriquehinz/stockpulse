# StockPulse Reddit Ingestor

Small Python project that:

- connects to Reddit using PRAW
- fetches the latest posts from `wallstreetbets`, `stocks`, and `investing`
- extracts post data
- extracts valid stock tickers from post `title` + `selftext`
- stores the results in PostgreSQL

## Requirements

- Python 3.11+
- PostgreSQL

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -e .
```

3. Copy `.env.example` to `.env` and fill in your credentials.

## Environment Variables

```env
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=stockpulse-reddit-ingestor/0.1.0
REDDIT_COMMENT_LIMIT=20

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=stockpulse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

REDDIT_POST_LIMIT=25
```

You can also use `DATABASE_URL` instead of the individual PostgreSQL variables.

## Run

```bash
python -m reddit_ingestor
```

On startup, the app creates the `reddit_posts` table if it does not already exist.

The ticker universe is loaded from the local seed file at `src/reddit_ingestor/data/us_tickers.txt`.

## Stored Fields

- `title`
- `selftext`
- `author`
- `upvotes`
- `number_of_comments`
- `created_at`

The table also stores `reddit_post_id`, `subreddit`, and `fetched_at` to support deduplication and traceability.

## Ticker Storage

Ticker extraction only analyzes `title` and `selftext`.

The app stores ticker data in:

- `tickers`
- `post_tickers`

Duplicate ticker symbols and duplicate post-ticker relations are ignored at the database level.

## Post Snapshots

The app also stores post engagement snapshots in:

- `post_snapshots`

Supported snapshot types are `1h`, `3h`, and `24h`.

Each run checks stored posts, determines which snapshots are due from `created_at`, fetches the latest post state from Reddit, and stores missing snapshots without creating duplicates for the same `post_id` and `snapshot_type`.

## Comments

Each run also fetches top-level comments for stored posts, limited by `REDDIT_COMMENT_LIMIT` per post.

The app stores comment data in:

- `comments`
- `comment_ticker_mentions`

Comment ticker detection reuses the same local ticker catalog and cleaning rules as post ticker extraction. Each comment stores unique ticker symbols plus `mention_count`, and duplicate comments and duplicate comment-ticker rows are ignored at the database level.

## Market Data

The app also evaluates stored `post_tickers` against market prices using `yfinance`.

It stores raw market checkpoints in:

- `market_price_points`

And derived post-ticker outcomes in:

- `signal_outcomes`

Tracked checkpoints are:

- `entry_0h`
- `entry_1h`
- `entry_3h`
- `entry_6h`
- `price_7d`
- `price_30d`
- `price_90d`

The same checkpoints are also fetched for `SPY`. The app uses the nearest available market price at or after each target timestamp, skips unavailable or invalid tickers safely, and upserts outcomes so later runs can fill in longer horizons as they become available.
