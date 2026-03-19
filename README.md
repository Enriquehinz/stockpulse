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
