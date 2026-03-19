from __future__ import annotations

from functools import lru_cache
from importlib.resources import files


FALSE_POSITIVES = frozenset(
    {
        "A",
        "I",
        "IT",
        "ALL",
        "ON",
        "YOLO",
        "DD",
        "USA",
        "CEO",
        "GDP",
        "ETF",
        "ATH",
        "FOMO",
        "IMO",
        "TLDR",
        "AI",
    }
)


@lru_cache(maxsize=1)
def load_us_tickers() -> frozenset[str]:
    data_path = files("reddit_ingestor").joinpath("data/us_tickers.txt")
    tickers = {
        line.strip().upper()
        for line in data_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    return frozenset(tickers)
