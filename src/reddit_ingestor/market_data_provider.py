from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from reddit_ingestor.models import MarketPricePoint, MarketPriceRequest


class MarketDataProvider(Protocol):
    source_name: str

    def fetch_price_points(
        self,
        ticker_symbol: str,
        requests: Sequence[MarketPriceRequest],
        as_of: datetime,
    ) -> list[MarketPricePoint]:
        ...
