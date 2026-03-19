from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

from reddit_ingestor.models import MarketPricePoint, MarketPriceRequest


SHORT_HORIZON_PRICE_TYPES = frozenset({"entry_0h", "entry_1h", "entry_3h", "entry_6h"})
LONG_HORIZON_PRICE_TYPES = frozenset({"price_7d", "price_30d", "price_90d"})


class YFinanceMarketDataProvider:
    source_name = "yfinance"

    def fetch_price_points(
        self,
        ticker_symbol: str,
        requests: Sequence[MarketPriceRequest],
        as_of: datetime,
    ) -> list[MarketPricePoint]:
        if not requests:
            return []

        due_requests = [request for request in requests if request.target_at <= as_of]
        if not due_requests:
            return []

        ticker = yf.Ticker(ticker_symbol)
        points: list[MarketPricePoint] = []

        short_requests = [
            request for request in due_requests if request.price_type in SHORT_HORIZON_PRICE_TYPES
        ]
        long_requests = [
            request for request in due_requests if request.price_type in LONG_HORIZON_PRICE_TYPES
        ]

        if short_requests:
            intraday_history = self._fetch_history(
                ticker=ticker,
                start=min(request.target_at for request in short_requests) - timedelta(days=2),
                end=max(request.target_at for request in short_requests) + timedelta(days=2),
                interval="60m",
            )
            points.extend(
                self._match_requests(
                    ticker_symbol=ticker_symbol,
                    requests=short_requests,
                    history=intraday_history,
                )
            )

        if long_requests:
            daily_history = self._fetch_history(
                ticker=ticker,
                start=min(request.target_at for request in long_requests) - timedelta(days=7),
                end=max(request.target_at for request in long_requests) + timedelta(days=7),
                interval="1d",
            )
            points.extend(
                self._match_requests(
                    ticker_symbol=ticker_symbol,
                    requests=long_requests,
                    history=daily_history,
                )
            )

        return points

    def _fetch_history(
        self,
        ticker: yf.Ticker,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> pd.DataFrame:
        try:
            history = ticker.history(
                start=start,
                end=end + timedelta(days=1),
                interval=interval,
                auto_adjust=False,
                actions=False,
                prepost=False,
            )
        except Exception:
            return pd.DataFrame()

        if history.empty:
            return history

        history = history.copy()
        history = history.dropna(subset=["Close"])
        if history.empty:
            return history

        history.index = self._normalize_index(history.index)
        return history.sort_index()

    def _match_requests(
        self,
        ticker_symbol: str,
        requests: Sequence[MarketPriceRequest],
        history: pd.DataFrame,
    ) -> list[MarketPricePoint]:
        if history.empty:
            return []

        points: list[MarketPricePoint] = []

        for request in requests:
            matching_rows = history.loc[history.index >= request.target_at]
            if matching_rows.empty:
                continue

            first_row = matching_rows.iloc[0]
            actual_market_at = self._to_utc_datetime(matching_rows.index[0])
            points.append(
                MarketPricePoint(
                    ticker_symbol=ticker_symbol,
                    price_type=request.price_type,
                    target_at=request.target_at,
                    actual_market_at=actual_market_at,
                    price=float(first_row["Close"]),
                    source=self.source_name,
                )
            )

        return points

    def _normalize_index(self, index: pd.Index) -> pd.DatetimeIndex:
        normalized = pd.to_datetime(index)
        if normalized.tz is None:
            return normalized.tz_localize(timezone.utc)
        return normalized.tz_convert(timezone.utc)

    def _to_utc_datetime(self, value) -> datetime:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(timezone.utc)
        else:
            timestamp = timestamp.tz_convert(timezone.utc)
        return timestamp.to_pydatetime()
