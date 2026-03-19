from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta

from reddit_ingestor.models import (
    MarketPricePoint,
    MarketPriceRequest,
    PostTickerSignal,
    SignalOutcome,
)


PRICE_TYPE_OFFSETS: tuple[tuple[str, timedelta], ...] = (
    ("entry_0h", timedelta(hours=0)),
    ("entry_1h", timedelta(hours=1)),
    ("entry_3h", timedelta(hours=3)),
    ("entry_6h", timedelta(hours=6)),
    ("price_7d", timedelta(days=7)),
    ("price_30d", timedelta(days=30)),
    ("price_90d", timedelta(days=90)),
)

SPY_SYMBOL = "SPY"


class SignalOutcomeService:
    def build_requests(self, signal: PostTickerSignal) -> tuple[MarketPriceRequest, ...]:
        requests: list[MarketPriceRequest] = []

        for ticker_symbol in (signal.ticker_symbol, SPY_SYMBOL):
            for price_type, offset in PRICE_TYPE_OFFSETS:
                requests.append(
                    MarketPriceRequest(
                        ticker_symbol=ticker_symbol,
                        price_type=price_type,
                        target_at=signal.created_at + offset,
                    )
                )

        return tuple(requests)

    def merge_price_points(
        self,
        existing_points: Iterable[MarketPricePoint],
        new_points: Iterable[MarketPricePoint],
    ) -> dict[tuple[str, str, datetime], MarketPricePoint]:
        merged: dict[tuple[str, str, datetime], MarketPricePoint] = {}

        for point in (*tuple(existing_points), *tuple(new_points)):
            merged[self._point_key(point)] = point

        return merged

    def build_outcome(
        self,
        signal: PostTickerSignal,
        points_by_key: dict[tuple[str, str, datetime], MarketPricePoint],
    ) -> SignalOutcome | None:
        requests = self.build_requests(signal)
        price_values = {
            request.price_type: self._get_price_value(
                points_by_key=points_by_key,
                ticker_symbol=signal.ticker_symbol,
                price_type=request.price_type,
                target_at=request.target_at,
            )
            for request in requests
            if request.ticker_symbol == signal.ticker_symbol
        }
        spy_values = {
            request.price_type: self._get_price_value(
                points_by_key=points_by_key,
                ticker_symbol=SPY_SYMBOL,
                price_type=request.price_type,
                target_at=request.target_at,
            )
            for request in requests
            if request.ticker_symbol == SPY_SYMBOL
        }

        if price_values["entry_1h"] is None or spy_values["entry_1h"] is None:
            return None

        stock_return_7d = self._compute_return(price_values["entry_1h"], price_values["price_7d"])
        stock_return_30d = self._compute_return(
            price_values["entry_1h"],
            price_values["price_30d"],
        )
        stock_return_90d = self._compute_return(
            price_values["entry_1h"],
            price_values["price_90d"],
        )
        spy_return_7d = self._compute_return(
            spy_values["entry_1h"],
            spy_values["price_7d"],
        )
        spy_return_30d = self._compute_return(
            spy_values["entry_1h"],
            spy_values["price_30d"],
        )
        spy_return_90d = self._compute_return(
            spy_values["entry_1h"],
            spy_values["price_90d"],
        )

        return SignalOutcome(
            post_id=signal.post_id,
            ticker_symbol=signal.ticker_symbol,
            entry_price_0h=price_values["entry_0h"],
            entry_price_1h=price_values["entry_1h"],
            entry_price_3h=price_values["entry_3h"],
            entry_price_6h=price_values["entry_6h"],
            price_7d=price_values["price_7d"],
            price_30d=price_values["price_30d"],
            price_90d=price_values["price_90d"],
            spy_entry_0h=spy_values["entry_0h"],
            spy_entry_1h=spy_values["entry_1h"],
            spy_entry_3h=spy_values["entry_3h"],
            spy_entry_6h=spy_values["entry_6h"],
            spy_7d=spy_values["price_7d"],
            spy_30d=spy_values["price_30d"],
            spy_90d=spy_values["price_90d"],
            return_7d=stock_return_7d,
            return_30d=stock_return_30d,
            return_90d=stock_return_90d,
            alpha_7d=self._compute_alpha(stock_return_7d, spy_return_7d),
            alpha_30d=self._compute_alpha(stock_return_30d, spy_return_30d),
            alpha_90d=self._compute_alpha(stock_return_90d, spy_return_90d),
        )

    def filter_due_missing_requests(
        self,
        requests: Iterable[MarketPriceRequest],
        existing_points: dict[tuple[str, str, datetime], MarketPricePoint],
        as_of: datetime,
    ) -> list[MarketPriceRequest]:
        due_requests: list[MarketPriceRequest] = []

        for request in requests:
            if request.target_at > as_of:
                continue
            if self._request_key(request) in existing_points:
                continue
            due_requests.append(request)

        return due_requests

    def _get_price_value(
        self,
        points_by_key: dict[tuple[str, str, datetime], MarketPricePoint],
        ticker_symbol: str,
        price_type: str,
        target_at: datetime,
    ) -> float | None:
        point = points_by_key.get((ticker_symbol, price_type, target_at))
        if point is None:
            return None
        return point.price

    def _compute_return(
        self,
        entry_price: float | None,
        future_price: float | None,
    ) -> float | None:
        if entry_price is None or future_price is None or entry_price == 0:
            return None
        return (future_price - entry_price) / entry_price

    def _compute_alpha(
        self,
        stock_return: float | None,
        spy_return: float | None,
    ) -> float | None:
        if stock_return is None or spy_return is None:
            return None
        return stock_return - spy_return

    def _point_key(self, point: MarketPricePoint) -> tuple[str, str, datetime]:
        return (point.ticker_symbol, point.price_type, point.target_at)

    def _request_key(self, request: MarketPriceRequest) -> tuple[str, str, datetime]:
        return (request.ticker_symbol, request.price_type, request.target_at)
