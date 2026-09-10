from collections import defaultdict, deque
from dataclasses import dataclass
import time

import httpx


@dataclass(frozen=True)
class PumpSignal:
    symbol: str
    price: float
    change_percent: float
    window_seconds: int


class MarketMonitor:
    def __init__(
        self,
        base_url: str,
        symbols: tuple[str, ...],
        window_seconds: int,
        threshold_percent: float,
        cooldown_seconds: int,
    ) -> None:
        self.symbols = set(symbols)
        self.window_seconds = window_seconds
        self.threshold_percent = threshold_percent
        self.cooldown_seconds = cooldown_seconds
        self.history: dict[str, deque[tuple[float, float]]] = defaultdict(deque)
        self.last_alert: dict[str, float] = {}
        self.client = httpx.Client(base_url=base_url, timeout=15.0)

    def fetch_prices(self) -> dict[str, float]:
        response = self.client.get("/api/v3/ticker/price")
        response.raise_for_status()
        return {
            item["symbol"]: float(item["price"])
            for item in response.json()
            if item.get("symbol") in self.symbols
        }

    def update(self, prices: dict[str, float], now: float | None = None) -> list[PumpSignal]:
        now = time.time() if now is None else now
        signals: list[PumpSignal] = []
        for symbol, price in prices.items():
            points = self.history[symbol]
            points.append((now, price))
            cutoff = now - self.window_seconds
            while points and points[0][0] < cutoff:
                points.popleft()
            if len(points) < 2 or now - points[0][0] < self.window_seconds * 0.8:
                continue
            minimum = min(value for _, value in points)
            change = (price / minimum - 1) * 100
            if change < self.threshold_percent:
                continue
            last_alert = self.last_alert.get(symbol)
            if last_alert is not None and now - last_alert < self.cooldown_seconds:
                continue
            self.last_alert[symbol] = now
            signals.append(PumpSignal(symbol, price, change, self.window_seconds))
        return signals

    def close(self) -> None:
        self.client.close()
