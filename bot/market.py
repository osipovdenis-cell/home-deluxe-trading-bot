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
    kind: str = "сильный"
    quote_volume_usdt: float = 0.0
    change_24h_percent: float = 0.0


class MarketMonitor:
    def __init__(
        self,
        base_url: str,
        symbols: tuple[str, ...],
        window_seconds: int,
        threshold_percent: float,
        cooldown_seconds: int,
        scan_all_usdt: bool = False,
        min_quote_volume_usdt: float = 0.0,
        early_threshold_percent: float | None = None,
        max_signals_per_cycle: int = 5,
    ) -> None:
        self.symbols = set(symbols)
        self.scan_all_usdt = scan_all_usdt
        self.min_quote_volume_usdt = min_quote_volume_usdt
        self.window_seconds = window_seconds
        self.early_threshold_percent = (
            threshold_percent
            if early_threshold_percent is None
            else early_threshold_percent
        )
        self.threshold_percent = threshold_percent
        self.cooldown_seconds = cooldown_seconds
        self.max_signals_per_cycle = max_signals_per_cycle
        self.history: dict[str, deque[tuple[float, float]]] = defaultdict(deque)
        self.last_alert: dict[str, float] = {}
        self.market_stats: dict[str, tuple[float, float]] = {}
        self.eligible_count = 0
        self.last_symbol_refresh = 0.0
        self.client = httpx.Client(base_url=base_url, timeout=15.0)

    def refresh_symbols(self, now: float | None = None) -> None:
        if not self.scan_all_usdt:
            return
        response = self.client.get("/api/v3/exchangeInfo")
        response.raise_for_status()
        excluded_assets = {
            "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "TRY", "BRL",
        }
        symbols = set()
        for item in response.json().get("symbols", []):
            base_asset = str(item.get("baseAsset", ""))
            if (
                item.get("status") == "TRADING"
                and item.get("quoteAsset") == "USDT"
                and item.get("isSpotTradingAllowed", True)
                and base_asset not in excluded_assets
                and not base_asset.endswith(("UP", "DOWN", "BULL", "BEAR"))
            ):
                symbols.add(str(item["symbol"]))
        if symbols:
            self.symbols = symbols
        self.last_symbol_refresh = time.time() if now is None else now

    def fetch_prices(self) -> dict[str, float]:
        now = time.time()
        if self.scan_all_usdt and (
            not self.symbols or now - self.last_symbol_refresh >= 3600
        ):
            self.refresh_symbols(now)
        response = self.client.get("/api/v3/ticker/24hr")
        response.raise_for_status()
        prices: dict[str, float] = {}
        stats: dict[str, tuple[float, float]] = {}
        for item in response.json():
            symbol = item.get("symbol")
            if symbol not in self.symbols:
                continue
            quote_volume = float(item.get("quoteVolume", 0))
            if quote_volume < self.min_quote_volume_usdt:
                continue
            prices[symbol] = float(item["lastPrice"])
            stats[symbol] = (
                quote_volume,
                float(item.get("priceChangePercent", 0)),
            )
        self.market_stats = stats
        self.eligible_count = len(prices)
        return prices

    def fetch_minute_candles(
        self, symbol: str, started_at: float, finished_at: float
    ) -> list[tuple[float, float, float]]:
        candles: list[tuple[float, float, float]] = []
        cursor = int(started_at * 1000)
        finished_ms = int(finished_at * 1000)
        while cursor < finished_ms:
            response = self.client.get(
                "/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": finished_ms,
                    "limit": 1000,
                },
            )
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break
            candles.extend(
                (float(row[0]) / 1000, float(row[3]), float(row[2])) for row in rows
            )
            next_cursor = int(rows[-1][0]) + 60_000
            if next_cursor <= cursor:
                break
            cursor = next_cursor
        return candles

    def update(self, prices: dict[str, float], now: float | None = None) -> list[PumpSignal]:
        now = time.time() if now is None else now
        candidates: list[PumpSignal] = []
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
            if change < self.early_threshold_percent:
                continue
            last_alert = self.last_alert.get(symbol)
            if last_alert is not None and now - last_alert < self.cooldown_seconds:
                continue
            quote_volume, change_24h = self.market_stats.get(symbol, (0.0, 0.0))
            kind = "сильный" if change >= self.threshold_percent else "ранний"
            candidates.append(
                PumpSignal(
                    symbol,
                    price,
                    change,
                    self.window_seconds,
                    kind,
                    quote_volume,
                    change_24h,
                )
            )
        candidates.sort(key=lambda signal: signal.change_percent, reverse=True)
        signals = candidates[: self.max_signals_per_cycle]
        for signal in signals:
            self.last_alert[signal.symbol] = now
        return signals

    def close(self) -> None:
        self.client.close()
