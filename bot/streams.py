import json
import threading
import time
from urllib.parse import quote

from websockets.sync.client import connect


BINANCE_STREAM_BASE_URL = "wss://stream.binance.com:9443"


class AllMarketMiniTickerStream:
    """Keeps the latest eligible Spot prices from Binance's 1s market stream."""

    def __init__(self, symbols: set[str], min_quote_volume_usdt: float) -> None:
        self.min_quote_volume_usdt = min_quote_volume_usdt
        self._symbols = set(symbols)
        self._prices: dict[str, float] = {}
        self._stats: dict[str, tuple[float, float]] = {}
        self._last_message_at = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def seed(
        self,
        prices: dict[str, float],
        stats: dict[str, tuple[float, float]],
    ) -> None:
        with self._lock:
            self._prices.update(prices)
            self._stats.update(stats)

    def set_symbols(self, symbols: set[str]) -> None:
        with self._lock:
            self._symbols = set(symbols)
            self._prices = {
                symbol: price
                for symbol, price in self._prices.items()
                if symbol in self._symbols
            }
            self._stats = {
                symbol: stats
                for symbol, stats in self._stats.items()
                if symbol in self._symbols
            }

    def ingest(self, payload: str | list[dict]) -> None:
        rows = json.loads(payload) if isinstance(payload, str) else payload
        received_at = time.time()
        with self._lock:
            for item in rows:
                symbol = str(item.get("s", ""))
                if symbol not in self._symbols:
                    continue
                quote_volume = float(item.get("q", 0))
                if quote_volume < self.min_quote_volume_usdt:
                    self._prices.pop(symbol, None)
                    self._stats.pop(symbol, None)
                    continue
                last_price = float(item["c"])
                open_price = float(item.get("o", 0))
                change_24h = (
                    (last_price / open_price - 1) * 100 if open_price > 0 else 0.0
                )
                self._prices[symbol] = last_price
                self._stats[symbol] = (quote_volume, change_24h)
            self._last_message_at = received_at

    def snapshot(
        self,
    ) -> tuple[dict[str, float], dict[str, tuple[float, float]]]:
        with self._lock:
            return dict(self._prices), dict(self._stats)

    def healthy(self, now: float | None = None, max_age_seconds: float = 5.0) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            return self._last_message_at > 0 and now - self._last_message_at <= max_age_seconds

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        delay = 1.0
        url = f"{BINANCE_STREAM_BASE_URL}/ws/!miniTicker@arr"
        while not self._stop.is_set():
            try:
                with connect(url, open_timeout=10, close_timeout=2) as websocket:
                    delay = 1.0
                    while not self._stop.is_set():
                        try:
                            self.ingest(websocket.recv(timeout=1))
                        except TimeoutError:
                            continue
            except Exception as error:
                if not self._stop.is_set():
                    print(f"Поток общего рынка переподключается: {error}", flush=True)
                    self._stop.wait(delay)
                    delay = min(delay * 2, 30.0)

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)


class PositionBookTickerStream:
    """Tracks the latest executable sell price (best bid) for open positions."""

    def __init__(self, max_symbols: int = 3) -> None:
        self.max_symbols = max_symbols
        self._symbols: tuple[str, ...] = ()
        self._pending: dict[str, dict[str, tuple[float, float]]] = {}
        self._latest: dict[str, float] = {}
        self._last_message_at = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._changed = threading.Event()
        self._thread: threading.Thread | None = None

    def set_symbols(self, symbols: tuple[str, ...] | list[str]) -> None:
        normalized = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if len(normalized) > self.max_symbols:
            raise ValueError(f"Можно отслеживать не больше {self.max_symbols} позиций")
        with self._lock:
            if normalized == self._symbols:
                return
            self._symbols = normalized
            allowed = set(normalized)
            self._pending = {
                symbol: points for symbol, points in self._pending.items() if symbol in allowed
            }
            self._latest = {
                symbol: price for symbol, price in self._latest.items() if symbol in allowed
            }
            self._last_message_at = 0.0
        self._changed.set()

    def subscription_url(self) -> str | None:
        with self._lock:
            symbols = self._symbols
        if not symbols:
            return None
        streams = "/".join(f"{symbol.lower()}@bookTicker" for symbol in symbols)
        return f"{BINANCE_STREAM_BASE_URL}/stream?streams={quote(streams, safe='/@')}"

    def ingest(self, payload: str | dict) -> None:
        message = json.loads(payload) if isinstance(payload, str) else payload
        item = message.get("data", message)
        symbol = str(item.get("s", ""))
        best_bid = float(item.get("b", 0))
        if not symbol or best_bid <= 0:
            return
        with self._lock:
            if symbol not in self._symbols:
                return
            received_at = time.time()
            points = self._pending.setdefault(symbol, {})
            current_low = points.get("low")
            current_high = points.get("high")
            if current_low is None or best_bid < current_low[0]:
                points["low"] = (best_bid, received_at)
            if current_high is None or best_bid > current_high[0]:
                points["high"] = (best_bid, received_at)
            points["latest"] = (best_bid, received_at)
            self._latest[symbol] = best_bid
            self._last_message_at = received_at

    def drain(self) -> dict[str, float]:
        with self._lock:
            result = {
                symbol: points["latest"][0]
                for symbol, points in self._pending.items()
            }
            self._pending.clear()
            return result

    def drain_events(self) -> list[tuple[float, str, float]]:
        """Returns low/high/latest observations in their actual time order."""
        with self._lock:
            events = {
                (timestamp, symbol, price)
                for symbol, points in self._pending.items()
                for price, timestamp in points.values()
            }
            self._pending.clear()
        return sorted(events)

    def latest(self) -> dict[str, float]:
        with self._lock:
            return dict(self._latest)

    def healthy(self, now: float | None = None, max_age_seconds: float = 5.0) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            if not self._symbols:
                return True
            return self._last_message_at > 0 and now - self._last_message_at <= max_age_seconds

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            url = self.subscription_url()
            if url is None:
                self._changed.wait(1)
                self._changed.clear()
                continue
            try:
                with connect(url, open_timeout=10, close_timeout=2) as websocket:
                    delay = 1.0
                    self._changed.clear()
                    while not self._stop.is_set() and not self._changed.is_set():
                        try:
                            self.ingest(websocket.recv(timeout=1))
                        except TimeoutError:
                            continue
            except Exception as error:
                if not self._stop.is_set():
                    print(f"Поток открытых позиций переподключается: {error}", flush=True)
                    self._stop.wait(delay)
                    delay = min(delay * 2, 30.0)

    def close(self) -> None:
        self._stop.set()
        self._changed.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
