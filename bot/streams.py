import json
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from urllib.parse import quote

from websockets.sync.client import connect


BINANCE_STREAM_BASE_URL = "wss://stream.binance.com:9443"


@dataclass(frozen=True)
class LeaderOrderFlowSnapshot:
    buy_5s_usdt: float
    sell_5s_usdt: float
    buy_15s_usdt: float
    sell_15s_usdt: float
    buy_60s_usdt: float
    sell_60s_usdt: float
    cvd_60s_percent: float
    trade_rate_acceleration: float | None
    price_change_60s_percent: float
    price_efficiency_per_10k: float | None
    ask_depletion_percent: float | None
    bid_support_percent: float | None
    spread_bps: float | None
    spread_change_bps: float | None


class LeaderOrderFlowStream:
    """Continuously tracks trades and depth changes for active leaders."""

    def __init__(self, max_symbols: int = 20) -> None:
        self.max_symbols = max_symbols
        self._symbols: tuple[str, ...] = ()
        self._trades = defaultdict(deque)
        self._quotes = defaultdict(deque)
        self._depth_changes = defaultdict(deque)
        self._depth = defaultdict(lambda: {"bid": {}, "ask": {}})
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._changed = threading.Event()
        self._thread: threading.Thread | None = None

    def set_symbols(self, symbols) -> None:
        normalized = tuple(dict.fromkeys(str(s).upper() for s in symbols))[:self.max_symbols]
        with self._lock:
            if normalized == self._symbols:
                return
            removed = set(self._symbols) - set(normalized)
            self._symbols = normalized
            for symbol in removed:
                self._trades.pop(symbol, None)
                self._quotes.pop(symbol, None)
                self._depth_changes.pop(symbol, None)
                self._depth.pop(symbol, None)
        self._changed.set()

    def subscription_url(self) -> str | None:
        with self._lock:
            symbols = self._symbols
        if not symbols:
            return None
        streams = []
        for symbol in symbols:
            lower = symbol.lower()
            streams.extend((f"{lower}@aggTrade", f"{lower}@bookTicker", f"{lower}@depth@100ms"))
        joined = "/".join(streams)
        return f"{BINANCE_STREAM_BASE_URL}/stream?streams={quote(joined, safe='/@')}"

    @staticmethod
    def _trim(points: deque, cutoff: float) -> None:
        while points and points[0][0] < cutoff:
            points.popleft()

    def ingest(self, payload: str | dict, received_at: float | None = None) -> None:
        message = json.loads(payload) if isinstance(payload, str) else payload
        item = message.get("data", message)
        symbol = str(item.get("s", "")).upper()
        if not symbol:
            return
        now = time.time() if received_at is None else received_at
        with self._lock:
            if symbol not in self._symbols:
                return
            event = item.get("e")
            if event == "aggTrade":
                price = float(item["p"])
                notional = price * float(item["q"])
                buyer_initiated = not bool(item.get("m"))
                self._trades[symbol].append((now, price, notional, buyer_initiated))
            elif event == "depthUpdate":
                added = {"bid": 0.0, "ask": 0.0}
                removed = {"bid": 0.0, "ask": 0.0}
                for side, key in (("bid", "b"), ("ask", "a")):
                    levels = self._depth[symbol][side]
                    for price_text, quantity_text in item.get(key, []):
                        price = float(price_text)
                        quantity = float(quantity_text)
                        previous = levels.get(price)
                        if previous is not None:
                            delta = (quantity - previous) * price
                            if delta > 0:
                                added[side] += delta
                            elif delta < 0:
                                removed[side] -= delta
                        if quantity > 0:
                            levels[price] = quantity
                        else:
                            levels.pop(price, None)
                self._depth_changes[symbol].append((
                    now, added["bid"], removed["bid"], added["ask"], removed["ask"]
                ))
            elif "b" in item and "a" in item:
                bid = float(item["b"])
                ask = float(item["a"])
                if bid > 0 and ask > 0:
                    self._quotes[symbol].append((now, bid, float(item.get("B", 0)), ask, float(item.get("A", 0))))
            cutoff = now - 120
            self._trim(self._trades[symbol], cutoff)
            self._trim(self._quotes[symbol], cutoff)
            self._trim(self._depth_changes[symbol], cutoff)

    def snapshot(self, symbol: str, now: float | None = None) -> LeaderOrderFlowSnapshot | None:
        now = time.time() if now is None else now
        symbol = symbol.upper()
        with self._lock:
            trades = list(self._trades.get(symbol, ()))
            quotes = list(self._quotes.get(symbol, ()))
            depth = list(self._depth_changes.get(symbol, ()))
        if not trades:
            return None

        def volume(seconds: int, buys: bool) -> float:
            cutoff = now - seconds
            return sum(row[2] for row in trades if row[0] >= cutoff and row[3] == buys)

        buy_5, sell_5 = volume(5, True), volume(5, False)
        buy_15, sell_15 = volume(15, True), volume(15, False)
        buy_60, sell_60 = volume(60, True), volume(60, False)
        total_60 = buy_60 + sell_60
        cvd = (buy_60 - sell_60) / total_60 * 100 if total_60 else 0.0
        count_15 = sum(row[0] >= now - 15 for row in trades)
        count_previous_45 = sum(now - 60 <= row[0] < now - 15 for row in trades)
        prior_rate = count_previous_45 / 45
        acceleration = (count_15 / 15) / prior_rate if prior_rate > 0 else None
        recent = [row for row in trades if row[0] >= now - 60]
        price_change = (
            (recent[-1][1] / recent[0][1] - 1) * 100 if len(recent) >= 2 else 0.0
        )
        net_buy = buy_60 - sell_60
        efficiency = price_change / (abs(net_buy) / 10_000) if abs(net_buy) >= 100 else None

        depth_recent = [row for row in depth if row[0] >= now - 60]
        bid_added = sum(row[1] for row in depth_recent)
        bid_removed = sum(row[2] for row in depth_recent)
        ask_added = sum(row[3] for row in depth_recent)
        ask_removed = sum(row[4] for row in depth_recent)
        bid_total = bid_added + bid_removed
        ask_total = ask_added + ask_removed
        bid_support = bid_added / bid_total * 100 if bid_total else None
        ask_depletion = ask_removed / ask_total * 100 if ask_total else None

        quote_recent = [row for row in quotes if row[0] >= now - 60]
        spread = spread_change = None
        if quote_recent:
            spreads = [(row[3] / row[1] - 1) * 10_000 for row in quote_recent]
            spread = spreads[-1]
            spread_change = spreads[-1] - spreads[0]
        return LeaderOrderFlowSnapshot(
            buy_5, sell_5, buy_15, sell_15, buy_60, sell_60, cvd,
            acceleration, price_change, efficiency, ask_depletion, bid_support,
            spread, spread_change,
        )

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
                    print(f"Поток order flow лидеров переподключается: {error}", flush=True)
                    self._stop.wait(delay)
                    delay = min(delay * 2, 30.0)

    def close(self) -> None:
        self._stop.set()
        self._changed.set()
        if self._thread is not None:
            self._thread.join(timeout=3)


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
        self._pending = deque(maxlen=100000)
        self._overflow = False
        self._latest: dict[str, float] = {}
        self._latest_at: dict[str, float] = {}
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
            self._pending = deque((e for e in self._pending if e[1] in allowed),
                                  maxlen=self._pending.maxlen)
            self._latest = {
                symbol: price for symbol, price in self._latest.items() if symbol in allowed
            }
            self._latest_at = {s:t for s,t in self._latest_at.items() if s in allowed}
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
        try:
            best_bid = float(item.get("b", 0))
        except (ValueError, TypeError):
            return
        if not symbol or not math.isfinite(best_bid) or best_bid <= 0:
            return
        with self._lock:
            if symbol not in self._symbols:
                return
            received_at = time.time()
            if len(self._pending) == self._pending.maxlen:
                self._overflow = True
            self._pending.append((received_at, symbol, best_bid))
            self._latest[symbol] = best_bid
            self._latest_at[symbol] = received_at
            self._last_message_at = received_at

    def drain(self) -> dict[str, float]:
        with self._lock:
            result = {symbol:price for _,symbol,price in self._pending}
            self._pending.clear()
            self._overflow = False
            return result

    def drain_events(self) -> list[tuple[float, str, float]]:
        """Retain every observation: extrema alone lose the first stop crossing."""
        return self.drain_batch()[0]

    def drain_batch(self):
        with self._lock:
            events, overflow = list(self._pending), self._overflow
            self._pending.clear()
            self._overflow = False
        return events, overflow

    def stale_symbols(self, now, max_age_seconds=5.0):
        with self._lock:
            return tuple(s for s in self._symbols
                         if now-self._latest_at.get(s, 0) > max_age_seconds)

    def latest(self) -> dict[str, float]:
        with self._lock:
            return dict(self._latest)

    def healthy(self, now: float | None = None, max_age_seconds: float = 5.0) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            if not self._symbols:
                return True
            return all(now-self._latest_at.get(s, 0) <= max_age_seconds
                       for s in self._symbols)

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
