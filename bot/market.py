from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
import json
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
    confirmation_progress_percent: float | None = None
    confirmation_pullback_percent: float | None = None
    confirmation_change_5s_percent: float | None = None
    confirmation_change_10s_percent: float | None = None
    is_rescue: bool = False


@dataclass(frozen=True)
class SignalMarketContext:
    quote_volume_5m_usdt: float
    volume_ratio_5m: float
    trades_5m: int
    taker_buy_ratio_percent: float
    spread_bps: float | None = None
    bid_depth_usdt: float | None = None
    ask_depth_usdt: float | None = None
    order_book_imbalance_percent: float | None = None
    large_trade_threshold_usdt: float | None = None
    large_buy_volume_15s_usdt: float | None = None
    large_sell_volume_15s_usdt: float | None = None
    large_buy_volume_60s_usdt: float | None = None
    large_sell_volume_60s_usdt: float | None = None
    large_trade_imbalance_60s_percent: float | None = None
    large_trade_count_60s: int | None = None
    bid_wall_share_percent: float | None = None
    ask_wall_share_percent: float | None = None
    trend_change_15m_percent: float | None = None
    trend_change_60m_percent: float | None = None
    trend_change_240m_percent: float | None = None
    trend_efficiency_15m_percent: float | None = None
    trend_efficiency_60m_percent: float | None = None
    trend_efficiency_240m_percent: float | None = None
    flow_buy_5s_usdt: float | None = None
    flow_sell_5s_usdt: float | None = None
    flow_buy_15s_usdt: float | None = None
    flow_sell_15s_usdt: float | None = None
    flow_buy_60s_usdt: float | None = None
    flow_sell_60s_usdt: float | None = None
    flow_cvd_60s_percent: float | None = None
    flow_trade_rate_acceleration: float | None = None
    flow_price_change_60s_percent: float | None = None
    flow_price_efficiency_per_10k: float | None = None
    flow_ask_depletion_percent: float | None = None
    flow_bid_support_percent: float | None = None
    flow_spread_bps: float | None = None
    flow_spread_change_bps: float | None = None


@dataclass(frozen=True)
class EntryDynamics:
    change_15s_percent: float
    change_30s_percent: float
    change_60s_percent: float
    change_180s_percent: float
    change_300s_percent: float
    pullback_from_5m_high_percent: float
    btc_change_60s_percent: float | None
    btc_change_300s_percent: float | None
    market_breadth_60s_percent: float

    def as_dict(self) -> dict:
        return {
            "change_15s_percent": round(self.change_15s_percent, 4),
            "change_30s_percent": round(self.change_30s_percent, 4),
            "change_60s_percent": round(self.change_60s_percent, 4),
            "change_180s_percent": round(self.change_180s_percent, 4),
            "change_300s_percent": round(self.change_300s_percent, 4),
            "pullback_from_5m_high_percent": round(
                self.pullback_from_5m_high_percent, 4
            ),
            "btc_change_60s_percent": self.btc_change_60s_percent,
            "btc_change_300s_percent": self.btc_change_300s_percent,
            "market_breadth_60s_percent": round(
                self.market_breadth_60s_percent, 2
            ),
        }


@dataclass
class PendingCandidate:
    started_at: float
    trigger_price: float
    peak_price: float
    samples: list[tuple[float, float]] = field(default_factory=list)
    signal_kind: str | None = None


@dataclass
class LeaderState:
    mode: str
    detected_at: float
    last_qualified_at: float
    peak_price: float
    trough_price: float
    awaiting_pullback: bool = False
    pullback_armed: bool = False
    reentry_ready: bool = False


@dataclass(frozen=True)
class ConfirmationEvent:
    started_at: float
    resolved_at: float
    symbol: str
    trigger_price: float
    resolution_price: float
    accepted: bool
    reason: str
    progress_percent: float = 0.0
    pullback_percent: float = 0.0
    change_5s_percent: float = 0.0
    change_10s_percent: float = 0.0
    signal_kind: str | None = None


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
        entry_confirmation_seconds: int = 20,
        rescue_window_seconds: int = 90,
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
        self.entry_confirmation_seconds = entry_confirmation_seconds
        self.rescue_window_seconds = rescue_window_seconds
        self.history: dict[str, deque[tuple[float, float]]] = defaultdict(deque)
        self.last_alert: dict[str, float] = {}
        self.pending_candidates: dict[str, PendingCandidate] = {}
        self.rescue_candidates: dict[str, PendingCandidate] = {}
        self.leaders: dict[str, LeaderState] = {}
        self.confirmation_rejections: deque[tuple[float, str, str]] = deque()
        self.confirmation_events: deque[ConfirmationEvent] = deque()
        self.market_stats: dict[str, tuple[float, float]] = {}
        self.change_12h_percent: dict[str, float] = {}
        self.last_12h_refresh = 0.0
        self.tick_sizes: dict[str, float] = {}
        self.eligible_count = 0
        self.last_symbol_refresh = 0.0
        self.client = httpx.Client(base_url=base_url, timeout=15.0)

    def order_flow_symbols(self) -> tuple[str, ...]:
        """Prioritize candidates being confirmed, then the freshest leaders."""
        fresh_leaders = sorted(
            self.leaders,
            key=lambda symbol: self.leaders[symbol].last_qualified_at,
            reverse=True,
        )
        return tuple(dict.fromkeys((
            *self.pending_candidates.keys(),
            *self.rescue_candidates.keys(),
            *fresh_leaders,
        )))

    @staticmethod
    def with_order_flow(context: SignalMarketContext, snapshot) -> SignalMarketContext:
        if snapshot is None:
            return context
        return replace(
            context,
            flow_buy_5s_usdt=snapshot.buy_5s_usdt,
            flow_sell_5s_usdt=snapshot.sell_5s_usdt,
            flow_buy_15s_usdt=snapshot.buy_15s_usdt,
            flow_sell_15s_usdt=snapshot.sell_15s_usdt,
            flow_buy_60s_usdt=snapshot.buy_60s_usdt,
            flow_sell_60s_usdt=snapshot.sell_60s_usdt,
            flow_cvd_60s_percent=snapshot.cvd_60s_percent,
            flow_trade_rate_acceleration=snapshot.trade_rate_acceleration,
            flow_price_change_60s_percent=snapshot.price_change_60s_percent,
            flow_price_efficiency_per_10k=snapshot.price_efficiency_per_10k,
            flow_ask_depletion_percent=snapshot.ask_depletion_percent,
            flow_bid_support_percent=snapshot.bid_support_percent,
            flow_spread_bps=snapshot.spread_bps,
            flow_spread_change_bps=snapshot.spread_change_bps,
        )

    def refresh_symbols(self, now: float | None = None) -> None:
        if not self.scan_all_usdt:
            return
        response = self.client.get("/api/v3/exchangeInfo")
        response.raise_for_status()
        excluded_assets = {
            "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "TRY", "BRL",
        }
        symbols = set()
        tick_sizes: dict[str, float] = {}
        for item in response.json().get("symbols", []):
            base_asset = str(item.get("baseAsset", ""))
            if (
                item.get("status") == "TRADING"
                and item.get("quoteAsset") == "USDT"
                and item.get("isSpotTradingAllowed", True)
                and base_asset not in excluded_assets
                and not base_asset.endswith(("UP", "DOWN", "BULL", "BEAR"))
            ):
                symbol = str(item["symbol"])
                symbols.add(symbol)
                price_filter = next(
                    (
                        value for value in item.get("filters", [])
                        if value.get("filterType") == "PRICE_FILTER"
                    ),
                    None,
                )
                if price_filter is not None:
                    tick_size = float(price_filter.get("tickSize", 0))
                    if tick_size > 0:
                        tick_sizes[symbol] = tick_size
        if symbols:
            self.symbols = symbols
            self.tick_sizes = tick_sizes
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

    def refresh_12h_changes(
        self, symbols, now: float | None = None
    ) -> dict[str, float]:
        """Refresh Binance rolling 12-hour returns in API-safe batches."""
        selected = sorted(set(symbols) & self.symbols)
        changes: dict[str, float] = {}

        def fetch_batch(batch: list[str]) -> None:
            response = self.client.get(
                "/api/v3/ticker",
                params={
                    "symbols": json.dumps(
                        batch, separators=(",", ":"), ensure_ascii=False
                    ),
                    "windowSize": "12h",
                    "type": "MINI",
                    "symbolStatus": "TRADING",
                },
            )
            try:
                response.raise_for_status()
            except httpx.HTTPError as error:
                # Binance rejects the complete list when just one recently
                # delisted or unusual symbol is invalid. Isolate that symbol
                # instead of crashing and restarting the whole bot.
                if getattr(getattr(error, "response", None), "status_code", None) != 400:
                    raise
                if len(batch) == 1:
                    return
                middle = len(batch) // 2
                fetch_batch(batch[:middle])
                fetch_batch(batch[middle:])
                return
            payload = response.json()
            rows = [payload] if isinstance(payload, dict) else payload
            for item in rows:
                symbol = str(item.get("symbol", ""))
                open_price = float(item.get("openPrice", 0))
                last_price = float(item.get("lastPrice", 0))
                if symbol in batch and open_price > 0 and last_price > 0:
                    changes[symbol] = (last_price / open_price - 1) * 100

        for offset in range(0, len(selected), 100):
            fetch_batch(selected[offset:offset + 100])
        self.change_12h_percent = changes
        self.last_12h_refresh = time.time() if now is None else now
        return dict(changes)

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

    def fetch_signal_context(self, symbol: str) -> SignalMarketContext:
        response = self.client.get(
            "/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": 241},
        )
        response.raise_for_status()
        rows = response.json()
        if len(rows) < 10:
            raise ValueError("Недостаточно минутных свечей для анализа объёма")
        recent = rows[-5:]
        # Keep the established 20-minute volume baseline unchanged while the
        # extra candles are used only by the new shadow trend features.
        previous = rows[-25:-5]
        recent_quote_volume = sum(float(row[7]) for row in recent)
        previous_quote_volume = sum(float(row[7]) for row in previous)
        comparable_blocks = len(previous) / 5
        previous_average_5m = (
            previous_quote_volume / comparable_blocks if comparable_blocks else 0.0
        )
        volume_ratio = (
            recent_quote_volume / previous_average_5m
            if previous_average_5m > 0
            else 0.0
        )
        trade_count_5m = sum(int(row[8]) for row in recent)
        taker_buy_quote_volume = sum(float(row[10]) for row in recent)
        taker_buy_ratio = (
            taker_buy_quote_volume / recent_quote_volume * 100
            if recent_quote_volume > 0
            else 0.0
        )

        def trend_metrics(minutes: int) -> tuple[float | None, float | None]:
            period = rows[-(minutes + 1):]
            if len(period) < min(minutes + 1, 10):
                return None, None
            closes = [float(row[4]) for row in period]
            if closes[0] <= 0:
                return None, None
            change = (closes[-1] / closes[0] - 1) * 100
            travelled = sum(
                abs(current - previous)
                for previous, current in zip(closes, closes[1:])
            ) / closes[0] * 100
            efficiency = change / travelled * 100 if travelled > 0 else 0.0
            return change, efficiency

        trend_15m = trend_metrics(15)
        trend_60m = trend_metrics(60)
        trend_240m = trend_metrics(240)
        spread_bps = None
        bid_depth = None
        ask_depth = None
        imbalance = None
        bid_wall_share = None
        ask_wall_share = None
        try:
            depth_response = self.client.get(
                "/api/v3/depth", params={"symbol": symbol, "limit": 20}
            )
            depth_response.raise_for_status()
            depth = depth_response.json()
            bids = [(float(price), float(quantity)) for price, quantity in depth["bids"]]
            asks = [(float(price), float(quantity)) for price, quantity in depth["asks"]]
            if bids and asks:
                best_bid = bids[0][0]
                best_ask = asks[0][0]
                spread_bps = (best_ask / best_bid - 1) * 10_000
                bid_depth = sum(price * quantity for price, quantity in bids)
                ask_depth = sum(price * quantity for price, quantity in asks)
                total_depth = bid_depth + ask_depth
                imbalance = (
                    (bid_depth - ask_depth) / total_depth * 100
                    if total_depth > 0
                    else 0.0
                )
                bid_wall_share = (
                    max(price * quantity for price, quantity in bids)
                    / bid_depth * 100 if bid_depth > 0 else 0.0
                )
                ask_wall_share = (
                    max(price * quantity for price, quantity in asks)
                    / ask_depth * 100 if ask_depth > 0 else 0.0
                )
        except Exception:
            # Стакан — дополнительный контекст: его сбой не должен скрывать
            # уже полученные свечи и объём сигнала.
            pass
        large_flow = (None,) * 7
        try:
            trades_response = self.client.get(
                "/api/v3/aggTrades", params={"symbol": symbol, "limit": 1000}
            )
            trades_response.raise_for_status()
            aggregate_trades = trades_response.json()
            if aggregate_trades:
                latest_ms = max(int(item["T"]) for item in aggregate_trades)
                notionals = sorted(
                    float(item["p"]) * float(item["q"])
                    for item in aggregate_trades
                )
                percentile_index = min(
                    len(notionals) - 1, int(len(notionals) * 0.95)
                )
                threshold = max(1000.0, notionals[percentile_index])

                def volumes(seconds: int) -> tuple[float, float, int]:
                    buys = sells = 0.0
                    count = 0
                    cutoff_ms = latest_ms - seconds * 1000
                    for item in aggregate_trades:
                        notional = float(item["p"]) * float(item["q"])
                        if int(item["T"]) < cutoff_ms or notional < threshold:
                            continue
                        count += 1
                        if bool(item.get("m")):
                            sells += notional
                        else:
                            buys += notional
                    return buys, sells, count

                buy_15, sell_15, _count_15 = volumes(15)
                buy_60, sell_60, count_60 = volumes(60)
                total_60 = buy_60 + sell_60
                flow_imbalance = (
                    (buy_60 - sell_60) / total_60 * 100
                    if total_60 > 0 else 0.0
                )
                large_flow = (
                    threshold, buy_15, sell_15, buy_60, sell_60,
                    flow_imbalance, count_60,
                )
        except Exception:
            # Крупный поток — дополнительный сигнал, а не причина остановки.
            pass
        return SignalMarketContext(
            recent_quote_volume,
            volume_ratio,
            trade_count_5m,
            taker_buy_ratio,
            spread_bps,
            bid_depth,
            ask_depth,
            imbalance,
            *large_flow,
            bid_wall_share,
            ask_wall_share,
            trend_15m[0],
            trend_60m[0],
            trend_240m[0],
            trend_15m[1],
            trend_60m[1],
            trend_240m[1],
        )

    def execution_safety(
        self,
        symbol: str,
        price: float,
        context: SignalMarketContext | None,
        max_spread_percent: float = 0.1,
        max_tick_percent: float = 0.1,
    ) -> tuple[bool, str | None, float | None]:
        if context is None or context.spread_bps is None:
            return False, "нет надёжных данных о спреде", None
        spread_percent = context.spread_bps / 100
        if spread_percent > max_spread_percent:
            return (
                False,
                f"спред {spread_percent:.3f}% выше лимита {max_spread_percent:g}%",
                None,
            )
        tick_size = self.tick_sizes.get(symbol)
        if tick_size is None or price <= 0:
            return False, "неизвестен минимальный шаг цены", None
        tick_percent = tick_size / price * 100
        if tick_percent > max_tick_percent:
            return (
                False,
                f"шаг цены {tick_percent:.3f}% выше лимита {max_tick_percent:g}%",
                tick_percent,
            )
        return True, None, tick_percent

    @staticmethod
    def _period_change(
        points: deque[tuple[float, float]], now: float, seconds: int
    ) -> float:
        if not points:
            return 0.0
        target = now - seconds
        base = points[0][1]
        for timestamp, price in points:
            if timestamp >= target:
                base = price
                break
        return (points[-1][1] / base - 1) * 100 if base > 0 else 0.0

    def entry_dynamics(self, symbol: str, now: float) -> EntryDynamics:
        points = self.history.get(symbol, deque())
        changes = {
            seconds: self._period_change(points, now, seconds)
            for seconds in (15, 30, 60, 180, 300)
        }
        current = points[-1][1] if points else 0.0
        high = max((price for _timestamp, price in points), default=current)
        pullback = (current / high - 1) * 100 if high > 0 else 0.0
        btc_points = self.history.get("BTCUSDT")
        btc_60 = self._period_change(btc_points, now, 60) if btc_points else None
        btc_300 = self._period_change(btc_points, now, 300) if btc_points else None
        breadth_values = [
            self._period_change(values, now, 60)
            for values in self.history.values()
            if len(values) >= 2
        ]
        breadth = (
            sum(value > 0 for value in breadth_values) / len(breadth_values) * 100
            if breadth_values else 0.0
        )
        return EntryDynamics(
            changes[15], changes[30], changes[60], changes[180], changes[300],
            pullback, btc_60, btc_300, breadth,
        )

    def entry_quality(
        self, context: SignalMarketContext, dynamics: EntryDynamics
    ) -> tuple[bool, str | None]:
        if context.volume_ratio_5m < 1.2:
            return False, f"объёмный импульс слабый: x{context.volume_ratio_5m:.2f}"
        if context.taker_buy_ratio_percent < 52:
            return False, (
                f"покупатели не доминируют: {context.taker_buy_ratio_percent:.1f}%"
            )
        if context.order_book_imbalance_percent is None:
            return False, "нет надёжного перевеса стакана"
        if context.order_book_imbalance_percent < -20:
            return False, (
                f"стакан против входа: {context.order_book_imbalance_percent:+.1f}%"
            )
        if dynamics.change_30s_percent <= 0 or dynamics.change_60s_percent <= 0.05:
            return False, "импульс уже не подтверждается на 30–60 секундах"
        if dynamics.pullback_from_5m_high_percent < -0.15:
            return False, (
                "цена уже откатила от локального максимума на "
                f"{abs(dynamics.pullback_from_5m_high_percent):.2f}%"
            )
        if (
            dynamics.btc_change_300s_percent is not None
            and dynamics.btc_change_300s_percent < -0.7
            and dynamics.change_60s_percent < 0.3
        ):
            return False, "рынок падает, относительной силы монеты недостаточно"
        return True, None

    def drain_confirmation_rejections(self) -> list[tuple[float, str, str]]:
        rejected = list(self.confirmation_rejections)
        self.confirmation_rejections.clear()
        return rejected

    def drain_confirmation_events(self) -> list[ConfirmationEvent]:
        events = list(self.confirmation_events)
        self.confirmation_events.clear()
        return events

    def active_confirmation_symbols(self) -> set[str]:
        return set(self.pending_candidates) | set(self.rescue_candidates)

    @staticmethod
    def _recent_candidate_change(
        candidate: PendingCandidate, now: float, seconds: int, price: float
    ) -> float:
        cutoff = now - seconds
        base = candidate.samples[0][1]
        for sampled_at, sampled_price in candidate.samples:
            if sampled_at >= cutoff:
                base = sampled_price
                break
        return (price / base - 1) * 100 if base > 0 else 0.0

    def update(self, prices: dict[str, float], now: float | None = None) -> list[PumpSignal]:
        now = time.time() if now is None else now
        candidates: list[PumpSignal] = []
        top_24h = {
            symbol for symbol, (_volume, change_24h) in sorted(
                self.market_stats.items(), key=lambda item: item[1][1], reverse=True
            )[:5]
            if change_24h >= 5
        }
        for symbol, price in prices.items():
            points = self.history[symbol]
            points.append((now, price))
            cutoff = now - self.window_seconds
            while points and points[0][0] < cutoff:
                points.popleft()
            change_12h = self.change_12h_percent.get(symbol)
            if self.change_12h_percent and (
                change_12h is None or change_12h <= 0
            ):
                self.pending_candidates.pop(symbol, None)
                self.rescue_candidates.pop(symbol, None)
                self.leaders.pop(symbol, None)
                continue
            if len(points) < 2 or now - points[0][0] < self.window_seconds * 0.8:
                continue
            minimum = min(value for _, value in points)
            change = (price / minimum - 1) * 100
            _quote_volume, change_24h = self.market_stats.get(symbol, (0.0, 0.0))
            leader_mode = None
            if change >= self.threshold_percent:
                leader_mode = "аномальный лидер"
            elif symbol in top_24h:
                leader_mode = "лидер"
            leader = self.leaders.get(symbol)
            if leader_mode is not None:
                if leader is None:
                    leader = LeaderState(
                        leader_mode, now, now, price, price
                    )
                    self.leaders[symbol] = leader
                else:
                    leader.mode = leader_mode
                    leader.last_qualified_at = now
            elif leader is not None and now - leader.last_qualified_at > 900:
                del self.leaders[symbol]
                leader = None
            if leader is not None:
                previous_peak = leader.peak_price
                if leader.awaiting_pullback:
                    leader.peak_price = max(leader.peak_price, price)
                    pullback = (price / leader.peak_price - 1) * 100
                    if not leader.pullback_armed and pullback <= -0.25:
                        leader.pullback_armed = True
                        leader.trough_price = price
                    if leader.pullback_armed:
                        leader.trough_price = min(leader.trough_price, price)
                        recovery = (price / leader.trough_price - 1) * 100
                        recent_5s = self._period_change(points, now, 5)
                        recent_10s = self._period_change(points, now, 10)
                        if (
                            recovery >= 0.15
                            and recent_5s >= 0.03
                            and recent_10s >= 0.06
                            and price >= previous_peak * 0.997
                        ):
                            leader.reentry_ready = True
                            leader.awaiting_pullback = False
                            leader.pullback_armed = False
                            leader.peak_price = price
                else:
                    leader.peak_price = max(leader.peak_price, price)
            rescue = self.rescue_candidates.get(symbol)
            if rescue is not None:
                prior_peak = rescue.peak_price
                rescue.peak_price = max(rescue.peak_price, price)
                rescue.samples.append((now, price))
                progress = (price / rescue.trigger_price - 1) * 100
                pullback = (price / rescue.peak_price - 1) * 100
                change_5s = self._recent_candidate_change(rescue, now, 5, price)
                change_10s = self._recent_candidate_change(rescue, now, 10, price)
                if now - rescue.started_at >= self.rescue_window_seconds:
                    del self.rescue_candidates[symbol]
                    reason = "повторное ускорение за 90 секунд не появилось"
                    self.confirmation_events.append(ConfirmationEvent(
                        rescue.started_at, now, symbol, rescue.trigger_price,
                        price, False, reason, progress, pullback,
                        change_5s, change_10s, rescue.signal_kind,
                    ))
                    self.last_alert[symbol] = now
                    continue
                recovered = (
                    change >= self.early_threshold_percent
                    and price > prior_peak
                    and change_5s >= 0.05
                    and change_10s >= 0.10
                    and pullback >= -0.03
                )
                if recovered:
                    del self.rescue_candidates[symbol]
                    self.confirmation_events.append(ConfirmationEvent(
                        rescue.started_at, now, symbol, rescue.trigger_price,
                        price, True, "повторное ускорение подтверждено",
                        progress, pullback, change_5s, change_10s,
                        rescue.signal_kind,
                    ))
                    quote_volume, change_24h = self.market_stats.get(
                        symbol, (0.0, 0.0)
                    )
                    kind = rescue.signal_kind or (
                        "сильный" if change >= self.threshold_percent else "ранний"
                    )
                    candidates.append(PumpSignal(
                        symbol, price, change, self.window_seconds, kind,
                        quote_volume, change_24h, progress, pullback,
                        change_5s, change_10s, True,
                    ))
                continue
            if change < self.early_threshold_percent:
                pending = self.pending_candidates.pop(symbol, None)
                if pending is not None:
                    reason = "импульс исчез во время подтверждения"
                    self.confirmation_rejections.append(
                        (now, symbol, reason)
                    )
                    self.confirmation_events.append(
                        ConfirmationEvent(
                            pending.started_at, now, symbol,
                            pending.trigger_price, price, False, reason,
                            (price / pending.trigger_price - 1) * 100,
                            (price / pending.peak_price - 1) * 100,
                            signal_kind=pending.signal_kind,
                        )
                    )
                    self.rescue_candidates[symbol] = PendingCandidate(
                        now, price, price, [(now, price)],
                        pending.signal_kind,
                    )
                continue
            last_alert = self.last_alert.get(symbol)
            leader_reentry = bool(leader and leader.reentry_ready)
            pending = self.pending_candidates.get(symbol)
            if (
                last_alert is not None
                and now - last_alert < self.cooldown_seconds
                and not leader_reentry
                and pending is None
            ):
                continue
            if pending is None:
                self.pending_candidates[symbol] = PendingCandidate(
                    now, price, price, [(now, price)],
                    leader.mode if leader else None,
                )
                if leader_reentry:
                    leader.reentry_ready = False
                continue
            pending.peak_price = max(pending.peak_price, price)
            pending.samples.append((now, price))
            if now - pending.started_at < self.entry_confirmation_seconds:
                continue
            progress = (price / pending.trigger_price - 1) * 100
            pullback = (price / pending.peak_price - 1) * 100
            change_5s = self._recent_candidate_change(pending, now, 5, price)
            change_10s = self._recent_candidate_change(pending, now, 10, price)
            del self.pending_candidates[symbol]
            if progress < 0.05 or pullback < -0.12:
                reason = (
                    f"нет продолжения за {self.entry_confirmation_seconds} сек: "
                    f"движение {progress:+.2f}%, откат {pullback:.2f}%"
                )
                self.confirmation_rejections.append((now, symbol, reason))
                self.confirmation_events.append(
                    ConfirmationEvent(
                        pending.started_at, now, symbol,
                        pending.trigger_price, price, False, reason,
                        progress, pullback, change_5s, change_10s,
                        pending.signal_kind,
                    )
                )
                self.rescue_candidates[symbol] = PendingCandidate(
                    now, price, price, [(now, price)],
                    pending.signal_kind,
                )
                continue
            self.confirmation_events.append(
                ConfirmationEvent(
                    pending.started_at, now, symbol,
                    pending.trigger_price, price, True, "подтверждён",
                    progress, pullback, change_5s, change_10s,
                    pending.signal_kind,
                )
            )
            quote_volume, change_24h = self.market_stats.get(symbol, (0.0, 0.0))
            kind = pending.signal_kind or (
                "сильный" if change >= self.threshold_percent else "ранний"
            )
            candidates.append(
                PumpSignal(
                    symbol,
                    price,
                    change,
                    self.window_seconds,
                    kind,
                    quote_volume,
                    change_24h,
                    progress,
                    pullback,
                    change_5s,
                    change_10s,
                )
            )
        candidates.sort(key=lambda signal: signal.change_percent, reverse=True)
        signals = candidates[: self.max_signals_per_cycle]
        for signal in signals:
            self.last_alert[signal.symbol] = now
            leader = self.leaders.get(signal.symbol)
            if leader is not None:
                leader.awaiting_pullback = True
                leader.pullback_armed = False
                leader.reentry_ready = False
                leader.peak_price = signal.price
                leader.trough_price = signal.price
        return signals

    def close(self) -> None:
        self.client.close()
