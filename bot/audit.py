from collections import deque
from dataclasses import dataclass
import math
from pathlib import Path
import sqlite3
from statistics import median
import time

from bot.probability import FEATURE_NAMES, train_probability_model
from bot.rocket_comparison import RocketComparison
from bot.scalp_shadow import ScalpShadow


@dataclass(frozen=True)
class AuditSummary:
    started_at: float
    finished_at: float
    expected_pumps: int
    delivered_alerts: int
    missed_pumps: int
    failed_alerts: int
    errors: int
    coverage_percent: float
    threshold_percent: float

    def telegram_text(self) -> str:
        hours = (self.finished_at - self.started_at) / 3600
        return (
            "📊 Суточный отчёт мониторинга\n"
            f"Период: {hours:.1f} ч.\n"
            f"Скачков от {self.threshold_percent:g}%: {self.expected_pumps}.\n"
            f"Сигналов обработано: {self.delivered_alerts}.\n"
            f"Предположительно пропущено: {self.missed_pumps}.\n"
            f"Ошибок отправки: {self.failed_alerts}.\n"
            f"Ошибок получения данных: {self.errors}.\n"
            f"Полнота наблюдений: {self.coverage_percent:.1f}%."
        )


@dataclass(frozen=True)
class SignalPerformance:
    signal_count: int
    evaluated: dict[int, int]
    positive_rate: dict[int, float]
    average_net_return: dict[int, float]

    def as_dict(self) -> dict:
        return {
            "signal_count": self.signal_count,
            "horizons": {
                str(minutes): {
                    "evaluated": self.evaluated.get(minutes, 0),
                    "positive_rate_percent": round(
                        self.positive_rate.get(minutes, 0.0), 2
                    ),
                    "average_net_return_percent": round(
                        self.average_net_return.get(minutes, 0.0), 4
                    ),
                }
                for minutes in (15, 30, 60)
            },
        }

    def telegram_text(self) -> str:
        lines = [
            "📈 Проверка качества сигналов",
            f"Всего сигналов: {self.signal_count}.",
        ]
        for minutes in (15, 30, 60):
            count = self.evaluated.get(minutes, 0)
            if not count:
                lines.append(f"Через {minutes} мин: данные накапливаются.")
                continue
            lines.append(
                f"Через {minutes} мин: проверено {count}, "
                f"в плюсе {self.positive_rate[minutes]:.1f}%, "
                f"средний результат {self.average_net_return[minutes]:+.2f}%."
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class HistoricalImpulse:
    timestamp: float
    setup_change_percent: float
    maximum_after_entry_percent: float
    minimum_after_entry_percent: float
    first_target_hit: bool
    second_target_hit: bool
    stopped_before_first_target: bool
    ai_score: int | None = None
    ai_decision: str | None = None
    ai_verdict: str | None = None
    ai_reason: str | None = None
    ai_risk: str | None = None
    volume_ratio_5m: float | None = None
    taker_buy_ratio_percent: float | None = None
    order_book_imbalance_percent: float | None = None
    change_60s_percent: float | None = None
    pullback_from_high_percent: float | None = None


@dataclass(frozen=True)
class SymbolBehavior:
    symbol: str
    impulses: tuple[HistoricalImpulse, ...]

    @property
    def first_target_hits(self) -> int:
        return sum(item.first_target_hit for item in self.impulses)

    @property
    def second_target_hits(self) -> int:
        return sum(item.second_target_hit for item in self.impulses)

    @property
    def favorable(self) -> bool:
        if len(self.impulses) < 3:
            return False
        required = math.ceil(len(self.impulses) * 2 / 3)
        repeated_failure = (
            len(self.impulses) >= 2
            and all(
                item.stopped_before_first_target for item in self.impulses[-2:]
            )
        )
        return self.first_target_hits >= required and not repeated_failure

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "completed_similar_impulses": len(self.impulses),
            "first_target_hits": self.first_target_hits,
            "second_target_hits": self.second_target_hits,
            "favorable_for_entry": self.favorable,
            "impulses": [
                {
                    "setup_change_percent": round(item.setup_change_percent, 3),
                    "maximum_next_15m_percent": round(
                        item.maximum_after_entry_percent, 3
                    ),
                    "minimum_next_15m_percent": round(
                        item.minimum_after_entry_percent, 3
                    ),
                    "reached_0_7_percent": item.first_target_hit,
                    "reached_1_percent": item.second_target_hit,
                    "stopped_before_first_target": item.stopped_before_first_target,
                    "ai_score_at_signal": item.ai_score,
                    "ai_decision_at_signal": item.ai_decision,
                    "ai_verdict_at_signal": item.ai_verdict,
                    "ai_reason_at_signal": item.ai_reason,
                    "ai_risk_at_signal": item.ai_risk,
                    "volume_ratio_5m": item.volume_ratio_5m,
                    "taker_buy_ratio_percent": item.taker_buy_ratio_percent,
                    "order_book_imbalance_percent": item.order_book_imbalance_percent,
                    "change_60s_percent": item.change_60s_percent,
                    "pullback_from_high_percent": item.pullback_from_high_percent,
                }
                for item in self.impulses
            ],
        }


@dataclass(frozen=True)
class LearningProfile:
    symbol: str
    symbol_examples: int
    symbol_successes: int
    similar_examples: int
    similar_successes: int
    consecutive_failures: int
    consecutive_trade_losses: int
    status: str
    score_adjustment: int
    explanation: str

    @property
    def symbol_success_rate_percent(self) -> float | None:
        if not self.symbol_examples:
            return None
        return self.symbol_successes / self.symbol_examples * 100

    @property
    def similar_success_rate_percent(self) -> float | None:
        if not self.similar_examples:
            return None
        return self.similar_successes / self.similar_examples * 100

    @property
    def blocked(self) -> bool:
        return False

    def required_ai_score(self, base_score: int) -> int:
        return max(65, min(90, base_score + self.score_adjustment))

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "status": self.status,
            "symbol_examples": self.symbol_examples,
            "symbol_successes": self.symbol_successes,
            "symbol_success_rate_percent": self.symbol_success_rate_percent,
            "similar_market_examples": self.similar_examples,
            "similar_market_successes": self.similar_successes,
            "similar_market_success_rate_percent": self.similar_success_rate_percent,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_trade_losses": self.consecutive_trade_losses,
            "ai_score_adjustment": self.score_adjustment,
            "explanation": self.explanation,
        }


@dataclass(frozen=True)
class LearningReport:
    examples: int
    successes: int
    learned_symbols: int
    best_symbols: tuple[tuple[str, int, float], ...]
    blocked_symbols: tuple[tuple[str, int, float], ...]

    def telegram_text(self) -> str:
        rate = self.successes / self.examples * 100 if self.examples else 0.0
        lines = [
            "🧠 Чему научился бот",
            f"Размеченных импульсов: {self.examples}.",
            f"Цель +0,7% раньше стопа: {rate:.1f}%.",
            f"Монет с накопленной историей: {self.learned_symbols}.",
        ]
        if self.best_symbols:
            lines.append(
                "Лучшее поведение: "
                + ", ".join(
                    f"{symbol} — {rate:.0f}% ({count})"
                    for symbol, count, rate in self.best_symbols
                )
                + "."
            )
        if self.blocked_symbols:
            lines.append(
                "Повторные входы ограничены: "
                + ", ".join(
                    f"{symbol} — {rate:.0f}% ({count})"
                    for symbol, count, rate in self.blocked_symbols
                )
                + "."
            )
        if not self.examples:
            lines.append("Обучающие примеры пока накапливаются.")
        return "\n".join(lines)


@dataclass(frozen=True)
class ConfirmationAudit:
    evaluated: int
    accepted: int
    rejected: int
    immediate_winners: int
    accepted_delayed_winners: int
    prevented_stops: int
    missed_winners: int
    rejected_neutral: int

    def telegram_text(self) -> str:
        if not self.evaluated:
            return "⏱ Проверка 20 секунд: результаты пока накапливаются."
        immediate_rate = self.immediate_winners / self.evaluated * 100
        delayed_rate = (
            self.accepted_delayed_winners / self.accepted * 100
            if self.accepted else 0.0
        )
        return (
            "⏱ Аудит ожидания 20 секунд\n"
            f"Проверено кандидатов: {self.evaluated}.\n"
            f"Прошли/отклонены: {self.accepted}/{self.rejected}.\n"
            f"Вход сразу достиг бы +0,7% раньше стопа: "
            f"{self.immediate_winners} ({immediate_rate:.1f}%).\n"
            f"После подтверждения цель достигли: "
            f"{self.accepted_delayed_winners}/{self.accepted} "
            f"({delayed_rate:.1f}%).\n"
            f"Предотвращено стопов: {self.prevented_stops}.\n"
            f"Пропущено потенциальных +0,7%: {self.missed_winners}.\n"
            f"Нейтральных отклонений: {self.rejected_neutral}."
        )


def detect_pumps(
    candles: list[tuple[float, float, float]],
    window_seconds: int,
    threshold_percent: float,
    cooldown_seconds: int,
) -> list[float]:
    """Return candle timestamps where Binance minute data shows a pump."""
    events: list[float] = []
    last_event: float | None = None
    for index, (timestamp, _low, high) in enumerate(candles):
        cutoff = timestamp - window_seconds
        previous_lows = [
            low
            for previous_timestamp, low, _previous_high in candles[:index]
            if previous_timestamp >= cutoff
        ]
        if not previous_lows:
            continue
        change = (high / min(previous_lows) - 1) * 100
        if change < threshold_percent:
            continue
        if last_event is not None and timestamp - last_event < cooldown_seconds:
            continue
        events.append(timestamp)
        last_event = timestamp
    return events


class AuditLog:
    def __init__(self, path: str) -> None:
        database = Path(path)
        database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.rocket_comparison = RocketComparison(self.connection)
        self.scalp_shadow = ScalpShadow(self.connection)
        self._probability_cache = None
        self._probability_cache_at = 0.0
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS samples (
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS samples_time ON samples(timestamp);
            CREATE INDEX IF NOT EXISTS samples_symbol_time
                ON samples(symbol, timestamp);
            CREATE TABLE IF NOT EXISTS alerts (
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                delivered INTEGER NOT NULL,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS alerts_time ON alerts(timestamp);
            CREATE TABLE IF NOT EXISTS errors (
                timestamp REAL NOT NULL,
                message TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_entry_rejections (
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                reason TEXT NOT NULL,
                spread_bps REAL,
                tick_percent REAL
            );
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                signal_kind TEXT NOT NULL,
                change_percent REAL NOT NULL,
                change_24h_percent REAL NOT NULL,
                quote_volume_usdt REAL NOT NULL,
                ai_score INTEGER,
                ai_verdict TEXT,
                quote_volume_5m_usdt REAL,
                volume_ratio_5m REAL,
                trades_5m INTEGER,
                taker_buy_ratio_percent REAL,
                spread_bps REAL,
                bid_depth_usdt REAL,
                ask_depth_usdt REAL,
                order_book_imbalance_percent REAL
                ,ai_decision TEXT
                ,ai_reason TEXT
                ,ai_risk TEXT
                ,analysis_version INTEGER NOT NULL DEFAULT 1
                ,change_15s_percent REAL
                ,change_30s_percent REAL
                ,change_60s_percent REAL
                ,change_180s_percent REAL
                ,change_300s_percent REAL
                ,pullback_from_high_percent REAL
                ,btc_change_60s_percent REAL
                ,btc_change_300s_percent REAL
                ,market_breadth_60s_percent REAL
                ,large_trade_threshold_usdt REAL
                ,large_buy_volume_15s_usdt REAL
                ,large_sell_volume_15s_usdt REAL
                ,large_buy_volume_60s_usdt REAL
                ,large_sell_volume_60s_usdt REAL
                ,large_trade_imbalance_60s_percent REAL
                ,large_trade_count_60s REAL
                ,bid_wall_share_percent REAL
                ,ask_wall_share_percent REAL
            );
            CREATE INDEX IF NOT EXISTS signal_events_time
                ON signal_events(timestamp);
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                signal_id INTEGER NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                measured_at REAL NOT NULL,
                exit_price REAL NOT NULL,
                gross_return_percent REAL NOT NULL,
                net_return_percent REAL NOT NULL,
                PRIMARY KEY(signal_id, horizon_minutes)
            );
            CREATE TABLE IF NOT EXISTS learning_examples (
                signal_id INTEGER PRIMARY KEY,
                matured_at REAL NOT NULL,
                signal_timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                success_before_stop INTEGER NOT NULL,
                reached_second_target INTEGER NOT NULL,
                maximum_return_percent REAL NOT NULL,
                minimum_return_percent REAL NOT NULL,
                setup_change_percent REAL NOT NULL,
                volume_ratio_5m REAL,
                taker_buy_ratio_percent REAL,
                order_book_imbalance_percent REAL,
                change_60s_percent REAL,
                pullback_from_high_percent REAL,
                ai_score INTEGER,
                ai_decision TEXT
            );
            CREATE INDEX IF NOT EXISTS learning_examples_symbol_time
                ON learning_examples(symbol, signal_timestamp);
            CREATE TABLE IF NOT EXISTS confirmation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at REAL NOT NULL,
                resolved_at REAL NOT NULL,
                symbol TEXT NOT NULL,
                trigger_price REAL NOT NULL,
                resolution_price REAL NOT NULL,
                accepted INTEGER NOT NULL,
                reason TEXT NOT NULL,
                evaluated_at REAL,
                immediate_success INTEGER,
                immediate_stopped_first INTEGER,
                immediate_max_return_percent REAL,
                immediate_min_return_percent REAL,
                delayed_success INTEGER,
                delayed_stopped_first INTEGER,
                delayed_max_return_percent REAL,
                delayed_min_return_percent REAL
                ,confirmation_progress_percent REAL
                ,confirmation_pullback_percent REAL
                ,confirmation_change_5s_percent REAL
                ,confirmation_change_10s_percent REAL
                ,volume_ratio_5m REAL
                ,taker_buy_ratio_percent REAL
                ,order_book_imbalance_percent REAL
                ,spread_bps REAL
                ,change_15s_percent REAL
                ,change_60s_percent REAL
                ,pullback_from_high_percent REAL
                ,btc_change_300s_percent REAL
                ,market_breadth_60s_percent REAL
                ,signal_kind TEXT
                ,flow_cvd_60s_percent REAL
                ,flow_trade_rate_acceleration REAL
                ,flow_price_change_60s_percent REAL
                ,flow_price_efficiency_per_10k REAL
                ,flow_ask_depletion_percent REAL
                ,flow_bid_support_percent REAL
                ,flow_spread_bps REAL
                ,flow_spread_change_bps REAL
            );
            CREATE INDEX IF NOT EXISTS confirmation_events_time
                ON confirmation_events(started_at);
            CREATE TABLE IF NOT EXISTS confirmation_samples (
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY(timestamp, symbol)
            );
            CREATE INDEX IF NOT EXISTS confirmation_samples_symbol_time
                ON confirmation_samples(symbol, timestamp);
            CREATE TABLE IF NOT EXISTS order_flow_snapshots (
                confirmation_id INTEGER PRIMARY KEY,
                timestamp REAL NOT NULL,
                symbol TEXT NOT NULL,
                buy_5s_usdt REAL, sell_5s_usdt REAL,
                buy_15s_usdt REAL, sell_15s_usdt REAL,
                buy_60s_usdt REAL, sell_60s_usdt REAL,
                cvd_60s_percent REAL,
                trade_rate_acceleration REAL,
                price_change_60s_percent REAL,
                price_efficiency_per_10k REAL,
                ask_depletion_percent REAL,
                bid_support_percent REAL,
                spread_bps REAL,
                spread_change_bps REAL
            );
            CREATE INDEX IF NOT EXISTS order_flow_snapshots_time
                ON order_flow_snapshots(timestamp);
            """
        )
        signal_columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(signal_events)")
        }
        for column, definition in (
            ("quote_volume_5m_usdt", "REAL"),
            ("volume_ratio_5m", "REAL"),
            ("trades_5m", "INTEGER"),
            ("taker_buy_ratio_percent", "REAL"),
            ("spread_bps", "REAL"),
            ("bid_depth_usdt", "REAL"),
            ("ask_depth_usdt", "REAL"),
            ("order_book_imbalance_percent", "REAL"),
            ("ai_decision", "TEXT"),
            ("ai_reason", "TEXT"),
            ("ai_risk", "TEXT"),
            ("analysis_version", "INTEGER NOT NULL DEFAULT 1"),
            ("change_15s_percent", "REAL"),
            ("change_30s_percent", "REAL"),
            ("change_60s_percent", "REAL"),
            ("change_180s_percent", "REAL"),
            ("change_300s_percent", "REAL"),
            ("pullback_from_high_percent", "REAL"),
            ("btc_change_60s_percent", "REAL"),
            ("btc_change_300s_percent", "REAL"),
            ("market_breadth_60s_percent", "REAL"),
        ):
            if column not in signal_columns:
                self.connection.execute(
                    f"ALTER TABLE signal_events ADD COLUMN {column} {definition}"
                )
        confirmation_columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(confirmation_events)")
        }
        for column in (
            "confirmation_progress_percent", "confirmation_pullback_percent",
            "confirmation_change_5s_percent", "confirmation_change_10s_percent",
            "volume_ratio_5m", "taker_buy_ratio_percent",
            "order_book_imbalance_percent", "spread_bps",
            "change_15s_percent", "change_60s_percent",
            "pullback_from_high_percent", "btc_change_300s_percent",
            "market_breadth_60s_percent",
            "large_trade_threshold_usdt", "large_buy_volume_15s_usdt",
            "large_sell_volume_15s_usdt", "large_buy_volume_60s_usdt",
            "large_sell_volume_60s_usdt", "large_trade_imbalance_60s_percent",
            "large_trade_count_60s", "bid_wall_share_percent",
            "ask_wall_share_percent",
            "trend_change_15m_percent", "trend_change_60m_percent",
            "trend_change_240m_percent", "trend_efficiency_15m_percent",
            "trend_efficiency_60m_percent", "trend_efficiency_240m_percent",
            "shadow_probability_percent", "shadow_model_examples",
            "flow_cvd_60s_percent", "flow_trade_rate_acceleration",
            "flow_price_change_60s_percent", "flow_price_efficiency_per_10k",
            "flow_ask_depletion_percent", "flow_bid_support_percent",
            "flow_spread_bps", "flow_spread_change_bps",
        ):
            if column not in confirmation_columns:
                self.connection.execute(
                    f"ALTER TABLE confirmation_events ADD COLUMN {column} REAL"
                )
        if "signal_kind" not in confirmation_columns:
            self.connection.execute(
                "ALTER TABLE confirmation_events ADD COLUMN signal_kind TEXT"
            )
        if self._metadata("period_started_at") is None:
            self._set_metadata("period_started_at", str(time.time()))
        self.connection.commit()

    def record_entry_rejection(
        self,
        timestamp: float,
        symbol: str,
        reason: str,
        spread_bps: float | None,
        tick_percent: float | None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO paper_entry_rejections("
            "timestamp, symbol, reason, spread_bps, tick_percent) "
            "VALUES(?, ?, ?, ?, ?)",
            (timestamp, symbol, reason, spread_bps, tick_percent),
        )
        self.connection.commit()

    def record_confirmation_event(self, event, context=None, dynamics=None,
                                  scalp_now=None, scalp_cost=0.2) -> int:
        dynamic_values = dynamics.as_dict() if dynamics is not None else {}
        feature_values = {
            "confirmation_progress_percent": getattr(event, "progress_percent", None),
            "confirmation_change_5s_percent": getattr(event, "change_5s_percent", None),
            "confirmation_change_10s_percent": getattr(event, "change_10s_percent", None),
            "volume_ratio_5m": getattr(context, "volume_ratio_5m", None),
            "taker_buy_ratio_percent": getattr(context, "taker_buy_ratio_percent", None),
            "order_book_imbalance_percent": getattr(context, "order_book_imbalance_percent", None),
            "spread_bps": getattr(context, "spread_bps", None),
            "change_15s_percent": dynamic_values.get("change_15s_percent"),
            "change_60s_percent": dynamic_values.get("change_60s_percent"),
            "pullback_from_high_percent": dynamic_values.get("pullback_from_5m_high_percent"),
            "btc_change_300s_percent": dynamic_values.get("btc_change_300s_percent"),
            "market_breadth_60s_percent": dynamic_values.get("market_breadth_60s_percent"),
            "large_buy_volume_15s_usdt": getattr(context, "large_buy_volume_15s_usdt", None),
            "large_sell_volume_15s_usdt": getattr(context, "large_sell_volume_15s_usdt", None),
            "large_buy_volume_60s_usdt": getattr(context, "large_buy_volume_60s_usdt", None),
            "large_sell_volume_60s_usdt": getattr(context, "large_sell_volume_60s_usdt", None),
            "large_trade_imbalance_60s_percent": getattr(context, "large_trade_imbalance_60s_percent", None),
            "large_trade_count_60s": getattr(context, "large_trade_count_60s", None),
            "trend_change_15m_percent": getattr(context, "trend_change_15m_percent", None),
            "trend_change_60m_percent": getattr(context, "trend_change_60m_percent", None),
            "trend_change_240m_percent": getattr(context, "trend_change_240m_percent", None),
            "trend_efficiency_15m_percent": getattr(context, "trend_efficiency_15m_percent", None),
            "trend_efficiency_60m_percent": getattr(context, "trend_efficiency_60m_percent", None),
            "trend_efficiency_240m_percent": getattr(context, "trend_efficiency_240m_percent", None),
            "flow_cvd_60s_percent": getattr(context, "flow_cvd_60s_percent", None),
            "flow_trade_rate_acceleration": getattr(context, "flow_trade_rate_acceleration", None),
            "flow_price_change_60s_percent": getattr(context, "flow_price_change_60s_percent", None),
            "flow_price_efficiency_per_10k": getattr(context, "flow_price_efficiency_per_10k", None),
            "flow_ask_depletion_percent": getattr(context, "flow_ask_depletion_percent", None),
            "flow_bid_support_percent": getattr(context, "flow_bid_support_percent", None),
            "flow_spread_bps": getattr(context, "flow_spread_bps", None),
            "flow_spread_change_bps": getattr(context, "flow_spread_change_bps", None),
        }
        if scalp_now is not None:
            self.scalp_shadow.candidate(
                event.symbol, getattr(event, "signal_kind", None), scalp_now,
                feature_values, event.accepted, scalp_cost,
            )
        model = self._current_probability_model(float(event.started_at))
        probability = model.predict_percent(feature_values) if model else None
        cursor = self.connection.execute(
            "INSERT INTO confirmation_events("
            "started_at,resolved_at,symbol,trigger_price,resolution_price,"
            "accepted,reason,confirmation_progress_percent,"
            "confirmation_pullback_percent,confirmation_change_5s_percent,"
            "confirmation_change_10s_percent,volume_ratio_5m,"
            "taker_buy_ratio_percent,order_book_imbalance_percent,spread_bps,"
            "change_15s_percent,change_60s_percent,pullback_from_high_percent,"
            "btc_change_300s_percent,market_breadth_60s_percent,"
            "large_trade_threshold_usdt,large_buy_volume_15s_usdt,"
            "large_sell_volume_15s_usdt,large_buy_volume_60s_usdt,"
            "large_sell_volume_60s_usdt,large_trade_imbalance_60s_percent,"
            "large_trade_count_60s,bid_wall_share_percent,"
            "ask_wall_share_percent,trend_change_15m_percent,"
            "trend_change_60m_percent,trend_change_240m_percent,"
            "trend_efficiency_15m_percent,trend_efficiency_60m_percent,"
            "trend_efficiency_240m_percent,shadow_probability_percent,"
            "shadow_model_examples,signal_kind) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                event.started_at, event.resolved_at, event.symbol,
                event.trigger_price, event.resolution_price,
                int(event.accepted), event.reason,
                getattr(event, "progress_percent", None),
                getattr(event, "pullback_percent", None),
                getattr(event, "change_5s_percent", None),
                getattr(event, "change_10s_percent", None),
                context.volume_ratio_5m if context else None,
                context.taker_buy_ratio_percent if context else None,
                context.order_book_imbalance_percent if context else None,
                context.spread_bps if context else None,
                dynamic_values.get("change_15s_percent"),
                dynamic_values.get("change_60s_percent"),
                dynamic_values.get("pullback_from_5m_high_percent"),
                dynamic_values.get("btc_change_300s_percent"),
                dynamic_values.get("market_breadth_60s_percent"),
                getattr(context, "large_trade_threshold_usdt", None),
                getattr(context, "large_buy_volume_15s_usdt", None),
                getattr(context, "large_sell_volume_15s_usdt", None),
                getattr(context, "large_buy_volume_60s_usdt", None),
                getattr(context, "large_sell_volume_60s_usdt", None),
                getattr(context, "large_trade_imbalance_60s_percent", None),
                getattr(context, "large_trade_count_60s", None),
                getattr(context, "bid_wall_share_percent", None),
                getattr(context, "ask_wall_share_percent", None),
                getattr(context, "trend_change_15m_percent", None),
                getattr(context, "trend_change_60m_percent", None),
                getattr(context, "trend_change_240m_percent", None),
                getattr(context, "trend_efficiency_15m_percent", None),
                getattr(context, "trend_efficiency_60m_percent", None),
                getattr(context, "trend_efficiency_240m_percent", None),
                probability,
                model.examples if model else None,
                getattr(event, "signal_kind", None),
            ),
        )
        confirmation_id = int(cursor.lastrowid)
        if context is not None and getattr(context, "flow_cvd_60s_percent", None) is not None:
            self.connection.execute(
                "UPDATE confirmation_events SET flow_cvd_60s_percent=?,"
                "flow_trade_rate_acceleration=?,flow_price_change_60s_percent=?,"
                "flow_price_efficiency_per_10k=?,flow_ask_depletion_percent=?,"
                "flow_bid_support_percent=?,flow_spread_bps=?,"
                "flow_spread_change_bps=? WHERE id=?",
                (
                    context.flow_cvd_60s_percent,
                    context.flow_trade_rate_acceleration,
                    context.flow_price_change_60s_percent,
                    context.flow_price_efficiency_per_10k,
                    context.flow_ask_depletion_percent,
                    context.flow_bid_support_percent,
                    context.flow_spread_bps,
                    context.flow_spread_change_bps,
                    confirmation_id,
                ),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO order_flow_snapshots("
                "confirmation_id,timestamp,symbol,buy_5s_usdt,sell_5s_usdt,"
                "buy_15s_usdt,sell_15s_usdt,buy_60s_usdt,sell_60s_usdt,"
                "cvd_60s_percent,trade_rate_acceleration,price_change_60s_percent,"
                "price_efficiency_per_10k,ask_depletion_percent,bid_support_percent,"
                "spread_bps,spread_change_bps) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    confirmation_id, event.resolved_at, event.symbol,
                    context.flow_buy_5s_usdt, context.flow_sell_5s_usdt,
                    context.flow_buy_15s_usdt, context.flow_sell_15s_usdt,
                    context.flow_buy_60s_usdt, context.flow_sell_60s_usdt,
                    context.flow_cvd_60s_percent,
                    context.flow_trade_rate_acceleration,
                    context.flow_price_change_60s_percent,
                    context.flow_price_efficiency_per_10k,
                    context.flow_ask_depletion_percent,
                    context.flow_bid_support_percent,
                    context.flow_spread_bps,
                    context.flow_spread_change_bps,
                ),
            )
        self.connection.commit()
        return confirmation_id

    def order_flow_report_text(self, now: float, lookback_seconds: int = 604800) -> str:
        rows = self.connection.execute(
            "SELECT c.immediate_success,c.immediate_stopped_first,"
            "f.cvd_60s_percent,f.trade_rate_acceleration,"
            "f.price_change_60s_percent,f.price_efficiency_per_10k,"
            "f.ask_depletion_percent,f.bid_support_percent,"
            "f.spread_bps,f.spread_change_bps "
            "FROM order_flow_snapshots f JOIN confirmation_events c "
            "ON c.id=f.confirmation_id WHERE f.timestamp>=? "
            "AND c.evaluated_at IS NOT NULL AND c.signal_kind LIKE '%лидер%'",
            (now - lookback_seconds,),
        ).fetchall()
        if not rows:
            return (
                "🌊 Теневой order flow лидеров\n"
                "Новые непрерывные снимки ещё не созрели 15 минут."
            )

        groups = {
            "цель": [row for row in rows if row[0] == 1],
            "стоп": [row for row in rows if row[1] == 1],
            "нейтр.": [row for row in rows if row[0] == 0 and row[1] == 0],
        }
        features = (
            ("CVD 60 с", 2, "%"),
            ("ускорение сделок", 3, "×"),
            ("цена 60 с", 4, "%"),
            ("эффективность / 10k", 5, "%"),
            ("снятие ask", 6, "%"),
            ("поддержка bid", 7, "%"),
            ("спред", 8, " б.п."),
            ("изменение спреда", 9, " б.п."),
        )
        lines = [
            "🌊 Теневой order flow лидеров",
            f"Созрело снимков: {len(rows)}; цель {len(groups['цель'])}, "
            f"стоп {len(groups['стоп'])}, нейтрально {len(groups['нейтр.'])}.",
        ]
        for label, index, suffix in features:
            values = []
            for name, group in groups.items():
                observed = [float(row[index]) for row in group if row[index] is not None]
                values.append(
                    f"{name} {sum(observed) / len(observed):.2f}{suffix} (n={len(observed)})"
                    if observed else f"{name} —"
                )
            lines.append(f"• {label}: " + ", ".join(values) + ".")

        absorption = [
            row for row in rows
            if row[2] is not None and row[4] is not None
            and row[2] >= 10 and row[4] <= 0.05
        ]
        continuation = [
            row for row in rows
            if row[2] is not None and row[4] is not None and row[5] is not None
            and row[2] > 0 and row[4] > 0.05 and row[5] > 0
        ]
        for label, cohort in (("поглощение покупок", absorption),
                              ("эффективное продолжение", continuation)):
            successes = sum(row[0] == 1 for row in cohort)
            rate = successes / len(cohort) * 100 if cohort else 0.0
            lines.append(f"• {label}: {successes}/{len(cohort)} целей ({rate:.1f}%).")
        lines.append("Пока это теневые гипотезы: на право входа не влияют.")
        return "\n".join(lines)

    def _probability_samples(self, before: float) -> list[tuple[int, dict]]:
        columns = ",".join(FEATURE_NAMES)
        rows = self.connection.execute(
            f"SELECT delayed_success,{columns} FROM confirmation_events "
            "WHERE evaluated_at IS NOT NULL AND delayed_success IS NOT NULL "
            "AND started_at < ? "
            "ORDER BY started_at DESC LIMIT 5000",
            (before,),
        ).fetchall()
        rows.reverse()
        return [
            (int(row[0]), dict(zip(FEATURE_NAMES, row[1:])))
            for row in rows
        ]

    def _current_probability_model(self, now: float):
        if self._probability_cache is None or now - self._probability_cache_at >= 300:
            self._probability_cache = train_probability_model(
                self._probability_samples(now)
            )
            self._probability_cache_at = now
        return self._probability_cache

    def probability_shadow_report_text(
        self, now: float, lookback_seconds: int = 7 * 86400
    ) -> str:
        model = self._current_probability_model(now)
        lines = ["🎯 Теневая вероятностная модель (общая, прежняя выборка)",
                 "Не является оценкой отдельного скальпинга v1."]
        if model is None:
            count = len(self._probability_samples(now))
            lines.append(f"Обучение накапливается: {count}/400 примеров.")
            return "\n".join(lines)
        lines.extend((
            f"Обучающих примеров: {model.examples}; контрольных: "
            f"{model.validation_examples}.",
            f"Контрольная база: {model.validation_base_rate_percent:.1f}%; "
            f"верхняя четверть прогнозов: "
            f"{model.validation_top_quartile_rate_percent:.1f}%.",
            f"Brier score: {model.validation_brier_score:.3f} "
            "(меньше — лучше).",
        ))
        lines.append("Контрольные диапазоны (только отложенная выборка):")
        for lower, upper, successes, count in model.validation_buckets:
            if not count:
                continue
            label = f"{lower}–{upper - 1}%" if upper < 101 else "60%+"
            lines.append(
                f"• прогноз {label}: {successes}/{count} "
                f"({successes / count * 100:.1f}%)."
            )
        rows = self.connection.execute(
            "SELECT shadow_probability_percent,delayed_success,"
            "delayed_stopped_first,COALESCE(spread_bps,0) "
            "FROM confirmation_events WHERE evaluated_at IS NOT NULL "
            "AND delayed_success IS NOT NULL AND started_at>=? "
            "AND shadow_probability_percent IS NOT NULL",
            (now - lookback_seconds,),
        ).fetchall()
        if not rows:
            lines.append("Новые прогнозы ещё не созрели 15 минут.")
            return "\n".join(lines)
        strong = [row for row in rows if float(row[0]) >= 40]
        lines.append(
            f"Созревшие теневые прогнозы за {lookback_seconds // 86400} дн.: "
            f"{len(rows)} (это отдельная выборка, не контрольная)."
        )
        if strong:
            successes = sum(int(row[1]) for row in strong)
            stops = sum(int(row[2] or 0) for row in strong)
            neutral = len(strong) - successes - stops
            # Conservative paper estimate: 0.1% fee on entry and exit plus
            # the observed entry spread. Neutral observations are closed flat.
            net = sum(
                (0.7 if int(row[1]) else -0.5 if int(row[2] or 0) else 0.0)
                - 0.2 - float(row[3] or 0) / 100
                for row in strong
            )
            lines.append(
                f"Прогноз 40%+: {len(strong)}; цели {successes}, стопы "
                f"{stops}, нейтральные {neutral}; упрощённая сумма "
                f"{net:+.2f} п.п. (не доходность банка; нейтральные условно по нулю, "
                "комиссия 0,2% + входной спред; неизвестный спред принят за ноль)."
            )
        lines.append("Модель пока не открывает сделки — только проверяется.")
        return "\n".join(lines)

    def leader_report_text(
        self, now: float, lookback_seconds: int = 86400
    ) -> str:
        rows = self.connection.execute(
            "SELECT s.signal_kind,COUNT(*),"
            "SUM(CASE WHEN s.ai_decision='BUY' THEN 1 ELSE 0 END),"
            "SUM(COALESCE(l.success_before_stop,0)),"
            "COUNT(l.signal_id) FROM signal_events s "
            "LEFT JOIN learning_examples l ON l.signal_id=s.id "
            "WHERE s.timestamp>=? AND s.signal_kind LIKE '%лидер%' "
            "GROUP BY s.signal_kind ORDER BY s.signal_kind",
            (now - lookback_seconds,),
        ).fetchall()
        lines = ["🚀 Усиленное наблюдение за лидерами"]
        if not rows:
            lines.append("Сигналы лидеров пока не сформированы.")
            return "\n".join(lines)
        for kind, signals, ai_buys, successes, matured in rows:
            rate = float(successes) / int(matured) * 100 if matured else 0.0
            lines.append(
                f"• {kind}: сигналов {signals}, AI BUY {ai_buys}; "
                f"созрело {matured}, цель раньше стопа {successes} "
                f"({rate:.1f}%)."
            )
        return "\n".join(lines)

    def leader_path_report_text(
        self, now: float, lookback_seconds: int = 86400,
        horizon_seconds: int = 3600,
    ) -> str:
        """Report the full price path after every detected leader candidate."""
        rows = self.connection.execute(
            "SELECT id,started_at,resolved_at,symbol,trigger_price,accepted "
            "FROM confirmation_events WHERE started_at>=? AND started_at<=? "
            "AND signal_kind LIKE '%лидер%' ORDER BY started_at",
            (now - lookback_seconds, now - horizon_seconds),
        ).fetchall()
        observations = []
        for event_id, started_at, resolved_at, symbol, trigger_price, accepted in rows:
            points = self.connection.execute(
                "SELECT timestamp,price FROM samples WHERE symbol=? "
                "AND timestamp>=? AND timestamp<=? ORDER BY timestamp",
                (symbol, started_at, float(started_at) + horizon_seconds),
            ).fetchall()
            if not points or float(points[-1][0]) < float(started_at) + horizon_seconds * 0.8:
                continue
            changes = [
                (float(price) / float(trigger_price) - 1) * 100
                for _timestamp, price in points
            ]
            success, stopped, maximum, minimum = self._path_outcome(
                [float(price) for _timestamp, price in points],
                float(trigger_price), 0.7, 0.5,
            )
            running_peak = changes[0]
            deepest_retracement = 0.0
            for change in changes:
                running_peak = max(running_peak, change)
                deepest_retracement = min(deepest_retracement, change - running_peak)
            signal = self.connection.execute(
                "SELECT id,ai_decision FROM signal_events WHERE symbol=? "
                "AND timestamp>=? AND timestamp<=? "
                "ORDER BY ABS(timestamp-?) LIMIT 1",
                (symbol, resolved_at, float(resolved_at) + 120, resolved_at),
            ).fetchone()
            ai_decision = str(signal[1]) if signal and signal[1] else None
            ai_blocked = False
            if signal is not None and ai_decision in {"WAIT", "SKIP"}:
                rejection = self.connection.execute(
                    "SELECT 1 FROM paper_entry_rejections WHERE symbol=? "
                    "AND timestamp>=? AND timestamp<=? AND reason LIKE 'AI решил %' LIMIT 1",
                    (symbol, resolved_at, float(resolved_at) + 120),
                ).fetchone()
                ai_blocked = rejection is not None
            observations.append({
                "accepted": bool(accepted), "success": bool(success),
                "stopped": bool(stopped), "maximum": maximum, "minimum": minimum,
                "end": changes[-1], "retracement": deepest_retracement,
                "ai_avoid": ai_decision in {"WAIT", "SKIP"},
                "ai_blocked": ai_blocked,
            })

        title = f"📈 Путь всех лидеров за {horizon_seconds // 60} минут"
        if not observations:
            return title + "\nСозревших наблюдений пока нет."

        def summary(label: str, items: list[dict]) -> str:
            if not items:
                return f"• {label}: пока нет."
            targets = sum(item["success"] for item in items)
            stops = sum(item["stopped"] for item in items)
            neutral = len(items) - targets - stops
            maxima = [float(item["maximum"]) for item in items]
            minima = [float(item["minimum"]) for item in items]
            reached_1 = sum(value >= 1 for value in maxima)
            reached_3 = sum(value >= 3 for value in maxima)
            reached_5 = sum(value >= 5 for value in maxima)
            average_end = sum(float(item["end"]) for item in items) / len(items)
            average_retracement = sum(
                float(item["retracement"]) for item in items
            ) / len(items)
            return (
                f"• {label}: {len(items)}; цель/стоп/нейтр. "
                f"{targets}/{stops}/{neutral}; максимум в среднем "
                f"{sum(maxima) / len(maxima):+.2f}% (медиана {median(maxima):+.2f}%); "
                f"минимум {sum(minima) / len(minima):+.2f}% "
                f"(медиана {median(minima):+.2f}%); достигли +1/+3/+5%: "
                f"{reached_1}/{reached_3}/{reached_5}; через час "
                f"{average_end:+.2f}%; откат от вершины {average_retracement:.2f} п.п."
            )

        lines = [title, summary("все", observations)]
        lines.append(summary(
            "прошли 20 секунд", [item for item in observations if item["accepted"]]
        ))
        lines.append(summary(
            "не прошли 20 секунд", [item for item in observations if not item["accepted"]]
        ))
        lines.append(summary(
            "AI WAIT/SKIP", [item for item in observations if item["ai_avoid"]]
        ))
        blocked = [item for item in observations if item["ai_blocked"]]
        if blocked:
            lines.append(summary("раньше заблокированы AI", blocked))
        return "\n".join(lines)

    def leader_funnel_report_text(self, now: float, since: float) -> str:
        """Show where green-12h leader candidates disappear before a trade."""
        confirmation = self.connection.execute(
            "SELECT accepted,reason FROM confirmation_events "
            "WHERE resolved_at>=? AND resolved_at<? "
            "AND signal_kind LIKE '%лидер%'",
            (since, now),
        ).fetchall()
        passed_confirmation = sum(bool(row[0]) for row in confirmation)
        confirmation_reasons: dict[str, int] = {}
        for accepted, reason in confirmation:
            if accepted:
                continue
            category = self._rejection_category(str(reason or ""))
            confirmation_reasons[category] = confirmation_reasons.get(category, 0) + 1

        signals = self.connection.execute(
            "SELECT timestamp,symbol,ai_score FROM signal_events "
            "WHERE timestamp>=? AND timestamp<? AND signal_kind LIKE '%лидер%'",
            (since, now),
        ).fetchall()
        post_reasons: dict[str, int] = {}
        for timestamp, symbol, _ai_score in signals:
            rejection = self.connection.execute(
                "SELECT reason FROM paper_entry_rejections WHERE symbol=? "
                "AND ABS(timestamp-?)<=0.01 ORDER BY rowid DESC LIMIT 1",
                (symbol, timestamp),
            ).fetchone()
            if rejection is not None:
                category = self._rejection_category(str(rejection[0]))
                post_reasons[category] = post_reasons.get(category, 0) + 1

        has_positions = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='paper_positions'"
        ).fetchone() is not None
        purchases = fallback = 0
        if has_positions:
            purchases, fallback = self.connection.execute(
                "SELECT COUNT(*),SUM(CASE WHEN ai_score=0 THEN 1 ELSE 0 END) "
                "FROM paper_positions WHERE opened_at>=? AND opened_at<? "
                "AND signal_kind LIKE '%лидер%'",
                (since, now),
            ).fetchone()
            purchases = int(purchases or 0)
            fallback = int(fallback or 0)
        ai_success = sum(row[2] is not None for row in signals)
        post_text = ", ".join(
            f"{name} {count}" for name, count in sorted(
                post_reasons.items(), key=lambda item: -item[1]
            )
        ) or "нет"
        confirmation_text = ", ".join(
            f"{name} {count}" for name, count in sorted(
                confirmation_reasons.items(), key=lambda item: -item[1]
            )
        ) or "нет"
        return (
            "🛰 Воронка лидеров\n"
            f"После фильтра роста за 12 ч: {len(confirmation)}.\n"
            f"Выдержали 20 секунд: {passed_confirmation}; отклонены: "
            f"{len(confirmation) - passed_confirmation} ({confirmation_text}).\n"
            f"Дошли до рыночной проверки: {len(signals)}.\n"
            f"Получили ответ AI: {ai_success}; без ответа: "
            f"{len(signals) - ai_success}.\n"
            f"Отклонены после подтверждения: {sum(post_reasons.values())} "
            f"({post_text}).\n"
            f"Тестовых покупок: {purchases}; из них резервных без AI: {fallback}."
        )

    def active_confirmation_symbols(
        self, now: float, horizon_seconds: int = 900
    ) -> set[str]:
        rows = self.connection.execute(
            "SELECT DISTINCT symbol FROM confirmation_events "
            "WHERE evaluated_at IS NULL AND started_at > ?",
            (now - horizon_seconds,),
        ).fetchall()
        return {str(row[0]) for row in rows}

    def record_confirmation_prices(
        self, prices: dict[str, float], timestamp: float
    ) -> None:
        if not prices:
            return
        self.connection.executemany(
            "INSERT OR REPLACE INTO confirmation_samples(timestamp,symbol,price) "
            "VALUES(?,?,?)",
            ((timestamp, symbol, price) for symbol, price in prices.items()),
        )
        self.connection.commit()

    @staticmethod
    def _path_outcome(
        prices: list[float], entry_price: float,
        target_percent: float, stop_percent: float,
    ) -> tuple[bool, bool, float, float]:
        changes = [(price / entry_price - 1) * 100 for price in prices]
        success = False
        stopped_first = False
        for change in changes:
            if change <= -stop_percent:
                stopped_first = True
                break
            if change >= target_percent:
                success = True
                break
        return success, stopped_first, max(changes), min(changes)

    def refresh_confirmation_outcomes(
        self,
        now: float,
        target_percent: float,
        stop_percent: float,
        horizon_seconds: int = 900,
    ) -> int:
        rows = self.connection.execute(
            "SELECT id,started_at,resolved_at,symbol,trigger_price,"
            "resolution_price,accepted FROM confirmation_events "
            "WHERE (evaluated_at IS NULL OR delayed_success IS NULL) "
            "AND started_at <= ? ORDER BY id",
            (now - horizon_seconds,),
        ).fetchall()
        updated = 0
        for event_id, started_at, resolved_at, symbol, trigger, resolved, accepted in rows:
            points = self.connection.execute(
                "SELECT timestamp,price FROM confirmation_samples WHERE symbol=? "
                "AND timestamp>=? AND timestamp<=? ORDER BY timestamp",
                (symbol, started_at, float(started_at) + horizon_seconds),
            ).fetchall()
            if not points or float(points[-1][0]) < float(started_at) + horizon_seconds * 0.8:
                continue
            immediate_prices = [float(price) for _timestamp, price in points]
            immediate = self._path_outcome(
                immediate_prices, float(trigger), target_percent, stop_percent
            )
            delayed = (None, None, None, None)
            delayed_prices = [
                float(price) for timestamp, price in points
                if float(timestamp) >= float(resolved_at)
            ]
            if delayed_prices:
                delayed = self._path_outcome(
                    delayed_prices, float(resolved), target_percent, stop_percent
                )
            self.connection.execute(
                "UPDATE confirmation_events SET evaluated_at=?,"
                "immediate_success=?,immediate_stopped_first=?,"
                "immediate_max_return_percent=?,immediate_min_return_percent=?,"
                "delayed_success=?,delayed_stopped_first=?,"
                "delayed_max_return_percent=?,delayed_min_return_percent=? "
                "WHERE id=?",
                (now, int(immediate[0]), int(immediate[1]), immediate[2], immediate[3],
                 None if delayed[0] is None else int(delayed[0]),
                 None if delayed[1] is None else int(delayed[1]),
                 delayed[2], delayed[3], event_id),
            )
            updated += 1
        if updated:
            self.connection.execute(
                "DELETE FROM confirmation_samples WHERE timestamp < ?",
                (now - 172800,),
            )
            self.connection.commit()
        return updated

    def build_confirmation_audit(self, now: float, lookback_seconds: int = 86400) -> ConfirmationAudit:
        rows = self.connection.execute(
            "SELECT accepted,immediate_success,immediate_stopped_first,"
            "delayed_success FROM confirmation_events "
            "WHERE evaluated_at IS NOT NULL AND started_at>=?",
            (now - lookback_seconds,),
        ).fetchall()
        accepted = sum(int(row[0]) for row in rows)
        rejected_rows = [row for row in rows if not int(row[0])]
        return ConfirmationAudit(
            len(rows), accepted, len(rows) - accepted,
            sum(int(row[1]) for row in rows),
            sum(int(row[3] or 0) for row in rows if int(row[0])),
            sum(int(row[2]) for row in rejected_rows),
            sum(int(row[1]) for row in rejected_rows),
            sum(not int(row[1]) and not int(row[2]) for row in rejected_rows),
        )

    def _metadata(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    def _set_metadata(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def period_started_at(self) -> float:
        return float(self._metadata("period_started_at") or time.time())

    def report_due(self, now: float) -> bool:
        return now - self.period_started_at() >= 86400

    def record_prices(self, prices: dict[str, float], timestamp: float) -> None:
        self.connection.executemany(
            "INSERT INTO samples(timestamp, symbol, price) VALUES(?, ?, ?)",
            ((timestamp, symbol, price) for symbol, price in prices.items()),
        )
        self.connection.commit()

    def record_alert(
        self, symbol: str, delivered: bool, timestamp: float, error: str | None = None
    ) -> None:
        self.connection.execute(
            "INSERT INTO alerts(timestamp, symbol, delivered, error) VALUES(?, ?, ?, ?)",
            (timestamp, symbol, int(delivered), error),
        )
        self.connection.commit()

    def record_signal(
        self,
        timestamp: float,
        symbol: str,
        entry_price: float,
        signal_kind: str,
        change_percent: float,
        change_24h_percent: float,
        quote_volume_usdt: float,
        ai_score: int | None,
        ai_verdict: str | None,
        quote_volume_5m_usdt: float | None = None,
        volume_ratio_5m: float | None = None,
        trades_5m: int | None = None,
        taker_buy_ratio_percent: float | None = None,
        spread_bps: float | None = None,
        bid_depth_usdt: float | None = None,
        ask_depth_usdt: float | None = None,
        order_book_imbalance_percent: float | None = None,
        ai_decision: str | None = None,
        ai_reason: str | None = None,
        ai_risk: str | None = None,
        analysis_version: int = 1,
        entry_dynamics: dict | None = None,
    ) -> int:
        dynamics = entry_dynamics or {}
        cursor = self.connection.execute(
            "INSERT INTO signal_events("
            "timestamp, symbol, entry_price, signal_kind, change_percent, "
            "change_24h_percent, quote_volume_usdt, ai_score, ai_verdict, "
            "quote_volume_5m_usdt, volume_ratio_5m, trades_5m, "
            "taker_buy_ratio_percent, spread_bps, bid_depth_usdt, ask_depth_usdt, "
            "order_book_imbalance_percent, ai_decision, ai_reason, ai_risk, "
            "analysis_version, "
            "change_15s_percent, change_30s_percent, change_60s_percent, "
            "change_180s_percent, change_300s_percent, "
            "pullback_from_high_percent, btc_change_60s_percent, "
            "btc_change_300s_percent, market_breadth_60s_percent) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                timestamp,
                symbol,
                entry_price,
                signal_kind,
                change_percent,
                change_24h_percent,
                quote_volume_usdt,
                ai_score,
                ai_verdict,
                quote_volume_5m_usdt,
                volume_ratio_5m,
                trades_5m,
                taker_buy_ratio_percent,
                spread_bps,
                bid_depth_usdt,
                ask_depth_usdt,
                order_book_imbalance_percent,
                ai_decision,
                ai_reason,
                ai_risk,
                analysis_version,
                dynamics.get("change_15s_percent"),
                dynamics.get("change_30s_percent"),
                dynamics.get("change_60s_percent"),
                dynamics.get("change_180s_percent"),
                dynamics.get("change_300s_percent"),
                dynamics.get("pullback_from_5m_high_percent"),
                dynamics.get("btc_change_60s_percent"),
                dynamics.get("btc_change_300s_percent"),
                dynamics.get("market_breadth_60s_percent"),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def record_due_outcomes(
        self,
        prices: dict[str, float],
        now: float,
        estimated_round_trip_cost_percent: float,
    ) -> int:
        rows = self.connection.execute(
            "SELECT id, timestamp, symbol, entry_price FROM signal_events "
            "WHERE timestamp <= ?",
            (now - 15 * 60,),
        ).fetchall()
        inserted = 0
        for signal_id, timestamp, symbol, entry_price in rows:
            current_price = prices.get(str(symbol))
            if current_price is None:
                continue
            age = now - float(timestamp)
            for minutes in (15, 30, 60):
                if age < minutes * 60:
                    continue
                exists = self.connection.execute(
                    "SELECT 1 FROM signal_outcomes "
                    "WHERE signal_id = ? AND horizon_minutes = ?",
                    (signal_id, minutes),
                ).fetchone()
                if exists is not None:
                    continue
                gross_return = (current_price / float(entry_price) - 1) * 100
                net_return = gross_return - estimated_round_trip_cost_percent
                self.connection.execute(
                    "INSERT INTO signal_outcomes("
                    "signal_id, horizon_minutes, measured_at, exit_price, "
                    "gross_return_percent, net_return_percent"
                    ") VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        signal_id,
                        minutes,
                        now,
                        current_price,
                        gross_return,
                        net_return,
                    ),
                )
                inserted += 1
        if inserted:
            self.connection.commit()
        return inserted

    def refresh_learning_examples(
        self,
        now: float,
        first_target_percent: float,
        second_target_percent: float,
        stop_loss_percent: float,
        horizon_seconds: int = 900,
    ) -> int:
        """Turn matured full-AI signals into persistent supervised examples."""
        rows = self.connection.execute(
            "SELECT s.id, s.timestamp, s.symbol, s.entry_price, "
            "s.change_percent, s.volume_ratio_5m, s.taker_buy_ratio_percent, "
            "s.order_book_imbalance_percent, s.change_60s_percent, "
            "s.pullback_from_high_percent, s.ai_score, s.ai_decision "
            "FROM signal_events s "
            "LEFT JOIN learning_examples l ON l.signal_id = s.id "
            "WHERE l.signal_id IS NULL AND s.analysis_version >= 2 "
            "AND s.ai_score IS NOT NULL AND s.ai_decision IS NOT NULL "
            "AND s.timestamp <= ? ORDER BY s.timestamp",
            (now - horizon_seconds,),
        ).fetchall()
        inserted = 0
        for row in rows:
            signal_id = int(row[0])
            event_at = float(row[1])
            symbol = str(row[2])
            entry_price = float(row[3])
            points = self.connection.execute(
                "SELECT timestamp, price FROM samples WHERE symbol = ? "
                "AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                (symbol, event_at, event_at + horizon_seconds),
            ).fetchall()
            if not points or float(points[-1][0]) < event_at + horizon_seconds * 0.8:
                continue
            changes = [
                (float(price) / entry_price - 1) * 100
                for _timestamp, price in points
            ]
            first_hit = False
            second_hit = False
            for change in changes:
                if not first_hit and change <= -stop_loss_percent:
                    break
                if change >= first_target_percent:
                    first_hit = True
                if first_hit and change >= second_target_percent:
                    second_hit = True
                    break
            self.connection.execute(
                "INSERT OR IGNORE INTO learning_examples("
                "signal_id, matured_at, signal_timestamp, symbol, "
                "success_before_stop, reached_second_target, "
                "maximum_return_percent, minimum_return_percent, "
                "setup_change_percent, volume_ratio_5m, "
                "taker_buy_ratio_percent, order_book_imbalance_percent, "
                "change_60s_percent, pullback_from_high_percent, ai_score, "
                "ai_decision) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    signal_id, now, event_at, symbol, int(first_hit),
                    int(second_hit), max(changes), min(changes), float(row[4]),
                    row[5], row[6], row[7], row[8], row[9], row[10], row[11],
                ),
            )
            inserted += 1
        if inserted:
            self.connection.commit()
        return inserted

    @staticmethod
    def _similar_learning_example(row: tuple, current: dict) -> bool:
        comparisons = (
            (row[3], current.get("confirmation_progress_percent"), 0.25),
            (row[9], current.get("confirmation_change_5s_percent"), 0.15),
            (row[10], current.get("confirmation_change_10s_percent"), 0.20),
            (row[4], current.get("volume_ratio_5m"), 0.8),
            (row[5], current.get("taker_buy_ratio_percent"), 8.0),
            (row[6], current.get("order_book_imbalance_percent"), 30.0),
            (row[7], current.get("change_60s_percent"), 0.35),
            (row[8], current.get("pullback_from_high_percent"), 0.25),
            (row[11], current.get("large_trade_imbalance_60s_percent"), 35.0),
            (row[12], current.get("large_trade_count_60s"), 6.0),
            (row[13], current.get("bid_wall_share_percent"), 15.0),
            (row[14], current.get("ask_wall_share_percent"), 15.0),
        )
        available = 0
        matches = 0
        for historical, present, tolerance in comparisons:
            if historical is None or present is None:
                continue
            available += 1
            matches += abs(float(historical) - float(present)) <= tolerance
        return available >= 3 and matches / available >= 0.6

    def build_learning_profile(
        self,
        symbol: str,
        now: float,
        current_features: dict,
        lookback_seconds: int = 30 * 86400,
        strategy: str | None = None,
    ) -> LearningProfile:
        if strategy == "scalp":
            # Never reuse trigger-price labels or rocket PnL for ordinary entries.
            samples = [s for symbol_, _, s, _ in self.scalp_shadow.samples(now)
                       if symbol_ == symbol]
            hits = sum(s["label"] for s in samples)
            failures = 0
            for s in reversed(samples):
                if s["label"]:
                    break
                failures += 1
            return LearningProfile(
                symbol, len(samples), hits, 0, 0, failures, 0,
                "SHADOW_ONLY", 0,
                "Отдельный скальпинг: исходы от наблюдаемого ask после решения; "
                "история ракет не используется. Новая модель пока не меняет пороги.",
            )
        rows = self.connection.execute(
            "SELECT symbol, success_before_stop, signal_timestamp, "
            "setup_change_percent, volume_ratio_5m, taker_buy_ratio_percent, "
            "order_book_imbalance_percent, change_60s_percent, "
            "pullback_from_high_percent, NULL, NULL, NULL, NULL, NULL, NULL "
            "FROM learning_examples "
            "WHERE signal_timestamp >= ? "
            "AND NOT EXISTS (SELECT 1 FROM confirmation_events c "
            "WHERE c.symbol=learning_examples.symbol "
            "AND c.evaluated_at IS NOT NULL "
            "AND ABS(c.started_at-learning_examples.signal_timestamp)<=60) "
            "ORDER BY signal_timestamp DESC LIMIT 2000",
            (now - lookback_seconds,),
        ).fetchall()
        shadow_rows = self.connection.execute(
            "SELECT symbol, immediate_success, started_at, "
            "confirmation_progress_percent, volume_ratio_5m, "
            "taker_buy_ratio_percent, order_book_imbalance_percent, "
            "change_60s_percent, pullback_from_high_percent, "
            "confirmation_change_5s_percent, confirmation_change_10s_percent, "
            "large_trade_imbalance_60s_percent, large_trade_count_60s, "
            "bid_wall_share_percent, ask_wall_share_percent "
            "FROM confirmation_events "
            "WHERE evaluated_at IS NOT NULL AND started_at >= ? "
            "ORDER BY started_at DESC LIMIT 4000",
            (now - lookback_seconds,),
        ).fetchall()
        rows = sorted(
            [*rows, *shadow_rows],
            key=lambda row: float(row[2]),
            reverse=True,
        )
        symbol_rows = [row for row in rows if str(row[0]) == symbol]
        similar_rows = [
            row for row in rows
            if str(row[0]) != symbol
            and self._similar_learning_example(row, current_features)
        ][:200]
        symbol_successes = sum(int(row[1]) for row in symbol_rows)
        similar_successes = sum(int(row[1]) for row in similar_rows)
        consecutive_failures = 0
        for row in symbol_rows:
            if int(row[1]):
                break
            consecutive_failures += 1
        trade_losses = 0
        has_positions = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'paper_positions'"
        ).fetchone()
        if has_positions is not None:
            trade_rows = self.connection.execute(
                "SELECT realized_pnl_usdt FROM paper_positions "
                "WHERE symbol = ? AND status = 'CLOSED' "
                "ORDER BY closed_at DESC LIMIT 3",
                (symbol,),
            ).fetchall()
            for trade_row in trade_rows:
                if float(trade_row[0]) >= 0:
                    break
                trade_losses += 1
        symbol_rate = (
            symbol_successes / len(symbol_rows) if symbol_rows else None
        )
        similar_rate = (
            similar_successes / len(similar_rows) if similar_rows else None
        )
        status = "LEARNING"
        adjustment = 0
        explanation = "Недостаточно размеченных примеров; действует базовый фильтр."
        if trade_losses:
            status = "CAUTION" if trade_losses == 1 else "HIGH_CAUTION"
            adjustment = (5, 10, 15)[min(trade_losses, 3) - 1]
            explanation = (
                f"Последние убыточные сделки подряд: {trade_losses}; "
                "требуется усиленное подтверждение нового импульса."
            )
        if len(symbol_rows) >= 4 and (
            symbol_rate is not None and symbol_rate < 0.35
            or consecutive_failures >= 3
        ):
            status = "HIGH_CAUTION"
            adjustment = max(adjustment, 15)
            explanation = (
                "Монета повторяет неудачные импульсы: цель +0,7% редко "
                "достигается раньше стопа; разрешён только исключительно "
                "сильный новый сценарий."
            )
        elif len(similar_rows) >= 12 and similar_rate is not None and similar_rate < 0.35:
            status = "HIGH_CAUTION"
            adjustment = max(adjustment, 12)
            explanation = (
                "Похожие рыночные ситуации чаще заканчиваются стопом, чем целью."
            )
        elif not trade_losses and len(symbol_rows) >= 4 and symbol_rate is not None and symbol_rate >= 0.65:
            status = "FAVORABLE"
            adjustment = -3
            explanation = "Монета стабильно достигала первой цели в похожих импульсах."
        elif not trade_losses and len(similar_rows) >= 12 and similar_rate is not None and similar_rate >= 0.60:
            status = "FAVORABLE"
            adjustment = -2
            explanation = "Похожие рыночные ситуации имеют положительную историю."
        elif not trade_losses and (len(symbol_rows) >= 3 or len(similar_rows) >= 8):
            status = "CAUTION"
            adjustment = 5
            explanation = "История смешанная; для входа требуется более сильное решение AI."
        return LearningProfile(
            symbol, len(symbol_rows), symbol_successes, len(similar_rows),
            similar_successes, consecutive_failures, trade_losses, status, adjustment,
            explanation,
        )

    def build_learning_report(self, now: float) -> LearningReport:
        detailed = self.connection.execute(
            "SELECT symbol, success_before_stop FROM learning_examples "
            "WHERE signal_timestamp >= ? "
            "AND NOT EXISTS (SELECT 1 FROM confirmation_events c "
            "WHERE c.symbol=learning_examples.symbol "
            "AND c.evaluated_at IS NOT NULL "
            "AND ABS(c.started_at-learning_examples.signal_timestamp)<=60)",
            (now - 30 * 86400,),
        ).fetchall()
        shadow = self.connection.execute(
            "SELECT symbol, immediate_success FROM confirmation_events "
            "WHERE evaluated_at IS NOT NULL AND started_at >= ?",
            (now - 30 * 86400,),
        ).fetchall()
        grouped: dict[str, list[int]] = {}
        for symbol, success in list(detailed) + list(shadow):
            grouped.setdefault(str(symbol), []).append(int(success))
        rows = [
            (symbol, len(values), sum(values))
            for symbol, values in grouped.items()
        ]
        ranked = [
            (str(symbol), int(count), float(successes or 0) / int(count) * 100)
            for symbol, count, successes in rows
        ]
        eligible = [item for item in ranked if item[1] >= 4]
        best = tuple(sorted(eligible, key=lambda item: (-item[2], -item[1]))[:5])
        blocked = tuple(
            sorted(
                (item for item in eligible if item[2] < 35),
                key=lambda item: (item[2], -item[1]),
            )[:5]
        )
        return LearningReport(
            sum(item[1] for item in ranked),
            sum(round(item[1] * item[2] / 100) for item in ranked),
            len(eligible), best, blocked,
        )

    def candidate_pattern_report_text(
        self, now: float, lookback_seconds: int = 7 * 86400
    ) -> str:
        """Compare resolved market snapshots that won and lost."""
        rows = self.connection.execute(
            "SELECT immediate_success, confirmation_progress_percent, "
            "confirmation_change_5s_percent, confirmation_change_10s_percent, "
            "volume_ratio_5m, taker_buy_ratio_percent, "
            "order_book_imbalance_percent, spread_bps, change_60s_percent, "
            "pullback_from_high_percent, market_breadth_60s_percent "
            ",large_trade_threshold_usdt,large_buy_volume_15s_usdt,"
            "large_sell_volume_15s_usdt,large_buy_volume_60s_usdt,"
            "large_sell_volume_60s_usdt,large_trade_imbalance_60s_percent,"
            "large_trade_count_60s,bid_wall_share_percent,ask_wall_share_percent,"
            "trend_change_15m_percent,trend_change_60m_percent,"
            "trend_change_240m_percent,trend_efficiency_15m_percent,"
            "trend_efficiency_60m_percent,trend_efficiency_240m_percent "
            "FROM confirmation_events WHERE evaluated_at IS NOT NULL "
            "AND started_at>=? AND confirmation_progress_percent IS NOT NULL",
            (now - lookback_seconds,),
        ).fetchall()
        winners = [row for row in rows if int(row[0])]
        losers = [row for row in rows if not int(row[0])]
        def average(group: list[tuple], index: int) -> tuple[float | None, int]:
            values = [float(row[index]) for row in group if row[index] is not None]
            return (sum(values) / len(values), len(values)) if values else (None, 0)

        features = (
            ("ход за 20 с", 1, "%"),
            ("ход за последние 5 с", 2, "%"),
            ("ход за последние 10 с", 3, "%"),
            ("объём", 4, "×"),
            ("taker-buy", 5, "%"),
            ("перевес стакана", 6, "%"),
            ("спред", 7, " б.п."),
            ("ход за 60 с", 8, "%"),
            ("откат от максимума", 9, "%"),
            ("ширина рынка", 10, "%"),
            ("порог крупной сделки", 11, " USDT"),
            ("крупные покупки 15 с", 12, " USDT"),
            ("крупные продажи 15 с", 13, " USDT"),
            ("крупные покупки 60 с", 14, " USDT"),
            ("крупные продажи 60 с", 15, " USDT"),
            ("перевес крупного потока 60 с", 16, "%"),
            ("крупных сделок 60 с", 17, ""),
            ("доля крупнейшей bid-стенки", 18, "%"),
            ("доля крупнейшей ask-стенки", 19, "%"),
            ("тренд за 15 мин", 20, "%"),
            ("тренд за 1 ч", 21, "%"),
            ("тренд за 4 ч", 22, "%"),
            ("эффективность тренда 15 мин", 23, "%"),
            ("эффективность тренда 1 ч", 24, "%"),
            ("эффективность тренда 4 ч", 25, "%"),
        )
        lines = ["🔬 Победители против остальных"]
        if rows:
            lines.append(
                f"Расширенных снимков: {len(rows)}; цель достигли {len(winners)} "
                f"({len(winners) / len(rows) * 100:.1f}%)."
            )
        if winners and losers:
            for label, index, suffix in features:
                (winner_average, winner_count) = average(winners, index)
                (loser_average, loser_count) = average(losers, index)
                if winner_average is None or loser_average is None:
                    continue
                prefix = "×" if suffix == "×" else ""
                suffix_text = "" if suffix == "×" else suffix
                lines.append(
                    f"• {label}: успешно {prefix}{winner_average:.2f}{suffix_text} "
                    f"(n={winner_count}), неуспешно "
                    f"{prefix}{loser_average:.2f}{suffix_text} (n={loser_count})."
                )
        else:
            lines.append(
                "Для сравнения нужны созревшие снимки и успешной, "
                "и неуспешной группы."
            )
        rescue_rows = self.connection.execute(
            "SELECT resolved_at,symbol,accepted,delayed_success,"
            "delayed_stopped_first FROM confirmation_events "
            "WHERE evaluated_at IS NOT NULL AND started_at>=? "
            "AND reason LIKE 'повторное ускорение%'",
            (now - lookback_seconds,),
        ).fetchall()
        if rescue_rows:
            recovered = [row for row in rescue_rows if int(row[2])]
            ai_checked = 0
            ai_buy = 0
            purchased = 0
            has_positions = self.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='paper_positions'"
            ).fetchone() is not None
            for resolved_at, symbol, accepted, _success, _stopped in recovered:
                decision = self.connection.execute(
                    "SELECT ai_decision FROM signal_events WHERE symbol=? "
                    "AND ABS(timestamp-?)<=15 AND ai_score IS NOT NULL "
                    "ORDER BY ABS(timestamp-?) LIMIT 1",
                    (symbol, resolved_at, resolved_at),
                ).fetchone()
                if decision is not None:
                    ai_checked += 1
                    ai_buy += str(decision[0]) == "BUY"
                if has_positions:
                    purchased += self.connection.execute(
                        "SELECT COUNT(*) FROM paper_positions WHERE symbol=? "
                        "AND ABS(opened_at-?)<=15",
                        (symbol, resolved_at),
                    ).fetchone()[0] > 0
            rescue_successes = sum(int(row[3] or 0) for row in recovered)
            rescue_stops = sum(int(row[4] or 0) for row in recovered)
            lines.extend((
                "",
                "🔁 Второй шанс — 90 секунд",
                f"Наблюдение завершено: {len(rescue_rows)}.",
                f"Повторно ускорились: {len(recovered)}.",
                f"Дошли до AI: {ai_checked}; AI BUY: {ai_buy}; "
                f"тестовых покупок: {purchased}.",
                f"После повторного ускорения: цель +0,7% — "
                f"{rescue_successes}, стоп раньше цели — {rescue_stops}, "
                f"нейтрально — {len(recovered) - rescue_successes - rescue_stops}.",
            ))
        else:
            lines.extend(("", "🔁 Второй шанс: результаты накапливаются."))
        return "\n".join(lines)

    def recent_ai_decisions_text(self, limit: int = 8) -> str:
        rows = self.connection.execute(
            "SELECT timestamp, symbol, ai_score, ai_decision, ai_reason "
            "FROM signal_events WHERE analysis_version >= 2 "
            "AND ai_score IS NOT NULL ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        if not rows:
            return "🤖 Решения AI пока не накоплены."
        lines = ["🤖 Последние решения AI"]
        for timestamp, symbol, score, decision, reason in rows:
            clock = time.strftime("%H:%M", time.localtime(float(timestamp)))
            compact_reason = " ".join(str(reason or "").split())[:220]
            lines.append(
                f"• {clock} {symbol}: {decision}, {score}/100. {compact_reason}"
            )
        return "\n".join(lines)

    @staticmethod
    def _rejection_category(reason: str) -> str:
        lowered = reason.lower()
        if "20 сек" in lowered or "подтвержден" in lowered or "импульс исчез" in lowered:
            return "не подтверждён импульс"
        if "история" in lowered:
            return "история монеты"
        if "ai решил" in lowered or "решения ai" in lowered:
            return "решение AI"
        if "спред" in lowered or "исполн" in lowered or "шаг цены" in lowered:
            return "исполнение/спред"
        if "объём" in lowered or "покупател" in lowered or "стакан" in lowered:
            return "рыночное качество"
        return "другие фильтры"

    def observer_report_text(self, now: float, since: float) -> str | None:
        decisions = self.connection.execute(
            "SELECT ai_decision, COUNT(*) FROM signal_events "
            "WHERE timestamp >= ? AND analysis_version >= 2 "
            "AND ai_score IS NOT NULL GROUP BY ai_decision",
            (since,),
        ).fetchall()
        rejection_rows = self.connection.execute(
            "SELECT reason FROM paper_entry_rejections WHERE timestamp >= ?",
            (since,),
        ).fetchall()
        error_rows = self.connection.execute(
            "SELECT message FROM errors WHERE timestamp >= ?", (since,)
        ).fetchall()
        error_count = len(error_rows)
        if not decisions and not rejection_rows and not error_count:
            return None
        decision_counts = {str(name or "ERROR"): int(count) for name, count in decisions}
        categories: dict[str, int] = {}
        for row in rejection_rows:
            category = self._rejection_category(str(row[0]))
            categories[category] = categories.get(category, 0) + 1
        hours = max(0.0, (now - since) / 3600)
        decisions_text = ", ".join(
            f"{name} {count}" for name, count in sorted(decision_counts.items())
        ) or "полных решений не было"
        filters_text = ", ".join(
            f"{name} {count}"
            for name, count in sorted(categories.items(), key=lambda item: -item[1])[:5]
        ) or "нет"
        error_categories: dict[str, int] = {}
        openai_subtypes: dict[str, int] = {}
        for row in error_rows:
            message = str(row[0] or "").lower()
            if message.startswith("openai") or "openai" in message:
                category = "OpenAI"
                subtype = "другое"
                for name in ("timeout", "format", "empty", "http-429", "http-500", "http-502", "http-503"):
                    if f"[{name}]" in message:
                        subtype = name
                        break
                openai_subtypes[subtype] = openai_subtypes.get(subtype, 0) + 1
            elif "telegram" in message:
                category = "Telegram"
            elif "binance" in message or "context" in message:
                category = "Binance/данные"
            else:
                category = "другие"
            error_categories[category] = error_categories.get(category, 0) + 1
        errors_text = ", ".join(
            f"{name} {count}"
            for name, count in sorted(
                error_categories.items(), key=lambda item: -item[1]
            )
        )
        openai_text = ", ".join(
            f"{name} {count}" for name, count in sorted(
                openai_subtypes.items(), key=lambda item: -item[1]
            )
        )
        return (
            "👁 Наблюдатель AI-бота\n"
            f"Период: {hours:.1f} ч.\n"
            f"Решения AI: {decisions_text}.\n"
            f"Отклонено до покупки: {len(rejection_rows)}.\n"
            f"Основные причины: {filters_text}.\n"
            f"Технические ошибки: {error_count}"
            + (f" ({errors_text})." if errors_text else ".")
            + (f"\nОшибки OpenAI: {openai_text}." if openai_text else "")
        )

    def build_symbol_behavior(
        self,
        symbol: str,
        now: float,
        setup_threshold_percent: float,
        first_target_percent: float,
        second_target_percent: float,
        stop_loss_percent: float,
        lookback_seconds: int = 86400,
        window_seconds: int = 300,
        horizon_seconds: int = 900,
        event_cooldown_seconds: int = 1800,
        limit: int = 4,
    ) -> SymbolBehavior:
        shadow_rows = self.connection.execute(
            "SELECT started_at, trigger_price, immediate_success, "
            "immediate_stopped_first, immediate_max_return_percent, "
            "immediate_min_return_percent FROM confirmation_events "
            "WHERE symbol=? AND evaluated_at IS NOT NULL "
            "AND started_at>=? AND started_at<=? "
            "ORDER BY started_at DESC LIMIT ?",
            (symbol, now - lookback_seconds, now - horizon_seconds, limit),
        ).fetchall()
        if shadow_rows:
            impulses = []
            for row in reversed(shadow_rows):
                maximum = float(row[4])
                impulses.append(HistoricalImpulse(
                    float(row[0]), setup_threshold_percent, maximum,
                    float(row[5]), bool(row[2]),
                    bool(row[2]) and maximum >= second_target_percent,
                    bool(row[3]),
                ))
            return SymbolBehavior(symbol, tuple(impulses))
        points = [
            (float(row[0]), float(row[1]))
            for row in self.connection.execute(
                "SELECT timestamp, price FROM samples WHERE symbol = ? "
                "AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                (symbol, now - lookback_seconds, now),
            ).fetchall()
        ]
        rows = self.connection.execute(
            "SELECT timestamp, entry_price, change_percent, ai_score, "
            "ai_decision, ai_verdict, ai_reason, ai_risk, "
            "volume_ratio_5m, taker_buy_ratio_percent, "
            "order_book_imbalance_percent, change_60s_percent, "
            "pullback_from_high_percent FROM signal_events "
            "WHERE symbol = ? AND analysis_version >= 2 "
            "AND ai_score IS NOT NULL AND ai_decision IS NOT NULL "
            "AND timestamp >= ? AND timestamp <= ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (symbol, now - lookback_seconds, now - horizon_seconds, limit),
        ).fetchall()
        completed = list(reversed(rows))
        impulses: list[HistoricalImpulse] = []
        for row in completed:
            event_at = float(row[0])
            entry_price = float(row[1])
            setup_change = float(row[2])
            future_points = [
                (timestamp, price) for timestamp, price in points
                if event_at <= timestamp <= event_at + horizon_seconds
            ]
            if (
                not future_points
                or future_points[-1][0] < event_at + horizon_seconds * 0.8
            ):
                continue
            future = [price for _timestamp, price in future_points]
            changes = [(price / entry_price - 1) * 100 for price in future]
            first_hit = False
            second_hit = False
            stopped_before_first = False
            for change in changes:
                if not first_hit and change <= -stop_loss_percent:
                    stopped_before_first = True
                    break
                if change >= first_target_percent:
                    first_hit = True
                if first_hit and change >= second_target_percent:
                    second_hit = True
                    break
            impulses.append(
                HistoricalImpulse(
                    event_at,
                    setup_change,
                    max(changes),
                    min(changes),
                    first_hit,
                    second_hit,
                    stopped_before_first,
                    int(row[3]) if row[3] is not None else None,
                    str(row[4]) if row[4] is not None else None,
                    str(row[5]) if row[5] is not None else None,
                    str(row[6]) if row[6] is not None else None,
                    str(row[7]) if row[7] is not None else None,
                    float(row[8]) if row[8] is not None else None,
                    float(row[9]) if row[9] is not None else None,
                    float(row[10]) if row[10] is not None else None,
                    float(row[11]) if row[11] is not None else None,
                    float(row[12]) if row[12] is not None else None,
                )
            )
        return SymbolBehavior(symbol, tuple(impulses))

    def build_signal_performance(self, now: float) -> SignalPerformance:
        started = self.period_started_at()
        signal_count = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM signal_events "
                "WHERE timestamp >= ? AND timestamp < ?",
                (started, now),
            ).fetchone()[0]
        )
        evaluated: dict[int, int] = {}
        positive_rate: dict[int, float] = {}
        average_net_return: dict[int, float] = {}
        for minutes in (15, 30, 60):
            values = [
                float(row[0])
                for row in self.connection.execute(
                    "SELECT o.net_return_percent FROM signal_outcomes o "
                    "JOIN signal_events s ON s.id = o.signal_id "
                    "WHERE s.timestamp >= ? AND s.timestamp < ? "
                    "AND o.horizon_minutes = ?",
                    (started, now, minutes),
                ).fetchall()
            ]
            evaluated[minutes] = len(values)
            if values:
                positive_rate[minutes] = (
                    sum(value > 0 for value in values) / len(values) * 100
                )
                average_net_return[minutes] = sum(values) / len(values)
        return SignalPerformance(
            signal_count,
            evaluated,
            positive_rate,
            average_net_return,
        )

    def record_error(self, message: str, timestamp: float | None = None) -> None:
        self.connection.execute(
            "INSERT INTO errors(timestamp, message) VALUES(?, ?)",
            (time.time() if timestamp is None else timestamp, message[:1000]),
        )
        self.connection.commit()

    def build_summary(
        self,
        now: float,
        symbols: tuple[str, ...],
        events_by_symbol: dict[str, list[float]],
        poll_interval_seconds: int,
        threshold_percent: float,
    ) -> AuditSummary:
        started = self.period_started_at()
        alert_rows = self.connection.execute(
            "SELECT timestamp, symbol, delivered FROM alerts "
            "WHERE timestamp >= ? AND timestamp < ?",
            (started, now),
        ).fetchall()
        delivered = [(float(ts), str(symbol)) for ts, symbol, ok in alert_rows if ok]
        failed = sum(1 for _ts, _symbol, ok in alert_rows if not ok)
        expected = sum(len(events) for events in events_by_symbol.values())
        matched = 0
        for symbol, events in events_by_symbol.items():
            for event in events:
                if any(
                    alert_symbol == symbol and event - 60 <= alert_time <= event + 300
                    for alert_time, alert_symbol in delivered
                ):
                    matched += 1
        samples = self.connection.execute(
            "SELECT COUNT(*) FROM samples WHERE timestamp >= ? AND timestamp < ?",
            (started, now),
        ).fetchone()[0]
        expected_samples = max(
            1, ((now - started) / poll_interval_seconds) * len(symbols)
        )
        errors = self.connection.execute(
            "SELECT COUNT(*) FROM errors WHERE timestamp >= ? AND timestamp < ?",
            (started, now),
        ).fetchone()[0]
        return AuditSummary(
            started_at=started,
            finished_at=now,
            expected_pumps=expected,
            delivered_alerts=len(delivered),
            missed_pumps=max(0, expected - matched),
            failed_alerts=failed,
            errors=int(errors),
            coverage_percent=min(100.0, samples / expected_samples * 100),
            threshold_percent=threshold_percent,
        )

    def finish_period(self, now: float) -> None:
        self._set_metadata("period_started_at", str(now))
        cutoff = now - 8 * 86400
        self.connection.execute("DELETE FROM samples WHERE timestamp < ?", (cutoff,))
        self.connection.execute("DELETE FROM alerts WHERE timestamp < ?", (cutoff,))
        self.connection.execute("DELETE FROM errors WHERE timestamp < ?", (cutoff,))
        self.connection.execute(
            "DELETE FROM signal_outcomes WHERE signal_id IN "
            "(SELECT id FROM signal_events WHERE timestamp < ?)",
            (cutoff,),
        )
        self.connection.execute(
            "DELETE FROM signal_events WHERE timestamp < ?", (cutoff,)
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
