from collections import deque
from dataclasses import dataclass
import math
from pathlib import Path
import sqlite3
import time


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
            (row[4], current.get("volume_ratio_5m"), 0.8),
            (row[5], current.get("taker_buy_ratio_percent"), 8.0),
            (row[6], current.get("order_book_imbalance_percent"), 30.0),
            (row[7], current.get("change_60s_percent"), 0.35),
            (row[8], current.get("pullback_from_high_percent"), 0.25),
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
    ) -> LearningProfile:
        rows = self.connection.execute(
            "SELECT symbol, success_before_stop, signal_timestamp, "
            "setup_change_percent, volume_ratio_5m, taker_buy_ratio_percent, "
            "order_book_imbalance_percent, change_60s_percent, "
            "pullback_from_high_percent FROM learning_examples "
            "WHERE signal_timestamp >= ? ORDER BY signal_timestamp DESC LIMIT 2000",
            (now - lookback_seconds,),
        ).fetchall()
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
        rows = self.connection.execute(
            "SELECT symbol, COUNT(*), SUM(success_before_stop) "
            "FROM learning_examples WHERE signal_timestamp >= ? "
            "GROUP BY symbol",
            (now - 30 * 86400,),
        ).fetchall()
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
