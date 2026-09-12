from dataclasses import dataclass
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
    ) -> int:
        cursor = self.connection.execute(
            "INSERT INTO signal_events("
            "timestamp, symbol, entry_price, signal_kind, change_percent, "
            "change_24h_percent, quote_volume_usdt, ai_score, ai_verdict, "
            "quote_volume_5m_usdt, volume_ratio_5m, trades_5m, "
            "taker_buy_ratio_percent, spread_bps, bid_depth_usdt, ask_depth_usdt, "
            "order_book_imbalance_percent) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
