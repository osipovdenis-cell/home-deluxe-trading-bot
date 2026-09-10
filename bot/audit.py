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
            f"Сигналов доставлено: {self.delivered_alerts}.\n"
            f"Предположительно пропущено: {self.missed_pumps}.\n"
            f"Ошибок отправки: {self.failed_alerts}.\n"
            f"Ошибок получения данных: {self.errors}.\n"
            f"Полнота наблюдений: {self.coverage_percent:.1f}%."
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
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        if self._metadata("period_started_at") is None:
            self._set_metadata("period_started_at", str(time.time()))
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
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
