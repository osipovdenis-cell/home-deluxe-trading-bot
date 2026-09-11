from dataclasses import dataclass
from pathlib import Path
import sqlite3


@dataclass(frozen=True)
class TradeNotice:
    action: str
    symbol: str
    price: float
    quantity: float
    position_usdt: float
    reason: str
    pnl_usdt: float = 0.0
    pnl_percent: float = 0.0
    remaining_percent: float = 100.0
    ai_score: int | None = None
    total_position_pnl_usdt: float | None = None

    def telegram_text(self) -> str:
        if self.action == "BUY":
            return (
                f"🧪 Тестовый вход {self.symbol}\n"
                f"Сумма: {self.position_usdt:.2f} USDT.\n"
                f"Цена входа: {self.price:.10g}.\n"
                f"ИИ-оценка: {self.ai_score}/100.\n"
                "Это виртуальная сделка: заявка на биржу не отправлена."
            )
        text = (
            f"🧪 Тестовый выход {self.symbol}\n"
            f"Причина: {self.reason}.\n"
            f"Цена: {self.price:.10g}.\n"
            f"Результат проданной части: {self.pnl_percent:+.2f}% "
            f"({self.pnl_usdt:+.3f} USDT).\n"
            f"Остаток позиции: {self.remaining_percent:.0f}%."
        )
        if self.total_position_pnl_usdt is not None:
            text += (
                f"\nПозиция закрыта. Общий результат: "
                f"{self.total_position_pnl_usdt:+.3f} USDT."
            )
        return text


@dataclass(frozen=True)
class PaperTradingSummary:
    closed_positions: int
    profitable_positions: int
    realized_pnl_usdt: float

    def telegram_text(self) -> str:
        if not self.closed_positions:
            return "🧪 Тестовые сделки: закрытых позиций пока нет."
        win_rate = self.profitable_positions / self.closed_positions * 100
        return (
            "🧪 Результат тестовой стратегии\n"
            f"Закрыто позиций: {self.closed_positions}.\n"
            f"Прибыльных: {win_rate:.1f}%.\n"
            f"Общий результат: {self.realized_pnl_usdt:+.3f} USDT."
        )


class PaperTrader:
    def __init__(
        self,
        database_path: str,
        position_usdt: float,
        max_open_positions: int,
        min_ai_score: int,
        stop_loss_percent: float,
        take_profit_1_percent: float,
        take_profit_2_percent: float,
        trailing_drawdown_percent: float,
        max_hold_seconds: int,
        round_trip_cost_percent: float,
    ) -> None:
        database = Path(database_path)
        database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.position_usdt = position_usdt
        self.max_open_positions = max_open_positions
        self.min_ai_score = min_ai_score
        self.stop_loss_percent = stop_loss_percent
        self.take_profit_1_percent = take_profit_1_percent
        self.take_profit_2_percent = take_profit_2_percent
        self.trailing_drawdown_percent = trailing_drawdown_percent
        self.max_hold_seconds = max_hold_seconds
        self.round_trip_cost_percent = round_trip_cost_percent
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                opened_at REAL NOT NULL,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                highest_price REAL NOT NULL,
                initial_quantity REAL NOT NULL,
                remaining_quantity REAL NOT NULL,
                position_usdt REAL NOT NULL,
                realized_pnl_usdt REAL NOT NULL DEFAULT 0,
                take_1_done INTEGER NOT NULL DEFAULT 0,
                take_2_done INTEGER NOT NULL DEFAULT 0,
                ai_score INTEGER NOT NULL,
                signal_kind TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                closed_at REAL,
                close_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS paper_positions_status
                ON paper_positions(status);
            CREATE TABLE IF NOT EXISTS paper_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                pnl_usdt REAL NOT NULL,
                reason TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def open_on_signal(
        self,
        symbol: str,
        price: float,
        signal_kind: str,
        ai_score: int | None,
        now: float,
    ) -> TradeNotice | None:
        if ai_score is None or ai_score < self.min_ai_score:
            return None
        exists = self.connection.execute(
            "SELECT 1 FROM paper_positions WHERE symbol = ? AND status = 'OPEN'",
            (symbol,),
        ).fetchone()
        if exists is not None:
            return None
        active_count = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM paper_positions WHERE status = 'OPEN'"
            ).fetchone()[0]
        )
        if active_count >= self.max_open_positions:
            return None
        quantity = self.position_usdt / price
        cursor = self.connection.execute(
            "INSERT INTO paper_positions("
            "opened_at, symbol, entry_price, highest_price, initial_quantity, "
            "remaining_quantity, position_usdt, ai_score, signal_kind"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                symbol,
                price,
                price,
                quantity,
                quantity,
                self.position_usdt,
                ai_score,
                signal_kind,
            ),
        )
        self.connection.execute(
            "INSERT INTO paper_fills("
            "position_id, timestamp, side, price, quantity, pnl_usdt, reason"
            ") VALUES(?, ?, 'BUY', ?, ?, 0, 'сигнал')",
            (cursor.lastrowid, now, price, quantity),
        )
        self.connection.commit()
        return TradeNotice(
            "BUY",
            symbol,
            price,
            quantity,
            self.position_usdt,
            "сигнал",
            ai_score=ai_score,
        )

    def _sell(
        self,
        row: sqlite3.Row,
        quantity: float,
        price: float,
        now: float,
        reason: str,
        take_column: str | None = None,
    ) -> TradeNotice:
        quantity = min(quantity, float(row["remaining_quantity"]))
        entry_price = float(row["entry_price"])
        initial_quantity = float(row["initial_quantity"])
        pnl_percent = (
            (price / entry_price - 1) * 100 - self.round_trip_cost_percent
        )
        pnl_usdt = entry_price * quantity * pnl_percent / 100
        remaining = max(0.0, float(row["remaining_quantity"]) - quantity)
        realized = float(row["realized_pnl_usdt"]) + pnl_usdt
        closed = remaining <= initial_quantity * 1e-9
        assignments = ["remaining_quantity = ?", "realized_pnl_usdt = ?"]
        values: list[object] = [remaining, realized]
        if take_column is not None:
            assignments.append(f"{take_column} = 1")
        if closed:
            assignments.extend(
                ["status = 'CLOSED'", "closed_at = ?", "close_reason = ?"]
            )
            values.extend([now, reason])
        values.append(int(row["id"]))
        self.connection.execute(
            f"UPDATE paper_positions SET {', '.join(assignments)} WHERE id = ?",
            values,
        )
        self.connection.execute(
            "INSERT INTO paper_fills("
            "position_id, timestamp, side, price, quantity, pnl_usdt, reason"
            ") VALUES(?, ?, 'SELL', ?, ?, ?, ?)",
            (row["id"], now, price, quantity, pnl_usdt, reason),
        )
        self.connection.commit()
        return TradeNotice(
            "SELL",
            str(row["symbol"]),
            price,
            quantity,
            entry_price * quantity,
            reason,
            pnl_usdt,
            pnl_percent,
            remaining / initial_quantity * 100,
            total_position_pnl_usdt=realized if closed else None,
        )

    def update_positions(
        self, prices: dict[str, float], now: float
    ) -> list[TradeNotice]:
        notices: list[TradeNotice] = []
        rows = self.connection.execute(
            "SELECT * FROM paper_positions WHERE status = 'OPEN' ORDER BY id"
        ).fetchall()
        for initial_row in rows:
            symbol = str(initial_row["symbol"])
            price = prices.get(symbol)
            if price is None:
                continue
            highest = max(float(initial_row["highest_price"]), price)
            self.connection.execute(
                "UPDATE paper_positions SET highest_price = ? WHERE id = ?",
                (highest, initial_row["id"]),
            )
            self.connection.commit()
            row = self.connection.execute(
                "SELECT * FROM paper_positions WHERE id = ?", (initial_row["id"],)
            ).fetchone()
            change = (price / float(row["entry_price"]) - 1) * 100
            if change <= -self.stop_loss_percent:
                notices.append(
                    self._sell(
                        row,
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "стоп-лосс",
                    )
                )
                continue
            if (
                not int(row["take_1_done"])
                and change + 1e-9 >= self.take_profit_1_percent
            ):
                notices.append(
                    self._sell(
                        row,
                        float(row["initial_quantity"]) * 0.4,
                        price,
                        now,
                        "фиксация +1,5%",
                        "take_1_done",
                    )
                )
                row = self.connection.execute(
                    "SELECT * FROM paper_positions WHERE id = ?", (row["id"],)
                ).fetchone()
            if (
                not int(row["take_2_done"])
                and change + 1e-9 >= self.take_profit_2_percent
            ):
                notices.append(
                    self._sell(
                        row,
                        float(row["initial_quantity"]) * 0.4,
                        price,
                        now,
                        "фиксация +3%",
                        "take_2_done",
                    )
                )
                row = self.connection.execute(
                    "SELECT * FROM paper_positions WHERE id = ?", (row["id"],)
                ).fetchone()
            if row["status"] != "OPEN":
                continue
            drawdown = (price / highest - 1) * 100
            if int(row["take_1_done"]) and drawdown <= -self.trailing_drawdown_percent:
                notices.append(
                    self._sell(
                        row,
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "откат от максимума",
                    )
                )
                continue
            if now - float(row["opened_at"]) >= self.max_hold_seconds:
                notices.append(
                    self._sell(
                        row,
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "лимит времени",
                    )
                )
        return notices

    def summary_since(self, started_at: float, now: float) -> PaperTradingSummary:
        rows = self.connection.execute(
            "SELECT realized_pnl_usdt FROM paper_positions "
            "WHERE status = 'CLOSED' AND closed_at >= ? AND closed_at < ?",
            (started_at, now),
        ).fetchall()
        values = [float(row[0]) for row in rows]
        return PaperTradingSummary(
            len(values),
            sum(value > 0 for value in values),
            sum(values),
        )

    def close(self) -> None:
        self.connection.close()
