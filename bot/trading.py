from dataclasses import dataclass
from pathlib import Path
import sqlite3
import time


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
    starting_balance_usdt: float
    period_start_equity_usdt: float
    cash_balance_usdt: float
    equity_usdt: float
    period_hours: float
    open_positions: int
    closed_positions: int
    profitable_positions: int

    def telegram_text(self) -> str:
        result = self.equity_usdt - self.starting_balance_usdt
        result_percent = result / self.starting_balance_usdt * 100
        period_result = self.equity_usdt - self.period_start_equity_usdt
        period_percent = period_result / self.period_start_equity_usdt * 100
        win_rate = (
            self.profitable_positions / self.closed_positions * 100
            if self.closed_positions
            else 0.0
        )
        return (
            "🧪 Виртуальный торговый банк\n"
            f"Старт: {self.starting_balance_usdt:.2f} USDT.\n"
            f"Сейчас: {self.equity_usdt:.3f} USDT.\n"
            f"За {self.period_hours:.1f} ч: {period_result:+.3f} USDT "
            f"({period_percent:+.2f}%).\n"
            f"Всего от старта: {result:+.3f} USDT ({result_percent:+.2f}%).\n"
            f"Свободно: {self.cash_balance_usdt:.3f} USDT.\n"
            f"Открыто позиций: {self.open_positions}.\n"
            f"Закрыто позиций: {self.closed_positions}.\n"
            f"Прибыльных среди закрытых: {win_rate:.1f}%."
        )


@dataclass(frozen=True)
class EntryBucketSummary:
    label: str
    trades: int
    profitable: int
    pnl_usdt: float
    first_target_hits: int
    second_target_hits: int


@dataclass(frozen=True)
class TradeBreakdown:
    symbol: str
    entry_change_percent: float | None
    ai_score: int
    maximum_percent: float
    pnl_usdt: float
    return_percent: float
    first_target_hit: bool
    second_target_hit: bool
    close_reason: str


@dataclass(frozen=True)
class TradingIntelligenceSummary:
    closed_positions: int
    actual_pnl_usdt: float
    profitable_rate_percent: float
    profit_factor: float | None
    average_mfe_percent: float
    average_mae_percent: float
    average_giveback_percent: float
    target_0_7_pnl_usdt: float
    target_1_pnl_usdt: float
    target_1_5_pnl_usdt: float
    entry_buckets: tuple[EntryBucketSummary, ...] = ()
    trade_breakdown: tuple[TradeBreakdown, ...] = ()

    def as_dict(self) -> dict:
        return {
            "closed_positions": self.closed_positions,
            "actual_strategy_pnl_usdt": round(self.actual_pnl_usdt, 4),
            "profitable_rate_percent": round(self.profitable_rate_percent, 2),
            "profit_factor": (
                None if self.profit_factor is None else round(self.profit_factor, 3)
            ),
            "average_maximum_favorable_excursion_percent": round(
                self.average_mfe_percent, 3
            ),
            "average_maximum_adverse_excursion_percent": round(
                self.average_mae_percent, 3
            ),
            "average_profit_given_back_percent": round(
                self.average_giveback_percent, 3
            ),
            "control_strategies": {
                "full_exit_at_0_7_percent_pnl_usdt": round(
                    self.target_0_7_pnl_usdt, 4
                ),
                "full_exit_at_1_percent_pnl_usdt": round(
                    self.target_1_pnl_usdt, 4
                ),
                "full_exit_at_1_5_percent_pnl_usdt": round(
                    self.target_1_5_pnl_usdt, 4
                ),
            },
            "entry_buckets": [
                {
                    "entry_change_range": bucket.label,
                    "trades": bucket.trades,
                    "profitable_rate_percent": round(
                        bucket.profitable / bucket.trades * 100, 2
                    ) if bucket.trades else 0.0,
                    "pnl_usdt": round(bucket.pnl_usdt, 4),
                    "first_target_hits": bucket.first_target_hits,
                    "second_target_hits": bucket.second_target_hits,
                }
                for bucket in self.entry_buckets
            ],
        }

    def telegram_text(self) -> str:
        if not self.closed_positions:
            return "🧠 Разбор сделок: закрытые позиции пока не накоплены."
        factor = "нет убытков" if self.profit_factor is None else f"{self.profit_factor:.2f}"
        variants = {
            "наша 50/50": self.actual_pnl_usdt,
            "всё на +0,7%": self.target_0_7_pnl_usdt,
            "всё на +1%": self.target_1_pnl_usdt,
            "всё на +1,5%": self.target_1_5_pnl_usdt,
        }
        winner = max(variants, key=variants.get)
        bucket_lines = []
        for bucket in self.entry_buckets:
            win_rate = (
                bucket.profitable / bucket.trades * 100 if bucket.trades else 0.0
            )
            bucket_lines.append(
                f"• {bucket.label}: {bucket.trades} сделок, в плюсе "
                f"{win_rate:.1f}%, PnL {bucket.pnl_usdt:+.3f}; "
                f"цели 0,7/1%: {bucket.first_target_hits}/{bucket.second_target_hits}."
            )
        return (
            "🧠 Расширенный разбор сделок\n"
            f"Закрыто: {self.closed_positions}, прибыльных: "
            f"{self.profitable_rate_percent:.1f}%.\n"
            f"Фактический результат: {self.actual_pnl_usdt:+.3f} USDT.\n"
            f"Profit factor: {factor}.\n"
            f"Средний максимум после входа: {self.average_mfe_percent:+.2f}%.\n"
            f"Средняя максимальная просадка: {self.average_mae_percent:+.2f}%.\n"
            f"Средняя отданная часть движения: "
            f"{self.average_giveback_percent:.2f} п.п.\n"
            "Параллельный пересчёт на тех же сигналах:\n"
            f"• наша 50/50: {self.actual_pnl_usdt:+.3f} USDT;\n"
            f"• всё на +0,7%: {self.target_0_7_pnl_usdt:+.3f} USDT;\n"
            f"• всё на +1%: {self.target_1_pnl_usdt:+.3f} USDT;\n"
            f"• всё на +1,5%: {self.target_1_5_pnl_usdt:+.3f} USDT.\n"
            f"Лучший вариант за период: {winner}.\n"
            "\nВходы по росту за 5 минут:\n"
            + "\n".join(bucket_lines)
        )

    def trade_breakdown_texts(self, max_length: int = 3500) -> list[str]:
        if not self.trade_breakdown:
            return []
        lines = ["📋 Сделки по точке входа"]
        for trade in self.trade_breakdown:
            entry = (
                "нет данных"
                if trade.entry_change_percent is None
                else f"{trade.entry_change_percent:+.2f}%"
            )
            levels = (
                f"0,7% {'✅' if trade.first_target_hit else '—'} / "
                f"1% {'✅' if trade.second_target_hit else '—'}"
            )
            lines.append(
                f"{trade.symbol}: вход {entry}, ИИ {trade.ai_score}, "
                f"макс {trade.maximum_percent:+.2f}%, итог "
                f"{trade.return_percent:+.2f}% ({trade.pnl_usdt:+.3f}), "
                f"{levels}, {trade.close_reason}."
            )
        chunks: list[str] = []
        current = lines[0]
        for line in lines[1:]:
            candidate = current + "\n" + line
            if len(candidate) > max_length:
                chunks.append(current)
                current = lines[0] + " (продолжение)\n" + line
            else:
                current = candidate
        chunks.append(current)
        return chunks


class PaperTrader:
    def __init__(
        self,
        database_path: str,
        starting_balance_usdt: float,
        position_usdt: float,
        max_open_positions: int,
        min_ai_score: int,
        stop_loss_percent: float,
        take_profit_1_percent: float,
        take_profit_2_percent: float,
        take_profit_3_percent: float,
        trailing_drawdown_percent: float,
        max_hold_seconds: int,
        round_trip_cost_percent: float,
        stagnation_after_seconds: int = 900,
        stagnation_window_seconds: int = 300,
    ) -> None:
        database = Path(database_path)
        database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.starting_balance_usdt = starting_balance_usdt
        self.position_usdt = position_usdt
        self.max_open_positions = max_open_positions
        self.min_ai_score = min_ai_score
        self.stop_loss_percent = stop_loss_percent
        self.take_profit_1_percent = take_profit_1_percent
        self.take_profit_2_percent = take_profit_2_percent
        self.take_profit_3_percent = take_profit_3_percent
        self.trailing_drawdown_percent = trailing_drawdown_percent
        self.max_hold_seconds = max_hold_seconds
        self.round_trip_cost_percent = round_trip_cost_percent
        self.stagnation_after_seconds = stagnation_after_seconds
        self.stagnation_window_seconds = stagnation_window_seconds
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
            CREATE TABLE IF NOT EXISTS paper_account (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                starting_balance_usdt REAL NOT NULL,
                cash_balance_usdt REAL NOT NULL,
                report_started_at REAL NOT NULL,
                report_start_equity_usdt REAL NOT NULL
            );
            """
        )
        account_columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(paper_account)")
        }
        if "report_started_at" not in account_columns:
            self.connection.execute(
                "ALTER TABLE paper_account ADD COLUMN report_started_at REAL"
            )
        if "report_start_equity_usdt" not in account_columns:
            self.connection.execute(
                "ALTER TABLE paper_account ADD COLUMN report_start_equity_usdt REAL"
            )
        self.connection.execute(
            "INSERT OR IGNORE INTO paper_account("
            "id, starting_balance_usdt, cash_balance_usdt, report_started_at, "
            "report_start_equity_usdt) VALUES(1, ?, ?, ?, ?)",
            (
                starting_balance_usdt,
                starting_balance_usdt,
                time.time(),
                starting_balance_usdt,
            ),
        )
        self.connection.execute(
            "UPDATE paper_account SET "
            "report_started_at = COALESCE(report_started_at, ?), "
            "report_start_equity_usdt = COALESCE("
            "report_start_equity_usdt, starting_balance_usdt) WHERE id = 1",
            (time.time(),),
        )
        self.connection.commit()

    def open_on_signal(
        self,
        symbol: str,
        price: float,
        signal_kind: str,
        ai_score: int | None,
        now: float,
        bypass_min_score: bool = False,
    ) -> TradeNotice | None:
        if not bypass_min_score and (
            ai_score is None or ai_score < self.min_ai_score
        ):
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
        cash_balance = float(
            self.connection.execute(
                "SELECT cash_balance_usdt FROM paper_account WHERE id = 1"
            ).fetchone()[0]
        )
        remaining_slots = self.max_open_positions - active_count
        if cash_balance <= 1e-9 or remaining_slots <= 0:
            return None
        trade_usdt = cash_balance / remaining_slots
        quantity = trade_usdt / price
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
                trade_usdt,
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
        self.connection.execute(
            "UPDATE paper_account SET cash_balance_usdt = "
            "cash_balance_usdt - ? WHERE id = 1",
            (trade_usdt,),
        )
        self.connection.commit()
        return TradeNotice(
            "BUY",
            symbol,
            price,
            quantity,
            trade_usdt,
            "сигнал",
            ai_score=ai_score,
        )

    def notice_telegram_text(
        self,
        notice: TradeNotice,
        prices: dict[str, float],
        now: float,
    ) -> str:
        summary = self.summary(prices, now)
        result = summary.equity_usdt - summary.starting_balance_usdt
        invested = max(0.0, summary.equity_usdt - summary.cash_balance_usdt)
        return (
            f"{notice.telegram_text()}\n\n"
            "💰 Виртуальный банк\n"
            f"Стартовый капитал: {summary.starting_balance_usdt:.2f} USDT.\n"
            f"Текущий баланс: {summary.equity_usdt:.3f} USDT.\n"
            f"Прибыль/убыток: {result:+.3f} USDT.\n"
            f"Свободно: {summary.cash_balance_usdt:.3f} USDT.\n"
            f"В открытых позициях: {invested:.3f} USDT."
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
        self.connection.execute(
            "UPDATE paper_account SET cash_balance_usdt = "
            "cash_balance_usdt + ? WHERE id = 1",
            (entry_price * quantity + pnl_usdt,),
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

    def stagnation_candidates(self, now: float) -> tuple[str, ...]:
        rows = self.connection.execute(
            "SELECT symbol FROM paper_positions WHERE status = 'OPEN' "
            "AND ? - opened_at >= ? ORDER BY id",
            (now, self.stagnation_after_seconds),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def open_symbols(self) -> tuple[str, ...]:
        rows = self.connection.execute(
            "SELECT symbol FROM paper_positions WHERE status = 'OPEN' ORDER BY id"
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def _price_has_stagnated(self, row: sqlite3.Row, now: float) -> bool:
        samples_table = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'samples'"
        ).fetchone()
        if samples_table is None:
            return False
        cutoff = now - self.stagnation_window_seconds
        earlier_high = self.connection.execute(
            "SELECT MAX(price) FROM samples WHERE symbol = ? "
            "AND timestamp >= ? AND timestamp < ?",
            (row["symbol"], row["opened_at"], cutoff),
        ).fetchone()[0]
        recent_high = self.connection.execute(
            "SELECT MAX(price) FROM samples WHERE symbol = ? "
            "AND timestamp >= ? AND timestamp <= ?",
            (row["symbol"], cutoff, now),
        ).fetchone()[0]
        if earlier_high is None or recent_high is None:
            return False
        return float(recent_high) <= float(earlier_high) * (1 + 1e-9)

    def update_positions(
        self,
        prices: dict[str, float],
        now: float,
        market_contexts: dict[str, tuple[float, float, float | None]] | None = None,
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
            row = initial_row
            if price > float(initial_row["highest_price"]):
                self.connection.execute(
                    "UPDATE paper_positions SET highest_price = ? WHERE id = ?",
                    (price, initial_row["id"]),
                )
                self.connection.commit()
                row = self.connection.execute(
                    "SELECT * FROM paper_positions WHERE id = ?", (initial_row["id"],)
                ).fetchone()
            change = (price / float(row["entry_price"]) - 1) * 100
            is_rocket = "лидер" in str(row["signal_kind"])
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
            if is_rocket:
                highest_change = (
                    float(row["highest_price"]) / float(row["entry_price"]) - 1
                ) * 100
                if not int(row["take_1_done"]) and highest_change >= 1.0:
                    self.connection.execute(
                        "UPDATE paper_positions SET take_1_done=1 WHERE id=?",
                        (row["id"],),
                    )
                    self.connection.commit()
                    row = self.connection.execute(
                        "SELECT * FROM paper_positions WHERE id=?", (row["id"],)
                    ).fetchone()
                if int(row["take_1_done"]):
                    protection = max(1.0, highest_change - 1.0)
                    if change + 1e-9 < highest_change and change <= protection:
                        notices.append(
                            self._sell(
                                row, float(row["remaining_quantity"]), price, now,
                                f"ракета: откат 1 п.п. от максимума "
                                f"{highest_change:+.2f}%",
                            )
                        )
                # A leader keeps 100% of the position; ordinary staged takes and
                # the stagnation exit below do not apply.
                continue
            if (
                not int(row["take_1_done"])
                and change + 1e-9 >= self.take_profit_1_percent
            ):
                notices.append(
                    self._sell(
                        row,
                        float(row["initial_quantity"]) * 0.5,
                        price,
                        now,
                        "фиксация +0,7%",
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
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "фиксация +1%",
                        "take_2_done",
                    )
                )
                row = self.connection.execute(
                    "SELECT * FROM paper_positions WHERE id = ?", (row["id"],)
                ).fetchone()
            if row["status"] != "OPEN":
                continue
            if (
                int(row["take_1_done"])
                and change + 1e-9 < self.take_profit_1_percent
            ):
                notices.append(
                    self._sell(
                        row,
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "защита прибыли +0,7%",
                    )
                )
                continue
            net_change = change - self.round_trip_cost_percent
            if (
                now - float(row["opened_at"]) >= self.stagnation_after_seconds
                and net_change > 0
                and self._price_has_stagnated(row, now)
            ):
                notices.append(
                    self._sell(
                        row,
                        float(row["remaining_quantity"]),
                        price,
                        now,
                        "15 минут без роста (выход в плюс)",
                    )
                )
                continue
        return notices

    def report_due(self, now: float) -> bool:
        started_at = float(
            self.connection.execute(
                "SELECT report_started_at FROM paper_account WHERE id = 1"
            ).fetchone()[0]
        )
        return now - started_at >= 86400

    def summary(self, prices: dict[str, float], now: float) -> PaperTradingSummary:
        account = self.connection.execute(
            "SELECT starting_balance_usdt, cash_balance_usdt, "
            "report_started_at, report_start_equity_usdt "
            "FROM paper_account WHERE id = 1"
        ).fetchone()
        rows = self.connection.execute(
            "SELECT realized_pnl_usdt FROM paper_positions "
            "WHERE status = 'CLOSED' AND closed_at >= ? AND closed_at < ?",
            (account["report_started_at"], now),
        ).fetchall()
        values = [float(row[0]) for row in rows]
        open_rows = self.connection.execute(
            "SELECT symbol, entry_price, remaining_quantity "
            "FROM paper_positions WHERE status = 'OPEN'"
        ).fetchall()
        equity = float(account["cash_balance_usdt"])
        for row in open_rows:
            entry_price = float(row["entry_price"])
            quantity = float(row["remaining_quantity"])
            current_price = prices.get(str(row["symbol"]), entry_price)
            pnl_percent = (
                (current_price / entry_price - 1) * 100
                - self.round_trip_cost_percent
            )
            entry_value = entry_price * quantity
            equity += entry_value * (1 + pnl_percent / 100)
        return PaperTradingSummary(
            float(account["starting_balance_usdt"]),
            float(account["report_start_equity_usdt"]),
            float(account["cash_balance_usdt"]),
            equity,
            (now - float(account["report_started_at"])) / 3600,
            len(open_rows),
            len(values),
            sum(value > 0 for value in values),
        )

    def rocket_report_text(
        self,
        prices: dict[str, float],
        now: float,
        since: float | None = None,
    ) -> str:
        if since is None:
            since = float(self.connection.execute(
                "SELECT report_started_at FROM paper_account WHERE id = 1"
            ).fetchone()[0])
        rows = self.connection.execute(
            "SELECT * FROM paper_positions WHERE opened_at >= ? "
            "AND opened_at < ? AND signal_kind LIKE '%лидер%' ORDER BY opened_at",
            (since, now),
        ).fetchall()
        if not rows:
            return "🚀 Сделки по ракетам: входов за период пока нет."
        closed = [row for row in rows if row["status"] == "CLOSED"]
        opened = [row for row in rows if row["status"] == "OPEN"]
        protected = sum(bool(row["take_1_done"]) for row in rows)
        without_ai = sum(int(row["ai_score"]) == 0 for row in rows)
        trailing = sum("ракета:" in str(row["close_reason"] or "") for row in closed)
        stops = sum(str(row["close_reason"] or "") == "стоп-лосс" for row in closed)
        realized = sum(float(row["realized_pnl_usdt"]) for row in closed)
        unrealized = 0.0
        mfe: list[float] = []
        for row in rows:
            entry = float(row["entry_price"])
            mfe.append((float(row["highest_price"]) / entry - 1) * 100)
            if row["status"] == "OPEN":
                current = prices.get(str(row["symbol"]), entry)
                change = (current / entry - 1) * 100 - self.round_trip_cost_percent
                unrealized += float(row["position_usdt"]) * change / 100
        return (
            "🚀 Сделки по ракетам\n"
            f"Входов: {len(rows)}; закрыто {len(closed)}, открыто {len(opened)}.\n"
            f"Резервных входов без ответа AI: {without_ai}.\n"
            f"Защита +1% включалась: {protected}; выходов по откату: {trailing}; "
            f"стопов: {stops}.\n"
            f"Реализованный результат: {realized:+.3f} USDT; "
            f"открытый: {unrealized:+.3f} USDT.\n"
            f"Максимальный рост после входа: {max(mfe):.2f}%; "
            f"средний: {sum(mfe) / len(mfe):.2f}%."
        )

    def _control_strategy_pnl(
        self,
        entry_price: float,
        position_usdt: float,
        observed_prices: list[float],
        target_percent: float,
    ) -> float:
        exit_change = (observed_prices[-1] / entry_price - 1) * 100
        for price in observed_prices:
            change = (price / entry_price - 1) * 100
            if change <= -self.stop_loss_percent or change >= target_percent:
                exit_change = change
                break
        net_change = exit_change - self.round_trip_cost_percent
        return position_usdt * net_change / 100

    def build_intelligence(self, now: float) -> TradingIntelligenceSummary:
        started_at = float(
            self.connection.execute(
                "SELECT report_started_at FROM paper_account WHERE id = 1"
            ).fetchone()[0]
        )
        signal_events_table = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'signal_events'"
        ).fetchone()
        entry_change_sql = (
            "(SELECT change_percent FROM signal_events "
            "WHERE signal_events.symbol = paper_positions.symbol "
            "AND signal_events.timestamp = paper_positions.opened_at "
            "ORDER BY signal_events.id DESC LIMIT 1) AS entry_change_percent "
            if signal_events_table is not None
            else "NULL AS entry_change_percent "
        )
        positions = self.connection.execute(
            "SELECT id, opened_at, closed_at, symbol, entry_price, highest_price, "
            "position_usdt, realized_pnl_usdt, ai_score, take_1_done, take_2_done, "
            "close_reason, " + entry_change_sql + "FROM paper_positions "
            "WHERE status = 'CLOSED' AND closed_at >= ? AND closed_at < ? "
            "ORDER BY closed_at",
            (started_at, now),
        ).fetchall()
        if not positions:
            return TradingIntelligenceSummary(0, 0, 0, None, 0, 0, 0, 0, 0, 0)
        samples_table = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'samples'"
        ).fetchone()
        actual_values: list[float] = []
        mfe_values: list[float] = []
        mae_values: list[float] = []
        giveback_values: list[float] = []
        target_0_7_pnl = 0.0
        target_1_pnl = 0.0
        target_1_5_pnl = 0.0
        breakdown: list[TradeBreakdown] = []
        for row in positions:
            entry_price = float(row["entry_price"])
            position_usdt = float(row["position_usdt"])
            close_fill = self.connection.execute(
                "SELECT price FROM paper_fills WHERE position_id = ? "
                "AND side = 'SELL' ORDER BY timestamp DESC, id DESC LIMIT 1",
                (row["id"],),
            ).fetchone()
            close_price = entry_price if close_fill is None else float(close_fill[0])
            observed_prices = [entry_price]
            if samples_table is not None:
                observed_prices.extend(
                    float(sample[0])
                    for sample in self.connection.execute(
                        "SELECT price FROM samples WHERE symbol = ? "
                        "AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                        (row["symbol"], row["opened_at"], row["closed_at"]),
                    ).fetchall()
                )
            observed_prices.append(close_price)
            changes = [(price / entry_price - 1) * 100 for price in observed_prices]
            recorded_high_change = (
                float(row["highest_price"]) / entry_price - 1
            ) * 100
            mfe = max(max(changes), recorded_high_change)
            mae = min(changes)
            actual_pnl = float(row["realized_pnl_usdt"])
            actual_return = actual_pnl / position_usdt * 100
            actual_values.append(actual_pnl)
            mfe_values.append(mfe)
            mae_values.append(mae)
            giveback_values.append(max(0.0, mfe - actual_return))
            breakdown.append(
                TradeBreakdown(
                    str(row["symbol"]),
                    None if row["entry_change_percent"] is None else float(
                        row["entry_change_percent"]
                    ),
                    int(row["ai_score"]),
                    mfe,
                    actual_pnl,
                    actual_return,
                    bool(row["take_1_done"]),
                    bool(row["take_2_done"]),
                    str(row["close_reason"] or "закрыта"),
                )
            )
            target_0_7_pnl += self._control_strategy_pnl(
                entry_price, position_usdt, observed_prices, 0.7
            )
            target_1_pnl += self._control_strategy_pnl(
                entry_price, position_usdt, observed_prices, 1.0
            )
            target_1_5_pnl += self._control_strategy_pnl(
                entry_price, position_usdt, observed_prices, 1.5
            )
        gains = sum(value for value in actual_values if value > 0)
        losses = abs(sum(value for value in actual_values if value < 0))
        profit_factor = gains / losses if losses > 0 else None
        bucket_specs = (
            ("1–1,49%", 1.0, 1.5),
            ("1,5–1,99%", 1.5, 2.0),
            ("2–2,99%", 2.0, 3.0),
            ("от 3%", 3.0, float("inf")),
            ("прочие/нет данных", float("-inf"), float("inf")),
        )
        buckets: list[EntryBucketSummary] = []
        assigned: set[int] = set()
        for label, lower, upper in bucket_specs:
            selected: list[TradeBreakdown] = []
            for index, trade in enumerate(breakdown):
                if index in assigned:
                    continue
                change = trade.entry_change_percent
                matches = (
                    label == "прочие/нет данных"
                    or (change is not None and lower <= change < upper)
                )
                if matches:
                    assigned.add(index)
                    selected.append(trade)
            if selected:
                buckets.append(
                    EntryBucketSummary(
                        label,
                        len(selected),
                        sum(trade.pnl_usdt > 0 for trade in selected),
                        sum(trade.pnl_usdt for trade in selected),
                        sum(trade.first_target_hit for trade in selected),
                        sum(trade.second_target_hit for trade in selected),
                    )
                )
        return TradingIntelligenceSummary(
            len(positions),
            sum(actual_values),
            sum(value > 0 for value in actual_values) / len(actual_values) * 100,
            profit_factor,
            sum(mfe_values) / len(mfe_values),
            sum(mae_values) / len(mae_values),
            sum(giveback_values) / len(giveback_values),
            target_0_7_pnl,
            target_1_pnl,
            target_1_5_pnl,
            tuple(buckets),
            tuple(breakdown),
        )

    def finish_report(self, prices: dict[str, float], now: float) -> None:
        equity = self.summary(prices, now).equity_usdt
        self.connection.execute(
            "UPDATE paper_account SET report_started_at = ?, "
            "report_start_equity_usdt = ? WHERE id = 1",
            (now, equity),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
