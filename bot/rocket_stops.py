"""Diagnostic replays on recorded rocket entries; never changes trading rules."""
import json
import math

from bot.reporting import utc_stamp


STOPS = (0.5, 1.0, 1.5, 2.0)
HORIZON = 3600
MAX_GAP = 30
VERSION = "rocket-stops-v1"


def replay(path, entry, stop, cost):
    """Chronological first exit, 100% position, same +1%/1pp protection as rockets.

    A crossing uses the observed price, including overshoot. An unclosed leg is
    marked to market at the observation horizon, not counted as a closed trade.
    """
    peak = low = 0.0
    armed = False
    reason = "OPEN"
    for timestamp, price in path:
        change = (price / entry - 1) * 100
        peak, low = max(peak, change), min(low, change)
        if change <= -stop + 1e-9:
            reason = "STOP"
            break
        armed = armed or peak >= 1.0
        if armed and change + 1e-9 < peak and change <= max(1.0, peak - 1.0):
            reason = "TRAIL"
            break
    return dict(reason=reason, at=timestamp, price=price, armed=armed,
                net_percent=change-cost, pnl=(change-cost)*0.5,
                mae=low, mfe=peak)


class RocketStopAudit:
    def __init__(self, db):
        self.db = db
        db.executescript("""
            CREATE TABLE IF NOT EXISTS rocket_stop_costs (
                position_id INTEGER PRIMARY KEY, cost_percent REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rocket_stop_replays (
                version TEXT NOT NULL, position_id INTEGER NOT NULL,
                evaluated_at REAL NOT NULL, status TEXT NOT NULL,
                payload TEXT NOT NULL, PRIMARY KEY(version, position_id)
            );
        """)

    def snapshot_cost(self, position_id, cost):
        self.db.execute("INSERT OR IGNORE INTO rocket_stop_costs VALUES(?,?)",
                        (position_id, cost))

    def _cost(self, row):
        saved = self.db.execute(
            "SELECT cost_percent FROM rocket_stop_costs WHERE position_id=?",
            (row["id"],),
        ).fetchone()
        if saved is not None:
            return float(saved[0])
        # Backfill old, fully closed single-exit rockets from their actual PnL.
        # Never silently apply today's fee setting to an older trade.
        if row["status"] != "CLOSED":
            return None
        fills = self.db.execute(
            "SELECT price,quantity,pnl_usdt FROM paper_fills "
            "WHERE position_id=? AND side='SELL'", (row["id"],),
        ).fetchall()
        if len(fills) != 1 or not math.isclose(
                fills[0][1], row["initial_quantity"], rel_tol=1e-6):
            return None
        price, quantity, pnl = fills[0]
        cost = ((price/row["entry_price"]-1) - pnl/(row["entry_price"]*quantity))*100
        if not math.isfinite(cost) or cost < -1e-6:
            return None
        return max(0.0, cost)

    def _evaluate(self, row, cost):
        start, end = row["opened_at"], row["opened_at"] + HORIZON
        exists = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='samples'"
        ).fetchone()
        if not exists:
            return dict(error="нет ценовой истории")
        samples = self.db.execute(
            "SELECT timestamp,price FROM samples WHERE symbol=? "
            "AND timestamp>? AND timestamp<=? ORDER BY timestamp,rowid",
            (row["symbol"], start, end),
        ).fetchall()
        if not samples:
            return dict(error="нет ценовой истории")
        points = {float(t): float(p) for t, p in samples}
        # Exact paper exit prices anchor the otherwise last-trade-price history.
        for t, p in self.db.execute(
            "SELECT timestamp,price FROM paper_fills WHERE position_id=? "
            "AND side='SELL' AND timestamp>? AND timestamp<=? ORDER BY timestamp,id",
            (row["id"], start, end),
        ):
            points[float(t)] = float(p)
        points[start] = row["entry_price"]
        path = sorted(points.items())
        if any(not math.isfinite(t) or not math.isfinite(p) or p <= 0 for t, p in path):
            return dict(error="некорректные цены")
        if end-path[-1][0] > MAX_GAP or any(
                b[0]-a[0] > MAX_GAP for a, b in zip(path, path[1:])):
            return dict(error="пропуск цен больше 30 секунд")
        return dict(symbol=row["symbol"], opened_at=start, cost=cost,
                    last_quote_at=path[-1][0], actual_reason=row["close_reason"],
                    actual_pnl=row["realized_pnl_usdt"] if row["status"] == "CLOSED" else None,
                    legs={str(s): replay(path, row["entry_price"], s, cost) for s in STOPS})

    def collect(self, now):
        rows = self.db.execute(
            "SELECT * FROM paper_positions WHERE signal_kind LIKE '%лидер%' "
            "AND opened_at<=? ORDER BY opened_at,id", (now,),
        ).fetchall()
        complete, pending, incomplete = [], 0, 0
        for row in rows:
            if now < row["opened_at"] + HORIZON:
                pending += 1
                continue
            saved = self.db.execute(
                "SELECT status,payload FROM rocket_stop_replays "
                "WHERE version=? AND position_id=? AND evaluated_at<=?",
                (VERSION, row["id"], now),
            ).fetchone()
            if saved is None:
                cost = self._cost(row)
                if cost is None and row["status"] == "OPEN":
                    pending += 1
                    continue
                payload = (dict(error="неизвестны исторические издержки")
                           if cost is None else self._evaluate(row, cost))
                status = "INCOMPLETE" if "error" in payload else "DONE"
                self.db.execute(
                    "INSERT OR REPLACE INTO rocket_stop_replays VALUES(?,?,?,?,?)",
                    (VERSION, row["id"], now, status, json.dumps(payload)),
                )
            else:
                status, serialized = saved
                payload = json.loads(serialized)
            if status == "DONE":
                complete.append(payload)
            else:
                incomplete += 1
        self.db.commit()
        return complete, pending, incomplete

    def report_texts(self, now):
        cases, pending, incomplete = self.collect(now)
        lines = ["🛑 Стопы ракет — сравнение на одинаковых входах",
                 f"Срез: {utc_stamp(now)}. Вся сохранённая выборка; {VERSION}.",
                 f"Полных путей: {len(cases)}; ожидаются: {pending}; неполных: {incomplete}.",
                 "Каждый вариант: 50 USDT, 100% позиции, защита от +1%, откат 1 п.п.; "
                 "горизонт 60 минут от входа."]
        for stop in STOPS:
            legs = [c["legs"][str(stop)] for c in cases]
            closed = [l for l in legs if l["reason"] != "OPEN"]
            opened = [l for l in legs if l["reason"] == "OPEN"]
            realized = sum(l["pnl"] for l in closed)
            unrealized = sum(l["pnl"] for l in opened)
            lines.append(
                f"• Стоп −{stop:g}%: закрыто {len(closed)} (плюс {sum(l['pnl']>0 for l in closed)}), "
                f"стопов {sum(l['reason']=='STOP' for l in closed)}, "
                f"по защите {sum(l['reason']=='TRAIL' for l in closed)}; "
                f"закрытые {realized:+.3f}, открыто {len(opened)} на {unrealized:+.3f}, "
                f"итого {realized+unrealized:+.3f} USDT."
            )
        if cases:
            baseline = [c["legs"]["0.5"] for c in cases]
            wide = [c["legs"]["2.0"] for c in cases]
            pairs = [(a,b) for a,b in zip(baseline,wide) if a["reason"] == "STOP"]
            saved = sum(b["reason"] != "OPEN" and b["pnl"] > 0 for a,b in pairs)
            worse = sum(b["reason"] != "OPEN" and b["pnl"] < a["pnl"]-1e-9 for a,b in pairs)
            still_open = sum(b["reason"] == "OPEN" for a,b in pairs)
            delta = sum(b["pnl"]-a["pnl"] for a,b in zip(baseline,wide))
            lines.append(f"−2% против −0,5%: среди {len(pairs)} стопов базового пересчёта "
                         f"закрылись в плюс {saved}, увеличили закрытый убыток {worse}, "
                         f"остались открыты {still_open}. Разница итогов {delta:+.3f} USDT.")
        lines.extend([
            "Это пересчёт всей доступной истории входов ракет, включая прибыльные. "
            "Он не добавляет пропущенные сигналы и не моделирует занятость банка.",
            "Цены истории — последние сделки с ценами фактических виртуальных выходов; "
            "издержки фиксируются на входе либо восстанавливаются из старой закрытой сделки. "
            "Bid/ask и проскальзывание после выхода не восстановлены. Пробелы >30 с исключены.",
            "Открытый результат — оценка на горизонте, не закрытая прибыль. "
            "Это предварительная оценка, не доходность банка. Торговые стопы автоматически не меняются.",
        ])
        yield "\n".join(lines)
        stopped = [c for c in cases if c["actual_reason"] == "стоп-лосс"][-5:]
        if stopped:
            details = ["🔎 Последние фактические стопы ракет — пересчёт по 50 USDT"]
            labels = {"STOP":"стоп", "TRAIL":"защита прибыли", "OPEN":"открыта на горизонте"}
            for c in stopped:
                a, b = c["legs"]["0.5"], c["legs"]["2.0"]
                details.append(f"• {c['symbol']}, {utc_stamp(c['opened_at'])}: "
                               f"−0,5% → {a['pnl']:+.3f} ({labels[a['reason']]}); "
                               f"−2% → {b['pnl']:+.3f} ({labels[b['reason']]}), "
                               f"минимум до выхода/горизонта {b['mae']:+.2f}%.")
            yield "\n".join(details)
