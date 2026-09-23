"""Diagnostic replays on recorded rocket entries; never changes trading rules."""
import json
import math

from bot.reporting import utc_stamp


STOPS = (0.5, 1.0, 1.5, 2.0)
WINDOWS = (20, 60)
VERSION = "rocket-stops-bid-v2"


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

    @staticmethod
    def _valid_case(case, row, minutes):
        """Accept only bounded, complete bid comparisons for this exact trade."""
        try:
            end = row["closed_at"] + minutes * 60
            if (case["position_id"] != row["id"] or case["symbol"] != row["symbol"]
                    or case["source"] != "bid" or case["opened_at"] != row["opened_at"]
                    or case["entry_price"] != row["entry_price"]
                    or case["closed_at"] != row["closed_at"]
                    or case["horizon_end_at"] != end
                    or case["horizon_minutes"] != minutes):
                return False
            cost = case["cost"]
            if not math.isfinite(cost) or cost < -1e-6:
                return False
            for stop in STOPS:
                leg = case["legs"][str(stop)]
                if (leg["reason"] not in ("STOP", "TRAIL", "OPEN")
                        or not row["opened_at"] <= leg["at"] <= end
                        or not math.isfinite(leg["price"]) or leg["price"] <= 0
                        or not math.isfinite(leg["pnl"])):
                    return False
                if leg["reason"] == "OPEN" and end - leg["at"] > 5:
                    return False
                net = (leg["price"] / row["entry_price"] - 1) * 100 - cost
                if not math.isclose(leg["pnl"], net * .5, abs_tol=1e-8):
                    return False
            return True
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            return False

    def _evaluate(self, row, now, minutes):
        # The card builder imports replay from this module. Import it here after
        # module initialization to share its coverage checks without a cycle.
        from bot.rocket_cards import build_card, exists

        card = None
        if exists(self.db, "rocket_trade_cards"):
            saved = self.db.execute(
                "SELECT payload FROM rocket_trade_cards WHERE position_id=?",
                (row["id"],),
            ).fetchone()
            if saved:
                try:
                    card = json.loads(saved[0])
                except (ValueError, TypeError):
                    pass
        # Persisted full cards outlive raw quote retention. Partial cards can be
        # rebuilt as a late batch arrives; never permanently cache INCOMPLETE.
        for attempt in range(2):
            if isinstance(card, dict) and card.get("source") == "bid":
                legs = card.get("comparisons", {}).get(str(minutes))
                if legs:
                    case = dict(position_id=card.get("position_id"),
                                symbol=card.get("symbol"), source="bid",
                                opened_at=card.get("opened_at"),
                                entry_price=card.get("entry_price"),
                                closed_at=card.get("closed_at"),
                                cost=card.get("cost_percent"),
                                horizon_minutes=minutes,
                                horizon_end_at=row["closed_at"] + minutes * 60,
                                actual_reason=row["close_reason"],
                                actual_pnl=row["realized_pnl_usdt"], legs=legs)
                    if self._valid_case(case, row, minutes):
                        return case
            if attempt == 0:
                card = build_card(self.db, row, now)
        return None

    def collect(self, now, minutes=60):
        if minutes not in WINDOWS:
            raise ValueError("Unsupported post-exit horizon")
        rows = self.db.execute(
            "SELECT * FROM paper_positions WHERE signal_kind LIKE '%лидер%' "
            "AND opened_at<=? ORDER BY opened_at,id", (now,),
        ).fetchall()
        complete, pending, incomplete = [], 0, 0
        version = f"{VERSION}:{minutes}m"
        for row in rows:
            if row["closed_at"] is None or now < row["closed_at"] + minutes * 60:
                pending += 1
                continue
            saved = self.db.execute(
                "SELECT status,payload FROM rocket_stop_replays "
                "WHERE version=? AND position_id=? AND evaluated_at<=?",
                (version, row["id"], now),
            ).fetchone()
            case = None
            if saved and saved[0] == "DONE":
                try:
                    candidate = json.loads(saved[1])
                    if self._valid_case(candidate, row, minutes):
                        case = candidate
                except (ValueError, TypeError):
                    pass
            if case is None:
                case = self._evaluate(row, now, minutes)
                if case is not None:
                    self.db.execute(
                        "INSERT OR REPLACE INTO rocket_stop_replays VALUES(?,?,?,?,?)",
                        (version, row["id"], now, "DONE", json.dumps(case)),
                    )
            if case is not None:
                complete.append(case)
            else:
                incomplete += 1
        self.db.commit()
        return complete, pending, incomplete

    def report_texts(self, now):
        lines = ["🛑 Стопы ракет — сравнение на одинаковых входах",
                 f"Срез: {utc_stamp(now)}. Вся сохранённая история; {VERSION}.",
                 "Источник — полные bid-пути карточек сделок. "
                 "Каждый вариант: 50 USDT, 100% позиции, защита от +1%, откат 1 п.п."]
        for minutes in WINDOWS:
            cases, pending, incomplete = self.collect(now, minutes)
            lines += [f"\nОт покупки до фактического выхода + {minutes} мин:",
                      f"Полных {len(cases)}; ожидаются {pending}; неполных {incomplete}."]
            for stop in STOPS:
                legs = [c["legs"][str(stop)] for c in cases]
                closed = [l for l in legs if l["reason"] != "OPEN"]
                opened = [l for l in legs if l["reason"] == "OPEN"]
                wins = sum(l["pnl"] > 0 for l in closed)
                losses = sum(l["pnl"] < 0 for l in closed)
                lines.append(
                    f"• Стоп −{stop:g}%: закрыто {len(closed)} (+{wins}/−{losses}), "
                    f"PnL {sum(l['pnl'] for l in closed):+.3f}; "
                    f"открыто {len(opened)} ({sum(l['pnl'] for l in opened):+.3f} USDT)."
                )
        lines.extend([
            "\nВнутри окна все стопы сравниваются на одних сделках. "
            "Выборки 20/60 мин могут отличаться и пересекаться; их не складываем.",
            "Это пересчёт, не результат банка. Открытые суммы — оценки, "
            "они не включены в закрытый PnL. Издержки каждой сделки сохранены; "
            "глубина и проскальзывание не моделируются.",
            "Разрывы записи или пробелы bid >5с исключают случай из всех вариантов окна. "
            "Неполные данные не достраиваются. Торговые правила не меняются.",
        ])
        # One replacement for the existing summary; per-trade details remain in
        # the existing cards rather than generating another Telegram message.
        yield "\n".join(lines)
