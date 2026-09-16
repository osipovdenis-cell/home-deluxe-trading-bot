"""Prospective paired entry experiment. Never touches paper trading accounts."""
import math
import sqlite3


class RocketComparison:
    VERSION = "entry-ab-v1"
    HORIZON = 3600
    MAX_GAP = 30

    def __init__(self, connection: sqlite3.Connection, namespace="rocket_ab"):
        if namespace not in {"rocket_ab", "rocket_spread"}:
            raise ValueError("Unknown comparison namespace")
        self.namespace = namespace
        self.db = connection
        self._script("""
            CREATE TABLE IF NOT EXISTS rocket_ab_episodes (
                id INTEGER PRIMARY KEY, version TEXT NOT NULL, symbol TEXT NOT NULL,
                started REAL NOT NULL, decision TEXT NOT NULL, phase TEXT NOT NULL,
                stop REAL NOT NULL, cost REAL NOT NULL, finished REAL
            );
            CREATE INDEX IF NOT EXISTS rocket_ab_active
                ON rocket_ab_episodes(symbol,finished);
            CREATE TABLE IF NOT EXISTS rocket_ab_legs (
                episode INTEGER NOT NULL, variant TEXT NOT NULL,
                status TEXT NOT NULL, ready REAL, spread REAL,
                entered REAL, entry REAL, peak REAL, trough REAL,
                last_at REAL, last_price REAL, exited REAL, exit_price REAL,
                net REAL, reason TEXT,
                PRIMARY KEY(episode,variant)
            );
        """)

    def _execute(self, sql, parameters=()):
        return self.db.execute(sql.replace("rocket_ab_", self.namespace + "_"), parameters)

    def _script(self, sql):
        return self.db.executescript(sql.replace("rocket_ab_", self.namespace + "_"))

    def candidate(self, symbol, decision, reentry, now, spread_bps, stop, cost):
        """Call ONLY after quality/execution checks, using actual AI completion time."""
        if decision not in {"BUY", "WAIT", "SKIP"}:
            return
        if spread_bps is None or not math.isfinite(spread_bps) or spread_bps < 0:
            return
        if not all(math.isfinite(v) for v in (now, stop, cost)) or stop <= 0 or cost < 0:
            return
        active = self._execute(
            "SELECT id,started FROM rocket_ab_episodes WHERE symbol=? "
            "AND finished IS NULL AND version=?", (symbol, self.VERSION)
        ).fetchone()
        if active:
            episode, started = active
            if now < started + self.HORIZON and reentry:
                self._execute(
                    "UPDATE rocket_ab_legs SET status='READY',ready=?,spread=? "
                    "WHERE episode=? AND variant='B' AND status='WAIT'",
                    (now, spread_bps, episode),
                )
                self.db.commit()
            return
        episode = self._execute(
            "INSERT INTO rocket_ab_episodes(version,symbol,started,decision,phase,stop,cost) "
            "VALUES(?,?,?,?,?,?,?)",
            (self.VERSION, symbol, now, decision,
             "повторный сигнал" if reentry else "первичный сигнал", stop, cost),
        ).lastrowid
        for variant in ("A", "B"):
            ready = variant == "A" or decision == "BUY" or reentry
            self._execute(
                "INSERT INTO rocket_ab_legs(episode,variant,status,ready,spread) VALUES(?,?,?,?,?)",
                (episode, variant, "READY" if ready else "WAIT", now if ready else None, spread_bps),
            )
        self.db.commit()

    def tick(self, prices, now):
        """Fresh market snapshots only; no replay or retrospective entries."""
        episodes = self._execute(
            "SELECT id,symbol,started,stop,cost FROM rocket_ab_episodes "
            "WHERE finished IS NULL AND version=?", (self.VERSION,)
        ).fetchall()
        for episode, symbol, started, stop, cost in episodes:
            legs = self._execute(
                "SELECT variant,status,ready,spread,entry,peak,trough,last_at "
                "FROM rocket_ab_legs WHERE episode=?", (episode,)
            ).fetchall()
            deadline = started + self.HORIZON
            raw = prices.get(symbol)
            valid = raw is not None and math.isfinite(raw) and raw > 0
            for variant, status, ready, spread, entry, peak, trough, last_at in legs:
                if status not in {"WAIT", "READY", "OPEN"}:
                    continue
                # After an outage, never pretend the missing stop/peak is known.
                if status == "OPEN" and now - last_at > self.MAX_GAP:
                    self._status(episode, variant, "INCOMPLETE", "разрыв котировок")
                    continue
                if status == "READY" and now - ready > self.MAX_GAP:
                    self._status(episode, variant, "INCOMPLETE", "нет свежей цены входа")
                    continue
                if now >= deadline:
                    if status in {"WAIT", "READY"}:
                        self._status(episode, variant, "NO_ENTRY", "не вошёл за час")
                    elif valid and now - deadline <= self.MAX_GAP:
                        bid = raw * (1 - spread / 20000)
                        net = (bid / entry - 1) * 100 - cost
                        self._execute(
                            "UPDATE rocket_ab_legs SET status='MARKED',net=?,last_price=?,last_at=?,"
                            "reason='оценка открытой позиции на горизонте' WHERE episode=? AND variant=?",
                            (net, bid, now, episode, variant),
                        )
                    else:
                        self._status(episode, variant, "INCOMPLETE", "нет цены на горизонте")
                    continue
                if not valid or status == "WAIT":
                    continue
                if status == "READY":
                    if now <= ready:
                        continue
                    ask = raw * (1 + spread / 20000)
                    self._execute(
                        "UPDATE rocket_ab_legs SET status='OPEN',entered=?,entry=?,peak=?,trough=?,"
                        "last_at=?,last_price=? WHERE episode=? AND variant=?",
                        (now, ask, ask, ask, now, raw * (1 - spread / 20000), episode, variant),
                    )
                    continue
                if now <= last_at:
                    continue
                bid = raw * (1 - spread / 20000)
                peak, trough = max(peak, bid), min(trough, bid)
                change = (bid / entry - 1) * 100
                highest = (peak / entry - 1) * 100
                reason = None
                if change <= -stop:
                    reason = "стоп"
                elif highest >= 1 and change + 1e-9 < highest and change <= max(1, highest - 1):
                    reason = "трейлинг"
                self._execute(
                    "UPDATE rocket_ab_legs SET peak=?,trough=?,last_at=?,last_price=? "
                    "WHERE episode=? AND variant=?", (peak, trough, now, bid, episode, variant),
                )
                if reason:
                    self._execute(
                        "UPDATE rocket_ab_legs SET status='CLOSED',exited=?,exit_price=?,net=?,reason=? "
                        "WHERE episode=? AND variant=?",
                        (now, bid, change - cost, reason, episode, variant),
                    )
            # Keep the paired cohort open for the same hour even if A exits early.
            if now >= deadline:
                self._execute("UPDATE rocket_ab_episodes SET finished=? WHERE id=?", (now, episode))
        self.db.commit()

    def _status(self, episode, variant, status, reason):
        self._execute(
            "UPDATE rocket_ab_legs SET status=?,reason=? WHERE episode=? AND variant=?",
            (status, reason, episode, variant),
        )

    def report(self):
        rows = self._execute(
            "SELECT e.id,e.phase,e.finished,l.variant,l.status,l.net,l.reason,l.entry,l.trough "
            "FROM rocket_ab_episodes e JOIN rocket_ab_legs l ON l.episode=e.id "
            "WHERE e.version=? ORDER BY e.id,l.variant", (self.VERSION,)
        ).fetchall()
        lines = ["⚖️ Входы ракет A/B — теневая проверка",
                 f"Период: вся сохранённая история версии {self.VERSION}; не только последние 2 часа.",
                 "A: после проверок; B: WAIT/SKIP ждёт повторного сигнала."]
        if not rows:
            return "\n".join(lines + ["Новых пар пока нет. Торговля не изменена."])
        pending = len({r[0] for r in rows if r[2] is None})
        bad = {r[0] for r in rows if r[4] == "INCOMPLETE"}
        complete = {r[0] for r in rows if r[2] is not None} - bad
        lines.append(f"Завершено пар: {len(complete)}; наблюдаются: {pending}; неполные: {len(bad)}.")
        for phase in ("первичный сигнал", "повторный сигнал"):
            cohort = [r for r in rows if r[0] in complete and r[1] == phase]
            if not cohort:
                continue
            lines.append(phase.capitalize() + ":")
            for variant in ("A", "B"):
                group = [r for r in cohort if r[3] == variant]
                closed = [r for r in group if r[4] == "CLOSED"]
                marked = [r for r in group if r[4] == "MARKED"]
                no_entry = sum(r[4] == "NO_ENTRY" for r in group)
                realized = sum(r[5] for r in closed) * 0.5
                unrealized = sum(r[5] for r in marked) * 0.5
                total = realized + unrealized
                mae = [(r[8] / r[7] - 1) * 100 for r in group if r[7] and r[8]]
                average_mae = sum(mae) / len(mae) if mae else 0
                lines.append(
                    f"• {variant}: закрыто {len(closed)} (плюс {sum(r[5] > 0 for r in closed)}), "
                    f"открыто на горизонте {len(marked)}, без входа {no_entry}; "
                    f"закрытые {realized:+.3f}, открытые {unrealized:+.3f}, "
                    f"итого {total:+.3f} USDT; средняя просадка от входа {average_mae:.2f}%."
                )
        lines.extend([
            "Только сигналы с ответом AI и спредом, прошедшие рыночные проверки; одна пара на монету в час.",
            "Условно по 50 USDT на вход, без списания банка и лимита слотов. Горизонт пары 60 мин.",
            "Оценочные ask/bid: цена ± половина спреда сигнала; спред выхода фиксирован. "
            "Издержки и стоп фиксируются при создании пары. Проскальзывание не моделируется.",
            "Первичный/повторный — тип сигнала, не номер реальной сделки. Автовключения нет.",
        ])
        return "\n".join(lines)
