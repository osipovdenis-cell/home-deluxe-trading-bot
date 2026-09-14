"""Prospective scalping experiments. No orders, balances or trading permissions.

One episode per symbol/15 minutes. All entry policies share the first fresh
ask after decision completion; sales use observed bid, including gap overshoot.
Quotes are indicative: displayed size, latency and market impact are not fills.
"""
import json
import math
from collections import deque
import time

from bot.probability import train_probability_model
from bot.streams import PositionBookTickerStream


class ScalpQuoteStream(PositionBookTickerStream):
    """Keep ordered bid/ask events, not extrema that lose barrier ordering."""
    def __init__(self):
        super().__init__(max_symbols=20)
        self.quotes = deque(maxlen=100000)
        self.overflow = False

    def ingest(self, payload):
        item = json.loads(payload) if isinstance(payload, str) else payload
        item = item.get("data", item)
        symbol = item.get("s")
        try:
            bid, ask = float(item.get("b", 0)), float(item.get("a", 0))
        except (ValueError, TypeError):
            return
        if not all(math.isfinite(x) and x > 0 for x in (bid, ask)) or ask < bid:
            return
        with self._lock:
            if symbol not in self._symbols:
                return
            if len(self.quotes) == self.quotes.maxlen:
                self.overflow = True
            self.quotes.append((time.time(), symbol, bid, ask))

    def drain_quotes(self):
        with self._lock:
            rows, overflow = list(self.quotes), self.overflow
            self.quotes.clear()
            self.overflow = False
        return rows, overflow


class ScalpShadow:
    VERSION = "scalp-v1"
    HORIZON = 900
    MAX_GAP = 30
    MAX_SYMBOLS = 20
    POLICIES = ("current", "continuation", "recovery")

    def __init__(self, db):
        self.db = db
        self.cache = None
        self.cache_at = -math.inf
        db.executescript("""
            CREATE TABLE IF NOT EXISTS scalp_shadow (
                id INTEGER PRIMARY KEY, version TEXT NOT NULL, symbol TEXT NOT NULL,
                created REAL NOT NULL, finished REAL, status TEXT NOT NULL,
                features TEXT NOT NULL, state TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS scalp_shadow_active ON scalp_shadow(status,symbol);
        """)

    def active_symbols(self):
        return tuple(r[0] for r in self.db.execute(
            "SELECT symbol FROM scalp_shadow WHERE version=? AND status='ACTIVE' ORDER BY id",
            (self.VERSION,),
        ))

    def candidate(self, symbol, kind, now, features, accepted, cost):
        if not kind or "лидер" in kind:
            return
        if not math.isfinite(cost) or cost < 0:
            return
        if self.db.execute(
            "SELECT 1 FROM scalp_shadow WHERE version=? AND symbol=? AND (status='ACTIVE' OR created>?)",
            (self.VERSION, symbol, now - self.HORIZON),
        ).fetchone():
            return
        features = {k: v for k, v in features.items()
                    if isinstance(v, (int, float)) and math.isfinite(v)}
        positive = lambda k: features.get(k, 0) > 0
        pullback = features.get("pullback_from_high_percent")
        continuation = (pullback is not None and pullback >= -0.15 and all(
            positive(k) for k in ("confirmation_progress_percent", "confirmation_change_5s_percent",
                                 "confirmation_change_10s_percent", "change_60s_percent")))
        recovery = (pullback is not None and pullback < -0.15 and all(
            positive(k) for k in ("confirmation_change_5s_percent",
                                 "confirmation_change_10s_percent", "change_60s_percent")))
        full = len(self.active_symbols()) >= self.MAX_SYMBOLS
        state = dict(ready=now, snapshot_at=now, cost=cost, accepted=bool(accepted), decision="NOT_REQUESTED",
                     baseline=False, continuation=continuation, recovery=recovery,
                     reason="capacity" if full else "", entered=None, last=None,
                     label=None, peak=None, trough=None, legs={})
        self.db.execute(
            "INSERT INTO scalp_shadow(version,symbol,created,finished,status,features,state) "
            "VALUES(?,?,?,?,?,?,?)",
            (self.VERSION, symbol, now, now if full else None,
             "CAPACITY" if full else "ACTIVE", json.dumps(features), json.dumps(state)),
        )
        self.db.commit()

    def decision(self, symbol, now, allowed=None, decision=None, reason=None):
        row = self.db.execute(
            "SELECT id,state FROM scalp_shadow WHERE version=? AND symbol=? AND status='ACTIVE'",
            (self.VERSION, symbol),
        ).fetchone()
        if not row:
            return
        state = json.loads(row[1])
        if state["entered"] is not None:
            return
        if now - state["snapshot_at"] > self.MAX_GAP:
            state["reason"] = "decision delayed; features stale"
            self._save(row[0], state, "INCOMPLETE", now)
            return
        state["ready"] = max(now, state["ready"])
        if allowed is not None:
            state["baseline"] = bool(allowed)
        if decision is not None:
            state["decision"] = decision
        if reason is not None:
            state["reason"] = reason
        self._save(row[0], state)

    def _save(self, key, state, status="ACTIVE", finished=None):
        self.db.execute("UPDATE scalp_shadow SET state=?,status=?,finished=? WHERE id=?",
                        (json.dumps(state), status, finished, key))

    def expire(self, now, overflow=False):
        for key, raw in self.db.execute(
            "SELECT id,state FROM scalp_shadow WHERE version=? AND status='ACTIVE'", (self.VERSION,)
        ).fetchall():
            s = json.loads(raw)
            reference = s["last"] if s["last"] is not None else s["ready"]
            if overflow or now - reference > self.MAX_GAP:
                s["reason"] = "quote buffer overflow" if overflow else "quote gap"
                self._save(key, s, "INCOMPLETE", now)

    def quote(self, now, symbol, bid, ask):
        if not all(math.isfinite(v) and v > 0 for v in (bid, ask)) or ask < bid:
            return
        row = self.db.execute(
            "SELECT id,state FROM scalp_shadow WHERE version=? AND symbol=? AND status='ACTIVE'",
            (self.VERSION, symbol),
        ).fetchone()
        if not row:
            return
        key, raw = row
        s = json.loads(raw)
        if now <= s["ready"] or (s["last"] is not None and now <= s["last"]):
            return
        previous = s["last"] if s["last"] is not None else s["ready"]
        if now - previous > self.MAX_GAP:
            s["reason"] = "quote gap"
            self._save(key, s, "INCOMPLETE", now)
            return
        if s["entered"] is None:
            # Same existing ordinary spread ceiling for every shadow policy.
            if (ask / bid - 1) * 100 > 0.1:
                s["reason"] = "entry spread > 0.1%"
                self._save(key, s, "NO_ENTRY", now)
                return
            s.update(entered=now, entry=ask, last=now, peak=(bid/ask-1)*100,
                     trough=(bid/ask-1)*100)
            s["probability"] = self.predict(now, self.features(key))
            for policy in self.POLICIES:
                selected = s["baseline"] if policy == "current" else s[policy]
                if selected:
                    for exit_policy in ("all07", "split"):
                        s["legs"][policy + ":" + exit_policy] = dict(
                            remaining=1.0, gross=0.0, first=False, net=None)
        s["last"] = now
        ret = (bid / s["entry"] - 1) * 100
        deadline = s["entered"] + self.HORIZON
        # Do not use a post-deadline price to claim an earlier barrier crossing.
        timed_out = now >= deadline
        if not timed_out:
            s["peak"], s["trough"] = max(s["peak"], ret), min(s["trough"], ret)
        if s["label"] is None:
            if not timed_out and ret <= -0.5:
                s["label"] = 0
            elif not timed_out and ret >= 0.7:
                s["label"] = 1
            elif timed_out:
                s["label"] = 0
                s["neutral"] = True
        for name, leg in s["legs"].items():
            if leg["net"] is not None:
                continue
            if not timed_out and name.endswith(":split") and ret >= 0.7 and not leg["first"]:
                leg["gross"] += 0.5 * ret
                leg.update(remaining=0.5, first=True)
            target = 0.7 if name.endswith(":all07") else 1.0
            if ret <= -0.5 or ret >= target or timed_out:
                leg["gross"] += leg["remaining"] * ret
                leg.update(remaining=0.0, net=leg["gross"] - s["cost"],
                           exited=now, reason="horizon" if timed_out else "stop" if ret <= -0.5 else "target")
        self._save(key, s, "DONE" if timed_out else "ACTIVE", now if timed_out else None)

    def features(self, key):
        return json.loads(self.db.execute("SELECT features FROM scalp_shadow WHERE id=?", (key,)).fetchone()[0])

    def samples(self, before):
        rows = self.db.execute(
            "SELECT symbol,features,state,finished FROM scalp_shadow "
            "WHERE version=? AND status='DONE' AND finished<=? ORDER BY created DESC LIMIT 5000",
            (self.VERSION, before),
        ).fetchall()
        return sorted([(symbol, json.loads(f), json.loads(s), end) for symbol, f, s, end in rows],
                      key=lambda row: row[2]["ready"])

    def predict(self, now, features):
        if now - self.cache_at >= 300:
            rows = self.samples(now)
            self.cache = train_probability_model([
                (s["label"], dict(f, _observed_at=s["ready"], _label_end=end))
                for _, f, s, end in rows
            ], purge_overlap=True)
            self.cache_at = now
        return self.cache.predict_percent(features) if self.cache else None

    def report(self, now):
        counts = dict(self.db.execute(
            "SELECT status,COUNT(*) FROM scalp_shadow WHERE version=? GROUP BY status", (self.VERSION,)))
        lines = ["🧪 Скальпинг отдельно — только тень (v1)",
                 f"Завершено: {counts.get('DONE',0)}; наблюдаются: {counts.get('ACTIVE',0)}; "
                 f"неполные: {counts.get('INCOMPLETE',0)}; спред: {counts.get('NO_ENTRY',0)}; "
                 f"лимит наблюдения: {counts.get('CAPACITY',0)}."]
        rows = self.samples(now)
        lines.append(f"Сравнение по последним {len(rows)} полным эпизодам (максимум 5000):")
        for policy in self.POLICIES:
            for exit_policy in ("all07", "split"):
                legs = [s["legs"].get(policy+":"+exit_policy) for _,_,s,_ in rows]
                closed = sorted([leg for leg in legs if leg and leg["net"] is not None],
                                key=lambda leg: leg["exited"])
                nets = [leg["net"] * 0.5 for leg in closed]
                if not nets:
                    continue
                gains, losses = sum(max(x,0) for x in nets), -sum(min(x,0) for x in nets)
                equity = high = drawdown = 0.0
                for net in nets:
                    equity += net
                    high = max(high, equity)
                    drawdown = max(drawdown, high - equity)
                title = {"current":"базовые фильтры + AI (новая история)", "continuation":"продолжение", "recovery":"восстановление"}[policy]
                pf = f"{gains/losses:.2f}" if losses else "нет убытков"
                lines.append(f"• {title}, {exit_policy}: n={len(nets)}, PnL {sum(nets):+.3f} USDT, "
                             f"PF {pf}, просадка суммы закрытий {drawdown:.3f} USDT.")
        self.predict(now, {})
        if self.cache:
            lines.append(f"Модель только скальпинга: n={self.cache.examples}, контроль={self.cache.validation_examples}; "
                         f"база {self.cache.validation_base_rate_percent:.1f}%, верхняя четверть "
                         f"{self.cache.validation_top_quartile_rate_percent:.1f}%, Brier {self.cache.validation_brier_score:.3f}.")
        else:
            lines.append(f"Обучение на новых согласованных примерах: {len(rows)}/400 (с временным зазором может потребоваться больше).")
        lines.extend(["Все варианты: условные 50 USDT на вход; ask→bid, стоп −0,5%, горизонт 15 мин.",
                      "all07: всё +0,7%; split: 50% +0,7% / 50% +1%. Комиссии учтены; нейтральные закрываются по bid.",
                      "Это сумма независимых опытов, не доходность банка. Объём исполнения и проскальзывание не моделируются.",
                      "Новые правила — проверяемые гипотезы; AI не разрешает реальные покупки скальпинга."])
        return "\n".join(lines)
