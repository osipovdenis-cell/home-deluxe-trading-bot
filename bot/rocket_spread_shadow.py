"""Isolate the spread contraction gate prospectively; never place orders."""
import math
from collections import Counter
from bot.rocket_comparison import RocketComparison


class SpreadShadow(RocketComparison):
    VERSION = 'stable-spread-v1'

    def __init__(self, connection):
        super().__init__(connection, 'rocket_spread')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS rocket_gate_decisions(
                id INTEGER PRIMARY KEY, timestamp REAL NOT NULL, symbol TEXT NOT NULL,
                stage TEXT NOT NULL, reason TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS rocket_gate_time ON rocket_gate_decisions(timestamp);
            CREATE TABLE IF NOT EXISTS rocket_spread_features(
                episode INTEGER PRIMARY KEY, spread_change REAL NOT NULL,
                volume_ratio REAL, cvd REAL, price_60 REAL);
        ''')

    def record_gate(self, now, symbol, stage, reason):
        self.db.execute('INSERT INTO rocket_gate_decisions(timestamp,symbol,stage,reason) VALUES(?,?,?,?)',
                        (now, symbol, stage, reason))
        self.db.commit()

    def observe(self, signal, context, dynamics, market, now, stop, cost):
        # Called after volume/execution gates, BEFORE the actual quality veto.
        delta = context.flow_spread_change_bps
        spread = context.spread_bps
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool)
                   and math.isfinite(v) for v in (delta, spread, now, stop, cost)):
            return
        if not 0 <= spread <= 25 or stop <= 0 or cost < 0:
            return
        if not market.leader_entry_quality(context, dynamics, allow_stable_spread=True)[0]:
            return
        if self._execute('SELECT 1 FROM rocket_ab_episodes WHERE symbol=? AND started>? AND version=?',
                         (signal.symbol, now-self.HORIZON, self.VERSION)).fetchone():
            return
        phase = 'stable' if delta == 0 else 'contracting'
        ident = self._execute('''INSERT INTO rocket_ab_episodes
            (version,symbol,started,decision,phase,stop,cost) VALUES(?,?,?,?,?,?,?)''',
            (self.VERSION, signal.symbol, now, 'ISOLATED_GATE', phase, stop, cost)).lastrowid
        for variant in ('A', 'B'):
            allowed = variant == 'B' or delta < 0
            self._execute('''INSERT INTO rocket_ab_legs
                (episode,variant,status,ready,spread,reason) VALUES(?,?,?,?,?,?)''',
                (ident, variant, 'READY' if allowed else 'NO_ENTRY', now if allowed else None,
                 spread, None if allowed else 'стабильный спред'))
        self.db.execute('INSERT INTO rocket_spread_features VALUES(?,?,?,?,?)',
                        (ident, delta, context.volume_ratio_5m, context.flow_cvd_60s_percent,
                         context.flow_price_change_60s_percent))
        self.db.commit()

    def report(self, now):
        lines = ['⚖️ Стабильный спред ракет — только тень',
                 'A: спред сокращается. B: также допускается неизменный спред. Лимит 25 б.п. сохранён.']
        rows = self._execute('''SELECT e.id,e.phase,e.finished,l.variant,l.status,l.net
            FROM rocket_ab_episodes e JOIN rocket_ab_legs l ON l.episode=e.id
            WHERE e.version=?''', (self.VERSION,)).fetchall()
        bad = {r[0] for r in rows if r[4] == 'INCOMPLETE'}
        complete = {r[0] for r in rows if r[2] is not None} - bad
        pending = {r[0] for r in rows if r[2] is None} - bad
        lines.append(f'Полных пар: {len(complete)}; ожидаются: {len(pending)}; неполных: {len(bad)}.')
        for phase, name in [('stable', 'Стабильный спред — отличие вариантов'),
                            ('contracting', 'Сокращающийся спред — общий контроль')]:
            cohort = [r for r in rows if r[0] in complete and r[1] == phase]
            lines.append(name + f': {len(cohort)//2} пар.')
            for variant in ('A', 'B'):
                legs = [r for r in cohort if r[3] == variant]
                closed = [r for r in legs if r[4] == 'CLOSED']
                marked = [r for r in legs if r[4] == 'MARKED']
                lines.append(f"• {variant}: закрыто {len(closed)}, плюс {sum(r[5]>0 for r in closed)}, "
                             f"PnL {sum(r[5] for r in closed)*.5:+.3f} USDT; "
                             f"на горизонте {len(marked)} на {sum(r[5] for r in marked)*.5:+.3f}; "
                             f"без входа {sum(r[4]=='NO_ENTRY' for r in legs)}.")
        lines.extend(['После 20с, объёма, исполнения и прочих проверок качества; учитываются и отклонённые по спреду сигналы.',
                      'Изолированный тест фильтра: AI, финальная перепроверка и лимиты банка в обеих ветках не моделируются.',
                      'По 50 USDT; стоп и комиссия фиксируются на старте, защита +1%, откат 1 п.п.; горизонт 60 мин.',
                      'Оценочные ask/bid из цены ± половина спреда сигнала; спред выхода фиксирован. Разрыв >30с исключает пару. Нет проскальзывания. Автовключения нет.'])
        decisions = self.db.execute('SELECT stage,reason FROM rocket_gate_decisions WHERE timestamp>=? AND timestamp<=?',
                                    (now-7200, now)).fetchall()
        counts = Counter(decisions)
        lines.append(f'🔎 Причины по ракетам за 2 ч.: {len(decisions)} событий; не уникальные монеты.')
        lines.extend(f'• {stage}: {reason} — {count}' for (stage,reason),count in counts.most_common(8))
        return '\n'.join(lines)
