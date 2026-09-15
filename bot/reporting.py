"""Period summaries and an observable, prospective model evaluation journal."""
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math

from bot.probability import ProbabilityModel


def utc_stamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime('%d.%m.%Y %H:%M UTC')


def period_label(now, since):
    return f"Период: {utc_stamp(since)} — {utc_stamp(now)} ({(now-since)/3600:.1f} ч)."


def periods(now):
    return (("2 часа", now-7200), ("24 часа", now-86400), ("С запуска", -math.inf))


def result_line(values):
    gains = sum(max(v, 0) for v in values)
    losses = -sum(min(v, 0) for v in values)
    pf = f"{gains/losses:.2f}" if losses else "—"
    return (f"закрыто {len(values)}, в плюс {sum(v > 0 for v in values)}; "
            f"PnL {sum(values):+.3f} USDT; PF {pf}")


def rocket_totals(trader, prices, now):
    db = trader.connection
    rows = db.execute(
        "SELECT opened_at,closed_at,status,realized_pnl_usdt FROM paper_positions "
        "WHERE signal_kind LIKE '%лидер%' AND opened_at<=?", (now,)
    ).fetchall()
    lines = ["🚀 Общий итог ракет — виртуальные сделки",
             f"Срез: {utc_stamp(now)}. Периоды перекрываются, их не складываем."]
    if rows:
        lines.append(f"С запуска = вся сохранённая история ракет с {utc_stamp(min(r[0] for r in rows))}.")
    for label, since in periods(now):
        values = [float(r[3]) for r in rows
                  if r[2] == 'CLOSED' and r[1] is not None and since < r[1] <= now]
        entries = sum(since < r[0] <= now for r in rows)
        lines.append(f"• {label}: входов {entries}; {result_line(values)}.")
    open_rows = db.execute(
        "SELECT symbol,entry_price,remaining_quantity FROM paper_positions "
        "WHERE signal_kind LIKE '%лидер%' AND status='OPEN' AND opened_at<=?", (now,)
    ).fetchall()
    open_pnl, missing = 0.0, 0
    for symbol, entry, quantity in open_rows:
        price = prices.get(symbol)
        if price is None or not math.isfinite(price) or price <= 0:
            missing += 1
            continue
        open_pnl += entry * quantity * ((price/entry-1) - trader.round_trip_cost_percent/100)
    lines.extend([
        f"Открыто сейчас {len(open_rows)}; оценка PnL по доступным ценам {open_pnl:+.3f} USDT; без цены {missing}.",
        "Закрытые считаются по времени закрытия, включая входы до начала периода. "
        "Издержки — из журнала сделок; открытый PnL не включён в закрытый.",
    ])
    return '\n'.join(lines)


def scalp_totals(shadow, now):
    # Reporting intentionally has no 5000-row training cap.
    rows = shadow.db.execute(
        "SELECT created,finished,status,state FROM scalp_shadow WHERE version=? AND created<=?",
        (shadow.VERSION, now),
    ).fetchall()
    complete = [json.loads(r[3]) for r in rows if r[2] == 'DONE' and r[1] is not None and r[1] <= now]
    lines = ["🧪 Общий итог теневого скальпинга", f"Срез: {utc_stamp(now)}; версия {shadow.VERSION}."]
    if rows:
        lines.append(f"С запуска = вся сохранённая история этой версии с {utc_stamp(min(r[0] for r in rows))}.")
    names = {'current':'фильтры + AI', 'continuation':'продолжение', 'recovery':'восстановление'}
    for label, since in periods(now):
        lines.append(label + ':')
        selected = [r for r in rows if since < r[0] <= now]
        lines.append(f"Кандидатов {len(selected)}; неполных {sum(r[2]=='INCOMPLETE' for r in selected)}; "
                     f"без входа по спреду {sum(r[2]=='NO_ENTRY' for r in selected)}; "
                     f"вне лимита наблюдения {sum(r[2]=='CAPACITY' for r in selected)}.")
        found = False
        for policy in shadow.POLICIES:
            for exit_policy in ('all07','split'):
                legs = [s['legs'].get(policy+':'+exit_policy) for s in complete]
                values = [l['net']*.5 for l in legs
                          if l and l.get('net') is not None and since < l['exited'] <= now]
                if values:
                    found = True
                    lines.append(f"• {names[policy]}, {exit_policy}: {result_line(values)}.")
        if not found:
            lines.append('Полных результатов закрытых опытов пока нет.')
    lines.extend([
        f"Наблюдаются сейчас: {sum(r[2]=='ACTIVE' for r in rows)}. Торговля скальпинга выключена.",
        "PnL по времени финального выхода; только полные эпизоды. Позднее завершение наблюдения может дополнить прошлый период.",
        "По 50 USDT на независимый опыт, с издержками. Это не доходность банка; варианты и периоды не складываем.",
    ])
    return '\n'.join(lines)


class ModelJournal:
    """Persist frozen versions; compare forecasts made before labels were known."""
    def __init__(self, db):
        self.db = db
        db.executescript('''
            CREATE TABLE IF NOT EXISTS probability_versions (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, trained_at REAL NOT NULL,
                base_probability REAL NOT NULL, parameters TEXT NOT NULL,
                previous_id TEXT
            );
        ''')

    def register(self, kind, now, model, base_probability):
        parameters = json.dumps(asdict(model), sort_keys=True, allow_nan=False)
        key = kind + ':' + hashlib.sha256((parameters+repr(base_probability)).encode()).hexdigest()[:12]
        existing = self.db.execute('SELECT id FROM probability_versions WHERE id=?', (key,)).fetchone()
        if not existing:
            previous = self.db.execute(
                'SELECT id FROM probability_versions WHERE kind=? AND trained_at<=? ORDER BY trained_at DESC,rowid DESC LIMIT 1',
                (kind, now),
            ).fetchone()
            self.db.execute('INSERT INTO probability_versions VALUES(?,?,?,?,?,?)',
                            (key, kind, now, base_probability, parameters, previous[0] if previous else None))
            self.db.commit()
        return key

    def forecast(self, key, features, now):
        row = self.db.execute('SELECT trained_at,base_probability,parameters,previous_id FROM probability_versions WHERE id=?',
                              (key,)).fetchone()
        if not row or row[0] > now:
            return None
        prediction = ProbabilityModel(**json.loads(row[2])).predict_percent(features)/100
        previous_prediction = None
        if row[3]:
            prior = self.db.execute('SELECT parameters,trained_at FROM probability_versions WHERE id=?', (row[3],)).fetchone()
            if prior and prior[1] <= now:
                previous_prediction = ProbabilityModel(**json.loads(prior[0])).predict_percent(features)/100
        return dict(model_id=key, predicted_at=now, probability=prediction,
                    baseline=row[1], previous_id=row[3], previous_probability=previous_prediction)

    def report(self, kind, observations, now):
        row = self.db.execute(
            'SELECT id,trained_at,parameters FROM probability_versions WHERE kind=? AND trained_at<=? ORDER BY trained_at DESC,rowid DESC LIMIT 1',
            (kind, now),
        ).fetchone()
        if not row:
            return 'Версий обученной модели пока нет. Прогнозы ещё не проверяются.'
        key, trained_at, raw = row
        model = ProbabilityModel(**json.loads(raw))
        lines = [f"Модель {key}; обновлена {utc_stamp(trained_at)}.",
                 f"Примеров в пуле {model.examples}; отложенный контроль {model.validation_examples}; "
                 f"Brier {model.validation_brier_score:.3f}."]
        # These are stored forecasts, not predictions recomputed on known outcomes.
        current = [(y, meta) for y, meta in observations if meta and meta.get('model_id') == key
                   and trained_at <= meta.get('predicted_at', -1) <= now]
        lines.append(f"Новые созревшие прогнозы этой версии: {len(current)}.")
        if not current:
            # With 5-minute refits and 15-minute labels, the newest version may
            # never yet have mature predictions. Report the latest evaluated
            # version explicitly rather than showing a permanent zero.
            available = [(y,m) for y,m in observations if m and m.get('model_id','').startswith(kind+':')
                         and m.get('predicted_at',now+1) <= now]
            if available:
                evaluated_key = max(available,key=lambda pair:pair[1]['predicted_at'])[1]['model_id']
                current = [(y,m) for y,m in available if m['model_id']==evaluated_key]
                lines.append(f"Последняя версия с созревшими прогнозами: {evaluated_key}; n={len(current)}.")
        if current:
            brier = sum((m['probability']-y)**2 for y,m in current)/len(current)
            base = sum((m['baseline']-y)**2 for y,m in current)/len(current)
            lines.append(f"На них Brier модели {brier:.3f}, постоянной базовой вероятности {base:.3f} (меньше лучше).")
        paired = [(y,m) for y,m in current if m.get('previous_probability') is not None]
        if paired:
            new = sum((m['probability']-y)**2 for y,m in paired)/len(paired)
            old = sum((m['previous_probability']-y)**2 for y,m in paired)/len(paired)
            lines.append(f"Те же {len(paired)} новых случаев: новая версия {new:.3f}, предыдущая {old:.3f}; "
                         f"изменение Brier {new-old:+.3f}. Это наблюдение, устойчивое улучшение ещё не доказано.")
        else:
            lines.append('Сравнение с предыдущей версией: новых пар прогнозов пока нет.')
        lines.append('Это проверка вероятностей, а не доказательство прибыльности торговли.')
        return '\n'.join(lines)
