"""Prospective per-decision rocket outcomes. Separate DB and stream; no orders."""
import json
import math
import sqlite3
import threading
import time
from collections import Counter, deque
from pathlib import Path
from queue import SimpleQueue, Empty
from datetime import datetime, timezone
from copy import deepcopy

from bot.rocket_quote_stream import RocketQuoteStream
from bot.rocket_recovery_shadow import new_leg, advance
from bot import rocket_volume_shadow as volume_shadow

SUFFIX = '.rocket_daily.sqlite3'
HORIZON = 3600


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS episodes(id TEXT PRIMARY KEY,symbol TEXT,at REAL,payload TEXT);
      CREATE INDEX IF NOT EXISTS episodes_at ON episodes(at);
      CREATE TABLE IF NOT EXISTS health(key TEXT PRIMARY KEY,value TEXT);
    ''')


class DailyModel:
    def __init__(self, db):
        self.db = db
        schema(db)
        self.active = {}
        for ident, payload in db.execute('SELECT id,payload FROM episodes'):
            s = json.loads(payload)
            if s['leg']['status'] in ('WAIT', 'OPEN'):
                s['leg'].update(status='INCOMPLETE', reason='перезапуск регистратора')
                self.save(ident, s)
        db.commit()

    def reload_active(self):
        self.active = {ident: s for ident, payload in self.db.execute('SELECT id,payload FROM episodes')
                       if (s := json.loads(payload))['leg']['status'] in ('WAIT', 'OPEN')}

    def save(self, ident, s):
        self.db.execute('INSERT OR REPLACE INTO episodes VALUES(?,?,?,?)',
                        (ident, s['symbol'], s['at'], json.dumps(s, allow_nan=False)))

    def capture(self, event):
        ident = event['id']
        if self.db.execute('SELECT 1 FROM episodes WHERE id=?', (ident,)).fetchone():
            return
        if not all(math.isfinite(event[k]) for k in ('at','stop','cost')) or event['stop']<=0 or event['cost']<0:
            raise ValueError('invalid decision')
        s = dict(deepcopy(event), quote_version=2, leg=dict(status='WAIT', net=None))
        experiment = s.get('volume_experiment')
        if experiment:
            stamp = experiment.get('evaluated_at')
            if (experiment['state'] != 'UNKNOWN' and
                    (not volume_shadow.finite(stamp) or not 0 <= s['at']-stamp <= 2)):
                experiment.update(state='UNKNOWN', reasons=['снимок устарел к записи отказа'])
            s['volume_execution'] = dict(state={'ELIGIBLE':'PENDING', 'NO_ENTRY':'NO_ENTRY',
                                                'UNKNOWN':'UNKNOWN'}[experiment['state']])
        if len({x['symbol'] for x in self.active.values()} | {s['symbol']}) > 100:
            s['leg'].update(status='INCOMPLETE', reason='лимит 100 монет')
        self.save(ident, s)
        if s['leg']['status']=='WAIT':
            self.active[ident]=s

    def tick(self, now, quotes, overflow=False, gaps=()):
        before = deepcopy(self.active)
        try:
            self._tick(now, quotes, overflow, gaps)
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.active = before
            raise

    def _tick(self, now, quotes, overflow, gaps):
        grouped = {}
        for at,symbol,bid,ask in sorted(quotes):
            if all(math.isfinite(x) and x>0 for x in (at,bid,ask)) and ask>=bid:
                grouped.setdefault(symbol, []).append((at,bid,ask))
        for ident,s in list(self.active.items()):
            leg=s['leg']
            if overflow:
                leg.update(status='INCOMPLETE', reason='пропуск регистратора/переполнение')
            gap = min((at for at, symbol in gaps if symbol == s['symbol']
                       and s['at'] <= at <= min(now, s['at']+HORIZON)), default=None)
            # A close observed before a disconnect remains valid; later prices
            # cannot turn an interrupted path into a known profit or loss.
            rows=[r for r in grouped.get(s['symbol'], []) if gap is None or r[0] < gap]
            if leg['status']=='WAIT':
                first=next((r for r in rows if r[0]>=s['at']), None)
                # No hindsight: first observed quote, never search for a nicer entry.
                if first and first[0]-s['at']<=5:
                    at,bid,ask=first
                    if s.get('volume_execution',{}).get('state') == 'PENDING':
                        s['volume_execution'] = volume_shadow.first_quote(
                            s['volume_experiment'], first, s['stop'])
                    s['leg']=leg=new_leg(at,ask,bid)
                    s['entry_delay_seconds']=at-s['at']
                    # Include the initial spread in immediate barrier evaluation.
                    leg['last_at']=at-1e-6
                elif now-s['at']>5:
                    leg.update(status='INCOMPLETE', reason='нет первой котировки в пределах 5с')
            advance(leg, [(at,bid) for at,bid,ask in rows], now if gap is None else gap,
                    s['at']+HORIZON, s['stop'], s['cost'])
            if gap is not None and leg['status'] in ('WAIT', 'OPEN', 'INCOMPLETE'):
                leg.update(status='INCOMPLETE', reason='разрыв соединения котировок')
            if leg['status']=='INCOMPLETE' and s.get('volume_execution',{}).get('state')=='PENDING':
                s['volume_execution'].update(state='UNKNOWN', reason=leg.get('reason'))
            self.save(ident,s)
            if leg['status'] not in ('WAIT','OPEN'):
                del self.active[ident]


class DailyWorker:
    def __init__(self, path):
        self.path=path+SUFFIX
        self.queue=SimpleQueue()
        self.stop=threading.Event()
        self.stream=RocketQuoteStream()
        self.watch=()
        self.thread=None

    def capture(self, symbol, signal_at, at, reason, opened, stop, cost, source='signal',
                volume_experiment=None):
        self.queue.put(dict(id=f'{source}:{symbol}:{signal_at!r}', symbol=symbol,
            signal_at=signal_at, at=at, reason=str(reason), opened=bool(opened),
            stop=stop, cost=cost, source=source, volume_experiment=deepcopy(volume_experiment)))

    def watch_symbols(self, symbols):
        self.watch=tuple(symbols)

    def start(self):
        self.thread=threading.Thread(target=self.run,name='rocket-daily',daemon=True)
        self.thread.start()

    def run(self):
        db=sqlite3.connect(self.path,timeout=5)
        db.execute('PRAGMA journal_mode=WAL')
        model=DailyModel(db)
        self.stream.start()
        pending=deque()
        errors=0
        degraded=False
        try:
            while not self.stop.is_set():
                try:
                    while True:
                        try: pending.append(self.queue.get_nowait())
                        except Empty: break
                    for event in pending:
                        model.capture(event)
                    db.commit()
                    pending.clear()
                    active=sorted({s['symbol'] for s in model.active.values()})
                    symbols=tuple(dict.fromkeys((*active,*self.watch)))[:100]
                    self.stream.set_symbols(symbols)
                    quotes,overflow,gaps=self.stream.drain_quotes()
                    now=time.time()
                    model.tick(now,quotes,overflow or degraded,gaps)
                    degraded=False
                    db.executemany('INSERT OR REPLACE INTO health VALUES(?,?)',
                        [('last_tick',str(now)),('errors',str(errors)),('queued',str(len(pending))),
                         ('quote_version','2'),('stream',json.dumps(self.stream.health()))])
                    db.commit()
                except Exception as error:
                    db.rollback()
                    model.reload_active()
                    errors+=1
                    degraded=True
                    print('Rocket daily recorder: '+type(error).__name__,flush=True)
                self.stop.wait(.2)
        finally:
            self.stream.close()
            db.close()

    def close(self):
        self.stop.set()
        if self.thread: self.thread.join(timeout=6)


def source_path(db):
    return next((r[2] for r in db.execute('PRAGMA database_list') if r[1]=='main'), '')


def report_data(main, now):
    path=source_path(main)
    result=dict(version='rocket-daily-v2',since=now-86400,until=now,episodes=[],health={},actual={},
                volume_test=volume_shadow.report_data([],now))
    exists=lambda name: main.execute('SELECT 1 FROM sqlite_master WHERE name=?',(name,)).fetchone()
    positions=list(main.execute('SELECT id,symbol,signal_timestamp,opened_at,closed_at,status,realized_pnl_usdt '
                               "FROM paper_positions WHERE signal_kind LIKE '%лидер%'")) if exists('paper_positions') else []
    closed=[r for r in positions if r[4] is not None and now-86400<=r[4]<=now and r[5]=='CLOSED']
    result['actual']=dict(entries=sum(now-86400<=r[3]<=now for r in positions),closed=len(closed),
        profitable=sum(r[6]>0 for r in closed),losing=sum(r[6]<0 for r in closed),
        pnl=sum(r[6] for r in closed),open=sum(r[5]=='OPEN' for r in positions))
    if not path or not Path(path+SUFFIX).exists():
        return result
    db=sqlite3.connect(Path(path+SUFFIX).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
    try:
        result['health']=dict(db.execute('SELECT key,value FROM health'))
        rows=list(db.execute('SELECT payload FROM episodes WHERE at>=? AND at<=? ORDER BY at',(now-7*86400,now)))
    finally: db.close()
    bought={(r[1],r[2]):r[0] for r in positions}
    waits={(r[0],r[1]):r[2] for r in main.execute('SELECT symbol,signal_at,state FROM rocket_entry_waits ORDER BY id')} if exists('rocket_entry_waits') else {}
    volume_rows=[]
    for payload, in rows:
        s=json.loads(payload)
        if s['leg']['status'] in ('WAIT','OPEN') and now-float(result['health'].get('last_tick',0))>10:
            s['leg'].update(status='INCOMPLETE',reason='регистратор не обновляется')
        if s['leg']['status']=='INCOMPLETE' and s.get('volume_execution',{}).get('state')=='PENDING':
            s['volume_execution'].update(state='UNKNOWN',reason=s['leg'].get('reason'))
        if s.get('volume_experiment'):
            volume_rows.append(s)
        if s['at']<now-86400:
            continue
        key=(s['symbol'],s['signal_at'])
        position=bought.get(key) if s['source']=='signal' else None
        wait=waits.get(key) if s['source']=='signal' else None
        s['position_id']=position
        s['classification']=('BOUGHT' if position is not None or s['opened'] else
            'PENDING_DECISION' if s['source']=='signal' and wait is None and now-s['at']<120 else 'REJECTED')
        s['wait_outcome']=wait
        result['episodes'].append(s)
    result['volume_test']=volume_shadow.report_data(volume_rows,now)
    return result


def volume_report_text(main, now):
    return volume_shadow.report_text(report_data(main,now)['volume_test'])


def outcome(items):
    legs=[s['leg'] for s in items]
    closed=[l for l in legs if l['status']=='CLOSED']
    return dict(count=len(items),profitable=sum(l['net']>0 for l in closed),
        losing=sum(l['net']<0 for l in closed),flat=sum(l['net']==0 for l in closed),
        pnl=sum(l['net']*.5 for l in closed),marked=sum(l['status']=='MARKED' for l in legs),
        marked_pnl=sum(l['net']*.5 for l in legs if l['status']=='MARKED'),
        pending=sum(l['status'] in ('WAIT','OPEN') for l in legs),
        incomplete=sum(l['status']=='INCOMPLETE' for l in legs))


def report_text(main, now):
    d=report_data(main,now)
    stamp=lambda t:datetime.fromtimestamp(t,timezone.utc).strftime('%d.%m %H:%M')
    a=d['actual']; ep=d['episodes']
    rejected=[s for s in ep if s['classification']=='REJECTED']
    o=outcome(rejected)
    lines=['📊 Ракеты за 24 часа — сделки и отказы',f"{stamp(d['since'])} — {stamp(now)} UTC.",
        f"Фактически: входов {a['entries']}; закрыто {a['closed']}, прибыльных {a['profitable']}, убыточных {a['losing']}; итог {a['pnl']:+.3f} USDT; открыто {a['open']}.",
        f"Записано решений {len(ep)} по {len({s['symbol'] for s in ep})} монетам; отказов {len(rejected)}; ожидают решения {sum(s['classification']=='PENDING_DECISION' for s in ep)}.",
        f"Если купить отклонённые: прибыльных {o['profitable']}, убыточных {o['losing']}, нулевых {o['flat']}; закрытые {o['pnl']:+.3f} USDT.",
        f"На горизонте ещё открыты {o['marked']} (оценка {o['marked_pnl']:+.3f} USDT); наблюдаются {o['pending']}; неполных {o['incomplete']}."]
    # Group by first recorded rejection stage; descriptive, not causal filter ablation.
    for source,label in [('confirmation','Отказ за 20с'),('signal','Отказ после подтверждения')]:
        group=[s for s in rejected if s['source']==source]
        g=outcome(group)
        lines.append(f"• {label}: {g['count']}; плюс/минус {g['profitable']}/{g['losing']}; закрытые {g['pnl']:+.3f} USDT; неполных {g['incomplete']}.")
    lines.extend([f"Регистратор: ошибок {d['health'].get('errors','—')}. Новые записи с установки; старые отказы не пересчитаны.",
        'Каждый сигнал отдельно, без лимита одной монеты в час. Покупка — первая ask не позднее 5с после решения; выход — bid. По 50 USDT, стоп и комиссия фиксируются на сигнале; защита +1%, откат 1 п.п., горизонт 60 мин. Открытые на горизонте не считаются закрытыми прибыльными.',
        'Это независимые виртуальные опыты, не доходность банка. Глубина и проскальзывание не моделируются. Повторная покупка после ожидания связана с исходным сигналом. Закрытые фактические сделки — по времени выхода.'])
    v2=[s for s in rejected if s.get('quote_version')==2]
    if v2:
        g=outcome(v2)
        lines.insert(5, f"Новый сбор котировок v2: отказов {g['count']}; закрыто плюс/минус {g['profitable']}/{g['losing']}; неполных {g['incomplete']}; ещё наблюдаются {g['pending']}.")
    return '\n'.join(lines)
