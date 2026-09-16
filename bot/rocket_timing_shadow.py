"""Independent prospective rocket entry timing experiment. No order/account access."""
import json
import math
import sqlite3
import threading
import time
from collections import deque
from dataclasses import asdict, replace
from queue import Queue, Empty, Full
from statistics import median

from bot.streams import LeaderOrderFlowStream
from bot.rocket_entry_guard import fading_buy_guard
from bot.rocket_entry_wait import RocketEntryWaitWorker
from bot.rocket_recovery_shadow import new_leg, advance

VERSION = 'rocket-timing-v1'
WAIT, HORIZON = 90, 3600


def schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS rocket_timing_pairs(
        id INTEGER PRIMARY KEY, version TEXT NOT NULL, symbol TEXT NOT NULL,
        started REAL NOT NULL, finished REAL, payload TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS rocket_timing_active ON rocket_timing_pairs(finished,symbol)')
    db.execute('CREATE TABLE IF NOT EXISTS rocket_timing_health(key TEXT PRIMARY KEY,value REAL NOT NULL)')


def strengthened(probe):
    w = probe.get('recovery_windows') or {}
    windows = w.get('windows') or []
    if not w.get('complete') or len(windows) != 2:
        return None
    previous, recent = windows
    return (recent['buy'] > previous['buy'] and recent['buy'] > recent['sell']
            and w['bid'] > w['bid_5s'])


class TimingModel:
    def __init__(self, db):
        self.db = db
        schema(db)

    def begin(self, symbol, now, signal_at, signal_price, stop, cost):
        if not all(math.isfinite(x) for x in (now, signal_at, signal_price, stop, cost)) or min(signal_price, stop) <= 0 or cost < 0:
            return None
        if self.db.execute('SELECT 1 FROM rocket_timing_pairs WHERE version=? AND symbol=? AND started>?',
                           (VERSION, symbol, now-HORIZON)).fetchone():
            return None
        s = dict(symbol=symbol, signal_at=signal_at, start=now, signal_price=signal_price,
                 stop=stop, cost=cost, approved=None, last_check=None, snapshots=[],
                 A=dict(status='PENDING',net=None), B=dict(status='PENDING',net=None))
        if len(self.states()) >= 20:
            for k in ('A','B'):
                s[k].update(status='INCOMPLETE',reason='лимит 20 наблюдений')
        ident = self.db.execute('INSERT INTO rocket_timing_pairs(version,symbol,started,payload) VALUES(?,?,?,?)',
                               (VERSION,symbol,now,json.dumps(s))).lastrowid
        self.save(ident,s,now)
        self.db.commit()
        return ident

    def save(self, ident, s, now):
        done = all(s[k]['status'] in ('CLOSED','MARKED','NO_ENTRY','INCOMPLETE') for k in ('A','B'))
        self.db.execute('UPDATE rocket_timing_pairs SET payload=?,finished=? WHERE id=?',
                        (json.dumps(s),now if done else None,ident))

    def states(self):
        return [(i,json.loads(p)) for i,p in self.db.execute(
            'SELECT id,payload FROM rocket_timing_pairs WHERE version=? AND finished IS NULL',(VERSION,))]

    def approve(self, ident, now):
        for i,s in self.states():
            if i == ident and s['approved'] is None:
                s.update(approved=now,last_check=now,approval_delay=now-s['signal_at'])
                for k in ('A','B'):
                    s[k] = dict(status='WAIT',net=None)
                self.save(i,s,now)

    def decision(self, ident, now, reason, opened):
        for i,s in self.states():
            if i != ident:
                continue
            s.update(actual_decision_at=now,immediate_trade_opened=bool(opened),actual_reason=reason)
            if s['approved'] is None:
                for k in ('A','B'):
                    s[k].update(status='NO_ENTRY',reason='общие проверки: '+reason)
            self.save(i,s,now)

    def tick(self, now, quotes, probes, overflow=False):
        for ident,s in self.states():
            approved = s['approved']
            if approved is None:
                p = probes.get(s['symbol']) or {}
                if p and (not s['snapshots'] or now-s['snapshots'][-1]['at']>=1):
                    s['snapshots'].append(dict(at=now,stage='до допуска',fresh=p.get('fresh'),
                        changes=p.get('changes'),windows=p.get('recovery_windows'),
                        trade_age=p.get('trade_age'),quote_age=p.get('quote_age')))
                    s['snapshots']=s['snapshots'][-60:]
                if now-s['start'] > 300:
                    for k in ('A','B'):
                        s[k].update(status='INCOMPLETE',reason='нет завершения решения за 300с')
                self.save(ident,s,now)
                continue
            end = approved+HORIZON
            # Replay every received bid in order BEFORE evaluating a new entry.
            for k in ('A','B'):
                if overflow and s[k]['status'] in ('WAIT','OPEN'):
                    s[k].update(status='INCOMPLETE',reason='переполнение буфера котировок')
                advance(s[k],quotes.get(s['symbol'],()),now,end,s['stop'],s['cost'])
            waiting = [k for k in ('A','B') if s[k]['status']=='WAIT']
            if waiting:
                p = probes.get(s['symbol']) or {}
                w = p.get('recovery_windows') or {}
                values = [w.get(k) for k in ('quote_at','bid','ask')]
                valid = (p.get('fresh') is True and w.get('complete') is True
                         and all(isinstance(v,(int,float)) and math.isfinite(v) for v in values)
                         and 0 <= now-values[0] <= 2 and values[2] >= values[1] > 0
                         and abs(p.get('at',0)-now) < 0.5)
                if now-s['last_check']>2 or not valid:
                    for k in waiting:
                        s[k].update(status='INCOMPLETE',reason='неполные данные ожидания/потока')
                elif now >= approved+WAIT:
                    for k in waiting:
                        s[k].update(status='NO_ENTRY',reason='90с без восстановления')
                else:
                    ask,bid = w['ask'],w['bid']
                    execution = ((ask/bid-1)*100 <= .25
                                 and abs((ask/s['signal_price']-1)*100) < s['stop'])
                    common = p.get('growth_12h') is True and execution
                    first = s.get('checks',0)==0
                    current = fading_buy_guard(p)[0] if first else RocketEntryWaitWorker.recovered(p)
                    for k in waiting:
                        allowed = common and current and (k=='A' or strengthened(p) is True)
                        if allowed:
                            s[k] = new_leg(now,ask,bid)
                            s[k]['entry_probe'] = {key:p.get(key) for key in
                                ('at','trade_age','quote_age','changes','after_flow','recovery_windows','allowed','reason')}
                    s['checks'] = s.get('checks',0)+1
                    # At most one diagnostic snapshot per second, no raw account data.
                    if not s['snapshots'] or now-s['snapshots'][-1]['at']>=1:
                        s['snapshots'].append(dict(at=now,changes=p.get('changes'),
                            windows=w,allowed=p.get('allowed'),reason=p.get('reason')))
                s['last_check'] = now
            self.save(ident,s,now)
        self.db.commit()


class TimingFlowStream(LeaderOrderFlowStream):
    def __init__(self):
        super().__init__(max_symbols=40)
        self.events = deque(maxlen=100000)
        self.events_lock = threading.Lock()
        self.overflow = False

    def ingest(self, payload, received_at=None):
        now = time.time() if received_at is None else received_at
        super().ingest(payload, now)
        item = json.loads(payload) if isinstance(payload,str) else payload
        item = item.get('data',item)
        if 'b' not in item or 'a' not in item or item.get('e')=='depthUpdate':
            return
        symbol = item.get('s')
        bid,ask = float(item['b']),float(item['a'])
        if not all(math.isfinite(v) and v>0 for v in (bid,ask)) or ask<bid:
            return
        with self.events_lock:
            if len(self.events)==self.events.maxlen:
                self.overflow=True
            self.events.append((now,symbol,bid))

    def drain(self):
        with self.events_lock:
            events,overflow = list(self.events),self.overflow
            self.events.clear()
            self.overflow=False
        return events,overflow


class TimingWorker:
    def __init__(self, path, market):
        self.path,self.market = path,market
        self.stream = TimingFlowStream()
        self.commands = Queue(maxsize=200)
        self.watch = ()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.dropped = 0

    def send(self, action, *args):
        try:
            self.commands.put_nowait((action,args))
        except Full:
            self.dropped += 1

    def watch_symbols(self, symbols):
        with self.lock:
            self.watch=tuple(symbols)

    def start(self):
        self.stream.start()
        self.thread=threading.Thread(target=self.run,name='rocket-timing-shadow',daemon=True)
        self.thread.start()

    def run(self):
        db=sqlite3.connect(self.path,timeout=1)
        model=TimingModel(db)
        # No historical recovery after restart: missing decision windows are unknown.
        for ident,s in model.states():
            for k in ('A','B'):
                if s[k]['status'] in ('PENDING','WAIT','OPEN'):
                    s[k].update(status='INCOMPLETE',reason='перезапуск наблюдателя')
            model.save(ident,s,time.time())
        db.commit()
        jobs={}
        errors=0
        recorder_failed=False
        try:
            while not self.stop.is_set():
                try:
                    # Keep diagnostic DB errors isolated from the trading threads.
                    while True:
                        try:
                            action,args=self.commands.get_nowait()
                        except Empty:
                            break
                        token=args[0]
                        if action=='begin':
                            _,signal,signal_at,received,stop,cost=args
                            ident=model.begin(signal.symbol,received,signal_at,signal.price,stop,cost)
                            jobs[token]=(ident,signal,None,None)
                        elif token in jobs:
                            ident,signal,context,dynamics=jobs[token]
                            if ident is None:
                                continue
                            if action=='approve':
                                _,at,context,dynamics=args
                                jobs[token]=(ident,signal,context,dynamics)
                                model.approve(ident,at)
                            elif action=='decision':
                                model.decision(ident,*args[1:])
                    if recorder_failed:
                        for ident,s in model.states():
                            for k in ('A','B'):
                                if s[k]['status'] in ('PENDING','WAIT','OPEN'):
                                    s[k].update(status='INCOMPLETE',reason='ошибка независимого регистратора')
                            model.save(ident,s,time.time())
                        db.commit()
                        recorder_failed=False
                    states=model.states()
                    active={s['symbol'] for _,s in states}
                    active_ids={i for i,_ in states}
                    jobs={k:v for k,v in jobs.items() if v[0] in active_ids}
                    with self.lock:
                        watch=self.watch
                    self.stream.set_symbols((*sorted(active),*watch))
                    events,overflow=self.stream.drain()
                    quotes={}
                    for at,symbol,bid in events:
                        quotes.setdefault(symbol,[]).append((at,bid))
                    now=time.time()
                    probes={}
                    for ident,signal,context,dynamics in jobs.values():
                        p=self.stream.entry_probe(signal.symbol,now)
                        snapshot=p.pop('snapshot')
                        if context is None:
                            probes[signal.symbol]=p
                            continue
                        p.update(before_context=asdict(context),allowed=False,reason='поток не готов',
                                 growth_12h=self.market.change_12h_percent.get(signal.symbol,0)>0)
                        if snapshot is not None and p['fresh']:
                            fresh_context=self.market.with_order_flow(context,snapshot)
                            fresh_dynamics=replace(dynamics,change_15s_percent=p['changes']['15'])
                            p['allowed'],p['reason']=self.market.leader_entry_quality(fresh_context,fresh_dynamics)
                            p['after_flow']=asdict(snapshot)
                        probes[signal.symbol]=p
                    model.tick(now,quotes,probes,overflow)
                    for key,value in (('last_tick',now),('errors',errors),('dropped_commands',self.dropped)):
                        db.execute('INSERT OR REPLACE INTO rocket_timing_health VALUES(?,?)',(key,value))
                    db.commit()
                except Exception as error:
                    db.rollback()
                    errors+=1
                    recorder_failed=True
                    # A failed drain/transaction can hide an extremum: invalidate,
                    # rather than silently treating a later price as a full path.
                    try:
                        for ident,s in model.states():
                            for k in ('A','B'):
                                if s[k]['status'] in ('PENDING','WAIT','OPEN'):
                                    s[k].update(status='INCOMPLETE',reason='ошибка независимого регистратора')
                            model.save(ident,s,time.time())
                        db.commit()
                    except sqlite3.Error:
                        db.rollback()
                    print('Rocket timing shadow: '+type(error).__name__,flush=True)
                self.stop.wait(.2)
        finally:
            db.close()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=5)
        self.stream.close()


def report_data(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='rocket_timing_pairs'").fetchone():
        return dict(version=VERSION,pairs=[],health={})
    rows=db.execute('SELECT id,finished,payload FROM rocket_timing_pairs WHERE version=? ORDER BY id',(VERSION,)).fetchall()
    return dict(version=VERSION,pairs=[dict(id=i,finished=f,**json.loads(p)) for i,f,p in rows],
                health=dict(db.execute('SELECT key,value FROM rocket_timing_health')))


def report_text(db):
    d=report_data(db)
    pairs=d['pairs']
    bad=[s for s in pairs if any(s[k]['status']=='INCOMPLETE' for k in ('A','B'))]
    eligible=[s for s in pairs if s['approved'] is not None and s['finished'] is not None and s not in bad]
    lines=['⏱ Момент входа ракет — независимая тень',
        f"Версия {VERSION}: кандидатов {len(pairs)}, полных допущенных пар {len(eligible)}, неполных {len(bad)}; наблюдаются {sum(s['finished'] is None for s in pairs)}.",
        f"Общие проверки отклонили {sum(s['approved'] is None and s['finished'] is not None and s not in bad for s in pairs)}; в обеих ветках без входа.",
        'A: текущая финальная проверка и короткое ожидание. B: дополнительно покупки 5с > предыдущих 5с и продаж, bid выше 5с назад. Ожидание до 90с без нового AI/20с.']
    delays=[s['approval_delay'] for s in pairs if 'approval_delay' in s]
    if delays:
        lines.append(f'Сигнал → допуск: медиана {median(delays):.2f}с; максимум {max(delays):.2f}с, n={len(delays)}.')
    health=d['health']
    lines.append(f"Ошибок регистратора {int(health.get('errors',0))}; потеряно команд {int(health.get('dropped_commands',0))}.")
    totals={}
    for k in ('A','B'):
        closed=[s[k] for s in eligible if s[k]['status']=='CLOSED']
        marked=[s[k] for s in eligible if s[k]['status']=='MARKED']
        pnl=sum(l['net']*.5 for l in closed)
        mark=sum(l['net']*.5 for l in marked)
        totals[k]=pnl+mark
        lines.append(f"• {k}: закрыто {len(closed)}, прибыльных {sum(l['net']>0 for l in closed)}; PnL {pnl:+.3f} USDT; открыто на горизонте {len(marked)} на {mark:+.3f}; без входа {sum(s[k]['status']=='NO_ENTRY' for s in eligible)}.")
    skipped=[s for s in eligible if s['B']['status']=='NO_ENTRY' and s['A']['status']=='CLOSED']
    lines.extend([f"Разница B−A с оценкой открытых: {totals['B']-totals['A']:+.3f} USDT.",
        f"B пропустил прибыльных A: {sum(s['A']['net']>0 for s in skipped)} на +{sum(max(0,s['A']['net'])*.5 for s in skipped):.3f}; убыточных: {sum(s['A']['net']<0 for s in skipped)} на −{-sum(min(0,s['A']['net'])*.5 for s in skipped):.3f} USDT.",
        'Отдельный поток сделок и bid/ask, проверка каждые 0,2с. Пропуск проверки >2с, bid >5с или неполные окна исключают пару.',
        'По 50 USDT; фактические bid/ask, комиссии и стоп фиксируются на старте; защита +1%, откат 1 п.п.; горизонт 60 мин от допуска.',
        'Только новые кандидаты, дошедшие до обработки сигнала; одна пара на монету в час. Не все лидеры Binance. Лимиты банка, глубина и проскальзывание не моделируются. A — модель правил, не фактическая сделка. На торговлю не влияет.'])
    return '\n'.join(lines)
