"""Read-only trade diagnostics with an independent post-exit bid recorder."""
import json
import math
import sqlite3
import threading
import time
from pathlib import Path
from queue import SimpleQueue, Empty
from dataclasses import asdict, replace

from bot.rocket_stops import replay, STOPS
from bot.reporting import utc_stamp
from bot.rocket_quote_stream import RocketQuoteStream
from bot.recording_gaps import read_gaps
from bot.rocket_entry_variants import evaluate as evaluate_variants, report_text as variants_report

from bot.rocket_recovery_shadow import RecoveryShadow, report_text as recovery_report

WINDOWS = (5, 10, 20, 60)
MAX_GAP = 5


def schema(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS rocket_bid_path (
            symbol TEXT NOT NULL, timestamp REAL NOT NULL, bid REAL NOT NULL,
            PRIMARY KEY(symbol,timestamp));
        CREATE TABLE IF NOT EXISTS rocket_entry_probes (
            position_id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rocket_trade_cards (
            position_id INTEGER PRIMARY KEY, updated_at REAL NOT NULL, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rocket_card_deliveries (
            position_id INTEGER NOT NULL, minutes INTEGER NOT NULL, delivered_at REAL NOT NULL,
            PRIMARY KEY(position_id,minutes));
        CREATE TABLE IF NOT EXISTS rocket_path_meta (key TEXT PRIMARY KEY,value REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS rocket_path_gaps (started REAL NOT NULL, ended REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS rocket_symbol_gaps (symbol TEXT NOT NULL, started REAL NOT NULL, ended REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS rocket_symbol_gaps_lookup ON rocket_symbol_gaps(symbol,ended);
    ''')


def exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def entry_probe(market, signal, context, dynamics, started, probe_provider=None):
    callback = probe_provider if probe_provider is not None else market.__dict__.get('rocket_probe')
    if callback is None:
        return None
    calculation_started = time.perf_counter()
    probe = {}
    try:
        at=time.time()
        probe=callback(signal.symbol, at)
        snapshot=probe.pop('snapshot')
        probe.update(signal_at=started, allowed=None, reason='поток устарел или окно ещё не накоплено',
                     signal_price=getattr(signal,'price',None),
                     before_context=asdict(context), before_dynamics=asdict(dynamics))
        if probe.get('freshness_reasons'):
            probe['reason'] = '; '.join(probe['freshness_reasons'])
        if snapshot is not None and probe['fresh']:
            fresh_context=market.with_order_flow(context,snapshot)
            fresh_dynamics=replace(dynamics,change_15s_percent=probe['changes']['15'])
            probe['allowed'],probe['reason']=market.leader_entry_quality(fresh_context,fresh_dynamics)
            probe['after_flow']=asdict(snapshot)
    except Exception:
        probe = dict(at=time.time(),allowed=None,reason='ошибка получения свежего снимка')
    probe['entry_variants'] = evaluate_variants(probe)
    probe['calculation_ms'] = (time.perf_counter() - calculation_started) * 1000
    return probe


def shadow_summary(db):
    if not exists(db,'rocket_entry_probes'):
        return '🔎 Свежесть входа ракет — тень: новые снимки ещё не накоплены.'
    rows=db.execute("SELECT e.payload,p.status,p.realized_pnl_usdt FROM rocket_entry_probes e JOIN paper_positions p ON p.id=e.position_id").fetchall()
    known=[(json.loads(payload),pnl) for payload,status,pnl in rows if status=='CLOSED' and json.loads(payload).get('allowed') is not None]
    kept=[pnl for probe,pnl in known if probe['allowed']]
    skipped=[pnl for probe,pnl in known if not probe['allowed']]
    return ('🔎 Свежесть входа ракет — только тень\n'
            f'Снимков {len(rows)}; закрытых с известной оценкой {len(known)}.\n'
            f'A, фактические входы этих пар: {sum(p for _,p in known):+.3f} USDT.\n'
            f'B, только свежий импульс: входов {len(kept)}, результат {sum(kept):+.3f} USDT; пропущено {len(skipped)}.\n'
            f'Среди пропущенных прибыльных {sum(p>0 for p in skipped)}, убыточных {sum(p<0 for p in skipped)}.\n'
            'В обеих ветках одинаковые фактические выходы и издержки; отказ = без сделки. '
            'Нет оценки замещающих сделок и свободного банка. Неизвестные оценки исключены. На торговлю не влияет.'
            '\n\n' + variants_report(db) + '\n\n' + recovery_report(db))


def window_result(points, entry, exit_at, exit_price, minutes, now, cost):
    end = exit_at + minutes * 60
    if now < end:
        return dict(status='pending', remaining_seconds=end-now)
    after = [(t,p) for t,p in points if exit_at < t <= end]
    if not after:
        return dict(status='missing')
    path = [(exit_at,exit_price), *after]
    trough_at, trough = min(path, key=lambda x:x[1])
    high_at, high = max(path, key=lambda x:x[1])
    rebound = max(p for t,p in path if t >= trough_at)
    complete = end-path[-1][0] <= MAX_GAP and all(b[0]-a[0] <= MAX_GAP for a,b in zip(path,path[1:]))
    def returned(price):
        return next(((t-exit_at)/60 for t,p in after if p >= price), None)
    return dict(status='complete' if complete else 'incomplete',
                low_price=trough, low_from_entry=(trough/entry-1)*100,
                low_from_exit=(trough/exit_price-1)*100, low_at=trough_at,
                minutes_to_low=(trough_at-exit_at)/60,
                high_price=high, high_from_entry=(high/entry-1)*100, high_at=high_at,
                rebound_from_low=(rebound/trough-1)*100,
                end_price=after[-1][1], end_quote_at=after[-1][0],
                end_from_entry=(after[-1][1]/entry-1)*100,
                return_to_entry_minutes=returned(entry), return_to_exit_minutes=returned(exit_price),
                target07_minutes=returned(entry*1.007))


def build_card(db, row, now):
    row = dict(row)
    entry, opened, closed = row['entry_price'], row['opened_at'], row['closed_at']
    card = dict(position_id=row['id'], symbol=row['symbol'], opened_at=opened,
                entry_price=entry, closed_at=closed, reason=row['close_reason'],
                actual_pnl_usdt=row['realized_pnl_usdt'], windows={}, comparisons={})
    card['actual_net_percent']=row['realized_pnl_usdt']/row['position_usdt']*100 if row['position_usdt'] else None
    if exists(db,'rocket_entry_probes'):
        probe = db.execute('SELECT payload FROM rocket_entry_probes WHERE position_id=?',(row['id'],)).fetchone()
        card['entry_probe'] = json.loads(probe[0]) if probe else None
    if closed is None:
        card['status']='open'
        return card
    sell = db.execute("SELECT price FROM paper_fills WHERE position_id=? AND side='SELL' ORDER BY timestamp DESC,id DESC LIMIT 1",(row['id'],)).fetchone()
    if sell is None:
        card['status']='missing_exit'
        return card
    exit_price = float(sell[0])
    card.update(exit_price=exit_price, exit_change_percent=(exit_price/entry-1)*100,
                seconds_to_exit=closed-opened, status='observing' if now<closed+3600 else 'finished')
    points = []
    if exists(db,'rocket_bid_path'):
        points = [(float(t),float(p)) for t,p in db.execute(
            'SELECT timestamp,bid FROM rocket_bid_path WHERE symbol=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp',
            (row['symbol'],opened,min(now,closed+3600)))]
    card['source']='bid'
    recorder_start=db.execute("SELECT value FROM rocket_path_meta WHERE key='started'").fetchone() if exists(db,'rocket_path_meta') else None
    predates_recorder=recorder_start is not None and opened<float(recorder_start[0])
    if (not points or predates_recorder) and exists(db,'samples'):
        legacy = [(float(t),float(p)) for t,p in db.execute(
            'SELECT timestamp,price FROM samples WHERE symbol=? AND timestamp>? AND timestamp<=? ORDER BY timestamp',
            (row['symbol'],opened,min(now,closed+3600)))]
        if legacy:
            card['source']='mixed_legacy_bid' if points else 'legacy_last_trade'
            merged=dict(legacy)
            merged.update(points)  # Preserve recorded bid where both sources have the same time.
            points=sorted(merged.items())
    saved_cost=db.execute('SELECT cost_percent FROM rocket_stop_costs WHERE position_id=?',(row['id'],)).fetchone() if exists(db,'rocket_stop_costs') else None
    cost=float(saved_cost[0]) if saved_cost else None
    if cost is None:
        fills=db.execute("SELECT price,quantity,pnl_usdt FROM paper_fills WHERE position_id=? AND side='SELL'",(row['id'],)).fetchall()
        if len(fills)==1 and math.isclose(fills[0][1],row['initial_quantity'],rel_tol=1e-6):
            price,quantity,pnl=fills[0]
            cost=((price/entry-1)-pnl/(entry*quantity))*100
    if cost is not None and (not math.isfinite(cost) or cost < -1e-6): cost=None
    card['cost_percent']=cost
    gaps=read_gaps(db, row['symbol'], opened, closed+3600)
    card['recording_gaps']=[list(g) for g in gaps]
    card['minute_path']=[]
    for minute in range(1,min(60,max(0,int((now-closed)//60)))+1):
        bucket=[(t,p) for t,p in points if closed+(minute-1)*60 < t <= closed+minute*60]
        card['minute_path'].append(dict(minute=minute,count=len(bucket),
            low=min(p for t,p in bucket) if bucket else None,
            high=max(p for t,p in bucket) if bucket else None,
            last=bucket[-1][1] if bucket else None, last_at=bucket[-1][0] if bucket else None))
    for minutes in WINDOWS:
        result = window_result(points,entry,closed,exit_price,minutes,now,cost)
        interrupted=any(b>=closed and a<=closed+minutes*60 for a,b in gaps)
        if (card['source']!='bid' or interrupted) and result['status']=='complete':
            result['status']='incomplete'
        card['windows'][str(minutes)]=result
        end=closed+minutes*60
        # Keep actual exit as an anchor, but never bridge missing observations silently.
        raw = [(opened,entry), *((t,p) for t,p in points if t<=end)]
        complete = (card['source']=='bid' and now>=end and cost is not None and
                    len(raw)>1 and end-raw[-1][0]<=MAX_GAP and
                    all(b[0]-a[0]<=MAX_GAP for a,b in zip(raw,raw[1:])) and
                    not any(b>=opened and a<=end for a,b in gaps))
        if complete:
            anchored=dict(raw); anchored[closed]=exit_price
            card['comparisons'][str(minutes)]={str(s):replay(sorted(anchored.items()),entry,s,cost) for s in STOPS}
    return card


def format_card(card):
    lines=[f"📍 Ракета {card['symbol']} · сделка #{card['position_id']}",
           f"Вход: {utc_stamp(card['opened_at'])}, цена {card['entry_price']:g}."]
    if card['closed_at'] is None:
        return '\n'.join(lines+['Позиция ещё открыта.'])
    if 'exit_price' not in card:
        return '\n'.join(lines+['Запись цены выхода отсутствует.'])
    lines += [f"Выход: {utc_stamp(card['closed_at'])}, {card['exit_price']:g}; через {card['seconds_to_exit']:.1f} с.",
              f"Причина: {card['reason']}. PnL {card['actual_pnl_usdt']:+.3f} USDT.",
              'Окна ниже отсчитываются после фактического выхода.']
    for minutes,result in card['windows'].items():
        status=result['status']
        if status in ('pending','missing'):
            lines.append(f"• {minutes} мин: "+('наблюдение ещё идёт.' if status=='pending' else 'нет котировок.'))
            continue
        label='по полному записанному пути' if status=='complete' else 'НЕПОЛНЫЕ данные, экстремумы могут быть пропущены'
        recovered=result['return_to_entry_minutes']
        return_text=f"через {recovered:.1f} мин" if recovered is not None else 'не зафиксирован'
        lines.append(f"• {minutes} мин ({label}): минимум {result['low_price']:g} "
                     f"({result['low_from_entry']:+.2f}% от входа; {result['low_from_exit']:+.2f}% от выхода) "
                     f"через {result['minutes_to_low']:.1f} мин; максимум {result['high_from_entry']:+.2f}% от входа; "
                     f"отскок после дна {result['rebound_from_low']:+.2f}%; в конце {result['end_from_entry']:+.2f}%. "
                     f"Возврат к входу: {return_text}.")
    for minutes in ('20','60'):
        legs=card['comparisons'].get(minutes)
        if legs:
            lines.append(f"Стопы от момента покупки до выхода + {minutes} мин; по 50 USDT:")
            for stop,leg in legs.items():
                reason={'STOP':'стоп','TRAIL':'защита прибыли','OPEN':'открыта, оценка'}[leg['reason']]
                lines.append(f"  −{float(stop):g}% → {leg['pnl']:+.3f} USDT ({reason}).")
        else:
            lines.append(f"Пересчёт стопов +{minutes} мин: полного пути от покупки пока нет; вывод не делаем.")
    probe=card.get('entry_probe')
    if probe:
        status={True:'пропустила бы',False:'отклонила бы',None:'нет свежих данных'}[probe.get('allowed')]
        lines.append(f"Теневая свежесть перед входом: {status}. {probe.get('reason') or ''}")
    lines.append('Только аналитика; стопы и сделки не меняются. История bid не моделирует глубину и проскальзывание.')
    return '\n'.join(lines)


def cards(db, now, limit=20):
    if not exists(db,'paper_positions'):
        return []
    result=[]
    cursor=db.execute("SELECT * FROM paper_positions WHERE signal_kind LIKE '%лидер%' ORDER BY id DESC LIMIT ?",(limit,))
    names=[c[0] for c in cursor.description]
    for values in cursor.fetchall():
        row=dict(zip(names,values))
        saved=db.execute('SELECT payload FROM rocket_trade_cards WHERE position_id=?',(row['id'],)).fetchone() if exists(db,'rocket_trade_cards') else None
        if saved:
            card=json.loads(saved[0])
            if card.get('status')!='finished': card=build_card(db,row,now)
        else: card=build_card(db,row,now)
        result.append(card)
    return result


class RecordedBidStream(RocketQuoteStream):
    """Diagnostic bids with real depth heartbeats and in-place subscriptions."""
    max_symbols = 128

    def drain_recording_batch(self):
        quotes, overflow, gaps = self.drain_quotes()
        return [(at, symbol, bid) for at, symbol, bid, _ask in quotes], overflow, gaps


class RetainedBidBatch:
    """A drained batch is acknowledged only after SQLite commits it."""
    LIMIT = 100000

    def __init__(self):
        self.events = []
        self.gaps = []
        self.symbol_gaps = []
        self.last = {}

    def append(self, events, overflow, previous, now, interruptions=()):
        self.events.extend(events)
        if interruptions:
            self.symbol_gaps.extend((symbol, at, max(at, now)) for at, symbol in interruptions)
        if overflow or len(self.events) > self.LIMIT:
            starts = [previous] + [e[0] for e in self.events]
            self.gaps.append((min(starts), now))
            self.events.clear()

    def write(self, db):
        # Do not mutate deduplication state until the transaction succeeds.
        last = dict(self.last)
        values = []
        for at, symbol, bid in self.events:
            previous = last.get(symbol)
            if previous is None or bid != previous[1] or at-previous[0] >= 1:
                values.append((symbol, at, bid))
                last[symbol] = (at, bid)
        db.executemany('INSERT OR IGNORE INTO rocket_bid_path VALUES(?,?,?)', values)
        db.executemany('INSERT INTO rocket_path_gaps VALUES(?,?)', self.gaps)
        db.executemany('INSERT INTO rocket_symbol_gaps VALUES(?,?,?)', self.symbol_gaps)
        db.commit()
        self.last = last
        self.events.clear()
        self.gaps.clear()
        self.symbol_gaps.clear()


class RocketPathWorker:
    """Never calls trading methods; only writes dedicated diagnostic tables."""
    def __init__(self, database, stream=None, recovery_probe=None, recovery_stop=.5):
        self.database=database
        self.recovery_probe,self.recovery_stop=recovery_probe,recovery_stop
        self.stream=stream or RecordedBidStream()
        self._stop=threading.Event()
        self._queue=SimpleQueue()
        self._thread=None
        self.notifications=SimpleQueue()
        self._acks=SimpleQueue()
        self._watch=()

    def watch_symbols(self, symbols):
        # Prewarm before entry. Actual open/recent positions retain first priority.
        self._watch=tuple(dict.fromkeys(symbols))[:128]

    def acknowledge(self, position_id, minutes):
        self._acks.put((position_id,minutes))

    def record_shadow(self, position_id, probe):
        self._queue.put((position_id,probe))

    def start(self):
        self.stream.start()
        self._thread=threading.Thread(target=self._run,name='rocket-path-recorder',daemon=True)
        self._thread.start()

    def _run(self):
        db=sqlite3.connect(self.database,timeout=5)
        db.row_factory=sqlite3.Row
        schema(db)
        db.execute("""CREATE TABLE IF NOT EXISTS rocket_recorder_health (
            id INTEGER PRIMARY KEY CHECK(id=1), started REAL, last_success REAL,
            retry_errors INTEGER, overflow_batches INTEGER, last_error_type TEXT,
            pending_quotes INTEGER)""")
        health_started = time.time()
        retry_errors = overflow_batches = 0
        last_error_type = None
        recovery=RecoveryShadow(db,self.recovery_probe,self.recovery_stop)
        db.execute("INSERT OR IGNORE INTO rocket_path_meta VALUES('started',?)",(time.time(),))
        started=db.execute("SELECT value FROM rocket_path_meta WHERE key='started'").fetchone()[0]
        db.commit()
        last_refresh=last_cards=0
        batch=RetainedBidBatch()
        enqueued=set()
        previous_drain=time.time()
        pending_probes=[]
        pending_acks=[]
        try:
            while not self._stop.wait(.2):
                try:
                    now=time.time()
                    if now-last_refresh>=1:
                        rows=db.execute("SELECT symbol FROM paper_positions WHERE signal_kind LIKE '%лидер%' AND (status='OPEN' OR closed_at>=?) ORDER BY id DESC",(now-3605,)).fetchall()
                        self.stream.set_symbols(tuple(dict.fromkeys((*[r[0] for r in rows], *self._watch)))[:128])
                        last_refresh=now
                    if isinstance(self.stream, RecordedBidStream):
                        events,overflow,interruptions=self.stream.drain_recording_batch()
                    else:
                        events,overflow=self.stream.drain_batch()
                        interruptions=()
                    batch.append(events, overflow, previous_drain, now, interruptions)
                    overflow_batches += int(overflow)
                    batch.write(db)
                    previous_drain=now
                    # Reporting/recovery failures must not roll back recorded bids.
                    db.execute('INSERT OR REPLACE INTO rocket_recorder_health VALUES(1,?,?,?,?,?,?)',
                        (health_started, now, retry_errors, overflow_batches, last_error_type, len(batch.events)))
                    while True:
                        try: position_id,probe=self._queue.get_nowait()
                        except Empty: break
                        pending_probes.append((position_id,json.dumps(probe)))
                    db.executemany('INSERT OR REPLACE INTO rocket_entry_probes VALUES(?,?)',pending_probes)
                    for position_id,payload in pending_probes:
                        probe=json.loads(payload)
                        recovery.seed(position_id,probe)
                        if probe.get('entry_bid') is not None and probe.get('entry_quote_at') is not None:
                            symbol=db.execute('SELECT symbol FROM paper_positions WHERE id=?',(position_id,)).fetchone()
                            if symbol:
                                db.execute('INSERT OR IGNORE INTO rocket_bid_path VALUES(?,?,?)',
                                           (symbol[0],probe['entry_quote_at'],probe['entry_bid']))
                    while True:
                        try: position_id,minutes=self._acks.get_nowait()
                        except Empty: break
                        pending_acks.append((position_id,minutes,now))
                    db.executemany('INSERT OR REPLACE INTO rocket_card_deliveries VALUES(?,?,?)',pending_acks)
                    recovery.tick(now)
                    db.commit()
                    pending_probes.clear(); pending_acks.clear()
                    if now-last_cards>=60:
                        for card in cards(db,now,100):
                            db.execute('INSERT OR REPLACE INTO rocket_trade_cards VALUES(?,?,?)',(card['position_id'],now,json.dumps(card)))
                            for minutes in (20,60):
                                key=(card['position_id'],minutes)
                                if card['opened_at'] < started or card['closed_at'] is None or now < card['closed_at']+minutes*60:
                                    continue
                                delivered=db.execute('SELECT 1 FROM rocket_card_deliveries WHERE position_id=? AND minutes=?',key).fetchone()
                                if not delivered and key not in enqueued:
                                    self.notifications.put((key, f'Наблюдение после выхода: {minutes} минут\n'+format_card(card)))
                                    enqueued.add(key)
                        db.execute('DELETE FROM rocket_bid_path WHERE timestamp<?',(now-8*86400,))
                        db.commit(); last_cards=now
                except Exception as error:
                    db.rollback()
                    retry_errors += 1
                    last_error_type = type(error).__name__
                    print('Rocket diagnostics: '+type(error).__name__,flush=True)
                    self._stop.wait(1)
        finally:
            db.close()

    def close(self):
        self._stop.set()
        if self._thread: self._thread.join(timeout=2)
        self.stream.close()
