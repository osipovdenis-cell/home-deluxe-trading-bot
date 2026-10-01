"""Immutable entry snapshots and read-only adapters; never infer missing features."""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from bot.idea_rules import FEATURES, finite


def snapshot(signal, context, dynamics, at, market=None):
    values={k:None for k in FEATURES}
    for obj in (context,dynamics):
        for key in values:
            v=getattr(obj,key,None)
            if finite(v): values[key]=v
    d=datetime.fromtimestamp(at,timezone.utc)
    values.update(utc_hour=d.hour,utc_weekday=d.weekday(),
                  signal_type=1 if 'аномаль' in getattr(signal,'kind','') else 0)
    v=getattr(signal,'change_24h_percent',None)
    if finite(v):values['change_24h_percent']=v
    if market is not None:
        values['confirmation_accepted']=market.__dict__.get('_idea_confirmation')
        values['change_12h_percent']=market.__dict__.get('change_12h_percent',{}).get(signal.symbol)
        for symbol,period,key in ((signal.symbol,5,'change_5s_percent'),('BTCUSDT',3600,'btc_change_3600s_percent')):
            points=[(t,p) for t,p in market.__dict__.get('_idea_history',market.__dict__.get('history',{})).get(symbol,()) if t<=at]
            base=next(((t,p) for t,p in reversed(points) if t<=at-period),None)
            if base and points and at-points[-1][0]<=5 and at-period-base[0]<=5 and base[1]>0:
                values[key]=(points[-1][1]/base[1]-1)*100
        points=[(t,p) for t,p in market.__dict__.get('_idea_anomaly_history',market.__dict__.get('anomaly_history',{})).get(signal.symbol,()) if at-600<=t<=at]
        if points and points[0][0]<=at-595 and at-points[-1][0]<=5 and min(p for _,p in points)>0:
            values['growth_10m_from_low_percent']=(points[-1][1]/min(p for _,p in points)-1)*100
    return {k:v if finite(v) else None for k,v in values.items()}


@contextmanager
def readonly(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=.2)
    db.row_factory=sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def exists(db,table):
    return bool(db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(table,)).fetchone())


def collect(path, journal, now):
    """Archive all observed records; never depend on the 7-day report window."""
    from bot.idea_store import enqueue
    with readonly(path) as db:
        if not exists(db,'paper_positions'):return
        for row in db.execute("SELECT * FROM paper_positions WHERE status='CLOSED' AND signal_kind LIKE '%лидер%'"):
            p=dict(row);ident=p['id'];key=f'trade:{ident}'
            if journal.execute('SELECT 1 FROM reviews WHERE trade_id=?',(ident,)).fetchone():continue
            # Wait for the existing path recorder to finish the extra hour.
            if now<p['closed_at']+3660:continue
            found=db.execute('SELECT payload FROM rocket_trade_cards WHERE position_id=?',(ident,)).fetchone() if exists(db,'rocket_trade_cards') else None
            card=json.loads(found[0]) if found else {}
            probe=card.get('entry_probe') or {}
            if not probe and exists(db,'rocket_entry_probes'):
                found=db.execute('SELECT payload FROM rocket_entry_probes WHERE position_id=?',(ident,)).fetchone()
                probe=json.loads(found[0]) if found else {}
            stamp=p.get('signal_timestamp') or p['opened_at']
            features={k:None for k in FEATURES}
            frozen=probe.get('idea_features')
            if frozen: features.update({k:v for k,v in frozen.items() if k in FEATURES and finite(v)})
            else:
                # Exact historical signal link only; no nearby/future confirmation joins.
                if exists(db,'signal_events'):
                    r=db.execute('SELECT * FROM signal_events WHERE symbol=? AND timestamp=? ORDER BY id DESC LIMIT 1',(p['symbol'],stamp)).fetchone()
                    if r:
                        r=dict(r);features.update({k:v for k,v in r.items() if k in FEATURES and finite(v)})
                for name in ('before_context','before_dynamics'):
                    features.update({k:v for k,v in (probe.get(name) or {}).items() if k in FEATURES and finite(v)})
                d=datetime.fromtimestamp(stamp,timezone.utc)
                features.update(utc_hour=d.hour,utc_weekday=d.weekday(),signal_type=1 if 'аномаль' in p['signal_kind'] else 0)
            prior=db.execute('SELECT COUNT(*) FROM paper_positions WHERE symbol=? AND opened_at>=? AND opened_at<?',(p['symbol'],stamp-86400,stamp)).fetchone()[0]
            pnl=db.execute("SELECT SUM(realized_pnl_usdt) FROM paper_positions WHERE symbol=? AND closed_at>=? AND closed_at<? AND status='CLOSED'",(p['symbol'],stamp-86400,stamp)).fetchone()[0]
            features.update(prior_entries_24h=prior,prior_closed_pnl_24h=pnl or 0.)
            if probe.get('resumed_without_ai'):
                features['filter_c_passed']=1
                features['ai_score']=p.get('ai_score')
            policy=json.loads(p['exit_policy_json']) if p.get('exit_policy_json') else {'version':'legacy-unversioned'}
            identities=[key,f"signal:{p['symbol']}:{stamp!r}"]
            event=dict(key=key,identity_keys=identities,symbol=p['symbol'],at=stamp,features=features,
                complete=True,net_percent=p['realized_pnl_usdt']/p['position_usdt']*100,
                entry_ask=p['entry_price'] if probe.get('entry_quote_at') is not None else None,
                policy=policy,cost=policy.get('cost_percent'),minutes=card.get('lifetime_minute_path',[]),
                path_complete=card.get('path_summary',{}).get('status')=='complete_observed',source='trade')
            payload=dict(trade_id=ident,identity_keys=identities,entry=dict(symbol=p['symbol'],at=stamp,features=features),
                strategy=policy,after_entry=dict(card=card,closed_at=p['closed_at'],reason=p['close_reason'],
                    pnl_usdt=p['realized_pnl_usdt'],holding_seconds=p['closed_at']-p['opened_at']),
                missing_features=[k for k,v in features.items() if v is None])
            with journal:
                journal.execute('INSERT OR REPLACE INTO events VALUES(?,?)',(key,json.dumps(event,allow_nan=False)))
                enqueue(journal,ident,payload,now)
    daily=Path(path+'.rocket_daily.sqlite3')
    if daily.exists():
        accepted_keys=set()
        for r in journal.execute("SELECT payload FROM events WHERE key LIKE 'trade:%'"):
            accepted_keys.update(json.loads(r[0])['identity_keys'])
        with readonly(daily) as db, journal:
            for ident,payload in db.execute('SELECT id,payload FROM episodes'):
                s=json.loads(payload);leg=s['leg']
                if ident in accepted_keys:
                    journal.execute('DELETE FROM events WHERE key=?',(ident,))
                    continue
                if s.get('opened') or leg['status'] in ('WAIT','OPEN'):continue
                e=dict(key=ident,identity_keys=[ident],symbol=s['symbol'],at=s['at'],source='rejection',
                    features=s.get('idea_features') or {},complete=leg['status']=='CLOSED',net_percent=leg.get('net'),
                    policy=dict(version='rejection-legacy-shadow-v1',stop_percent=s['stop'],protect_percent=1.,trail_pp=1.),
                    cost=s['cost'],entry_ask=leg.get('entry'),minutes=s.get('idea_minutes',[]),
                    path_complete=leg['status']=='CLOSED' and bool(s.get('idea_minutes')),
                    quotes=s.get('idea_quotes',[]))
                journal.execute('INSERT OR REPLACE INTO events VALUES(?,?)',(ident,json.dumps(e,allow_nan=False)))
