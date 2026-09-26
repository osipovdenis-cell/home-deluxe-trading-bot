"""Prospective market-regime comparison; never authorizes or rejects an order."""
import json
import math

VERSION = 'rocket-market-regime-v1'

def finite(value):
    return isinstance(value, (int,float)) and not isinstance(value,bool) and math.isfinite(value)

def change(points, now, seconds):
    rows = [(at,p) for at,p in points if at <= now]
    if not rows or rows[0][0] > now-seconds or not 0 <= now-rows[-1][0] <= 3:
        return None
    before = [r for r in rows if r[0] <= now-seconds]
    if not before or now-seconds-before[-1][0] > 3:
        return None
    rows = [before[-1]]+[r for r in rows if r[0] > now-seconds]
    if any(b[0]-a[0]>5 for a,b in zip(rows,rows[1:])) or any(not finite(p) or p<=0 for _,p in rows):
        return None
    return (rows[-1][1]/rows[0][1]-1)*100

def snapshot(history, symbols, now):
    symbols = tuple(symbols)
    btc = change(history.get('BTCUSDT',()),now,300)
    changes = [change(history.get(s,()),now,60) for s in symbols]
    known = [v for v in changes if v is not None]
    coverage = len(known)/len(symbols) if symbols else 0
    breadth = 100*sum(v>0 for v in known)/len(known) if known else None
    valid = btc is not None and len(known)>=10 and coverage>=.8
    return dict(at=now, state='KNOWN' if valid else 'UNKNOWN', btc_300=btc,
                breadth_60=breadth, known_symbols=len(known), universe_symbols=len(symbols), coverage=coverage)

def freeze(snapshot, at):
    row = dict(snapshot) if isinstance(snapshot,dict) else {}
    valid = (row.get('state')=='KNOWN' and finite(row.get('at'))
             and 0<=at-row['at']<=10 and finite(row.get('btc_300'))
             and finite(row.get('breadth_60')) and 0<=row['breadth_60']<=100)
    return dict(version=VERSION, evaluated_at=at, source=row,
                state='KNOWN' if valid else 'UNKNOWN',
                passed=not(row['btc_300']<0 and row['breadth_60']<50) if valid else None)

def report_data(db):
    result = dict(version=VERSION,recorded=0,unknown=0,open=0,paired_closed=0,
                  base_pnl=0.,filtered_pnl=0.,kept=0,kept_wins=0,kept_losses=0,
                  avoided_losses=0.,missed_profit=0.,skipped_winners=0,skipped_losers=0)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='rocket_entry_probes'").fetchone():
        return result
    rows=db.execute('''SELECT e.payload,p.status,p.realized_pnl_usdt,p.position_usdt
        FROM rocket_entry_probes e JOIN paper_positions p ON p.id=e.position_id
        WHERE p.signal_kind LIKE '%лидер%' ''').fetchall()
    for payload,status,pnl,stake in rows:
        try: probe=json.loads(payload)
        except (ValueError,TypeError): continue
        if probe.get('entry_policy')!='rocket-entry-C-live-v1': continue
        row=probe.get('market_regime') or {}
        if row.get('version')!=VERSION: continue
        result['recorded']+=1
        if row.get('state')!='KNOWN' or not isinstance(row.get('passed'),bool):
            result['unknown']+=1;continue
        if status!='CLOSED':result['open']+=1;continue
        if not finite(pnl) or not finite(stake) or stake<=0:
            result['unknown']+=1;continue
        net=50*pnl/stake
        result['paired_closed']+=1;result['base_pnl']+=net
        if row['passed']:
            result['kept']+=1;result['filtered_pnl']+=net
            result['kept_wins']+=net>0;result['kept_losses']+=net<0
        else:
            result['avoided_losses']+=max(0,-net);result['missed_profit']+=max(0,net)
            result['skipped_winners']+=net>0;result['skipped_losers']+=net<0
    return result

def report_text(db):
    d=report_data(db)
    return '\n'.join(['🌐 Фон рынка — новая теневая проверка',
        'Б: пропустить вход, только если BTC за 5 мин <0 и растут менее 50% наблюдаемых монет за минуту.',
        f"Новых записей {d['recorded']}; полных закрытых пар {d['paired_closed']}; неизвестно {d['unknown']}; открыто {d['open']}.",
        f"А, действующий C: {d['base_pnl']:+.3f} USDT. Б, с фоном рынка: {d['filtered_pnl']:+.3f} USDT; входов {d['kept']} (плюс/минус {d['kept_wins']}/{d['kept_losses']}).",
        f"Б пропустил прибыльных {d['skipped_winners']} на +{d['missed_profit']:.3f}, убыточных {d['skipped_losers']} на −{d['avoided_losses']:.3f} USDT.",
        'Только новые фактические входы C; снимок фиксируется до результата. Неполные данные исключаются из обеих веток. По 50 USDT с фактическими выходами и издержками. Без замещающих сделок и моделирования банка. Архивные 32 входа сюда не включены. На покупки не влияет.'])
