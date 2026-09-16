"""Prospective delayed-entry experiment; diagnostics only, no trading methods."""
import json
import math

VERSION = 'recovery-two-windows-v1'
WAIT, HORIZON, MAX_GAP = 90, 3600, 5


def finite(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def window_snapshot(trades, quotes, now):
    """Disjoint (t-10,t-5], (t-5,t]; anchors never use future observations."""
    trades = [r for r in trades if r[0] <= now]
    quotes = [r for r in quotes if r[0] <= now]
    times = (now-10, now-5, now)
    anchors = [next((r for r in reversed(quotes) if r[0] <= t), None) for t in times]
    covered = bool(trades and trades[0][0] <= now-10 and now-trades[-1][0] <= 2
                   and all(r is not None and 0 <= t-r[0] <= 2 for t,r in zip(times,anchors)))
    recent = [r for r in quotes if r[0] >= now-12]
    covered = covered and all(b[0]-a[0] <= 2 for a,b in zip(recent,recent[1:]))
    windows = []
    for low,high in ((now-10,now-5),(now-5,now)):
        rows = [r for r in trades if low < r[0] <= high]
        windows.append(dict(start=low,end=high,buy=sum(r[2] for r in rows if r[3]),
                            sell=sum(r[2] for r in rows if not r[3])))
    bids = [r[1] if r else None for r in anchors]
    valid = covered and all(finite(v) and v>0 for v in bids)
    valid = valid and all(finite(w[k]) and w[k]>=0 for w in windows for k in ('buy','sell'))
    passed = (all(w['buy']>w['sell'] for w in windows) and bids[2]>bids[1]) if valid else None
    return dict(at=now,complete=bool(valid),passed=passed,windows=windows,
                bid_10s=bids[0],bid_5s=bids[1],bid=bids[2],
                ask=quotes[-1][3] if quotes else None,quote_at=quotes[-1][0] if quotes else None)


def new_leg(at,ask,bid):
    return dict(status='OPEN',entered=at,entry=ask,peak=ask,trough=bid,
                last_at=at,last_bid=bid,net=None)


def advance(leg,points,now,end,stop,cost):
    if leg['status']!='OPEN':
        return
    for at,bid in points:
        if not leg['last_at']<at<=min(now,end):
            continue
        if not finite(bid) or bid<=0 or at-leg['last_at']>MAX_GAP:
            leg.update(status='INCOMPLETE',reason='разрыв bid')
            return
        leg.update(last_at=at,last_bid=bid,peak=max(leg['peak'],bid),trough=min(leg['trough'],bid))
        change=(bid/leg['entry']-1)*100
        peak=(leg['peak']/leg['entry']-1)*100
        reason=('STOP' if change<=-stop else
                'TRAIL' if peak>=1 and change+1e-9<peak and change<=max(1,peak-1) else None)
        if reason:
            leg.update(status='CLOSED',reason=reason,exited=at,net=change-cost)
            return
    if min(now,end)-leg['last_at']>MAX_GAP:
        leg.update(status='INCOMPLETE',reason='нет bid')
    elif now>=end:
        leg.update(status='MARKED',reason='открыто на горизонте',
                   net=(leg['last_bid']/leg['entry']-1)*100-cost)


class RecoveryShadow:
    def __init__(self,db,probe_provider=None,stop=.5):
        self.db,self.probe_provider,self.stop=db,probe_provider,stop
        db.execute("""CREATE TABLE IF NOT EXISTS rocket_recovery_pairs(
            position_id INTEGER PRIMARY KEY,version TEXT NOT NULL,finished REAL,payload TEXT NOT NULL)""")

    def seed(self,ident,probe):
        if 'recovery_windows' not in probe:
            return
        if self.db.execute('SELECT 1 FROM rocket_recovery_pairs WHERE position_id=?',(ident,)).fetchone():
            return
        row=self.db.execute("""SELECT p.symbol,p.opened_at,p.entry_price,c.cost_percent
            FROM paper_positions p JOIN rocket_stop_costs c ON c.position_id=p.id WHERE p.id=?""",(ident,)).fetchone()
        if not row:
            return
        symbol,start,ask,cost=row
        bid=probe.get('entry_bid')
        if not all(finite(v) for v in (start,ask,bid,cost,self.stop)) or min(ask,bid,self.stop)<=0 or cost<0:
            return
        w=probe['recovery_windows']
        dynamics=probe.get('before_dynamics') or {}
        immediate=(w.get('complete') is True and w.get('passed') is True
                   and probe.get('fresh') is True and probe.get('allowed') is True)
        state=dict(symbol=symbol,start=start,stop=self.stop,cost=cost,
                   signal_price=probe.get('signal_price') or ask,probe=probe,
                   market=dict(btc_60=dynamics.get('btc_change_60s_percent'),
                               btc_300=dynamics.get('btc_change_300s_percent'),
                               breadth_60=dynamics.get('market_breadth_60s_percent'),
                               source='снимок исходного анализа'),
                   last_check=start,missing=not w.get('complete',False),
                   A=new_leg(start,ask,bid),
                   B=new_leg(start,ask,bid) if immediate else dict(status='WAIT',net=None))
        self.db.execute('INSERT INTO rocket_recovery_pairs VALUES(?,?,NULL,?)',(ident,VERSION,json.dumps(state)))

    def tick(self,now):
        rows=self.db.execute('SELECT position_id,payload FROM rocket_recovery_pairs WHERE version=? AND finished IS NULL',(VERSION,)).fetchall()
        for ident,payload in rows:
            s=json.loads(payload)
            b,end=s['B'],s['start']+HORIZON
            if b['status']=='WAIT':
                if now-s['last_check']>2:
                    s['missing']=True
                if now>=s['start']+WAIT:
                    b.update(status='INCOMPLETE' if s['missing'] else 'NO_ENTRY',
                             reason='неполное наблюдение' if s['missing'] else '90с без устойчивого восстановления')
                elif self.probe_provider is not None:
                    try:
                        p=self.probe_provider(s['symbol'],s['probe'])
                    except Exception:
                        p=None
                    w=(p or {}).get('recovery_windows') or {}
                    decision_at=(p or {}).get('at',now)
                    if not w.get('complete') or not p.get('fresh'):
                        s['missing']=True
                    elif w.get('passed') is True and p.get('allowed') is True:
                        ask,bid,at=w.get('ask'),w.get('bid'),w.get('quote_at')
                        if (all(finite(v) for v in (ask,bid,at,decision_at)) and ask>=bid>0
                                and 0<=decision_at-at<=2 and s['start']<=decision_at<s['start']+WAIT
                                and (ask/bid-1)*100<=.25
                                and abs((ask/s['signal_price']-1)*100)<=s['stop']):
                            if s['missing']:
                                b.update(status='INCOMPLETE',reason='возможный более ранний вход пропущен')
                            else:
                                s['B']=b=new_leg(decision_at,ask,bid)
                                s['confirmation']=w
                else:
                    s['missing']=True
                s['last_check']=now
            for key in ('A','B'):
                leg=s[key]
                if leg['status']=='OPEN':
                    points=self.db.execute("""SELECT timestamp,bid FROM rocket_bid_path
                        WHERE symbol=? AND timestamp>? AND timestamp<=? ORDER BY timestamp""",
                        (s['symbol'],leg['last_at'],min(now,end))).fetchall()
                    advance(leg,points,now,end,s['stop'],s['cost'])
            done=now>=end or all(s[k]['status'] in ('CLOSED','NO_ENTRY','INCOMPLETE') for k in ('A','B'))
            self.db.execute('UPDATE rocket_recovery_pairs SET finished=?,payload=? WHERE position_id=?',
                            (now if done else None,json.dumps(s),ident))


def report_data(db):
    exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='rocket_recovery_pairs'").fetchone()
    rows=db.execute('SELECT position_id,finished,payload FROM rocket_recovery_pairs WHERE version=? ORDER BY position_id',(VERSION,)).fetchall() if exists else []
    pairs=[dict(id=i,finished=f,**json.loads(p)) for i,f,p in rows]
    complete=[s for s in pairs if s['finished'] is not None and all(s[k]['status']!='INCOMPLETE' for k in ('A','B'))]
    def total(group):
        result={}
        for k in ('A','B'):
            closed=[s[k] for s in group if s[k]['status']=='CLOSED']
            marked=[s[k] for s in group if s[k]['status']=='MARKED']
            result[k]=dict(closed=len(closed),wins=sum(l['net']>0 for l in closed),
                           realized=sum(l['net']*.5 for l in closed),marked=len(marked),
                           unrealized=sum(l['net']*.5 for l in marked),
                           no_entry=sum(s[k]['status']=='NO_ENTRY' for s in group))
        return result
    skipped=[s for s in complete if s['B']['status']=='NO_ENTRY' and s['A']['status']=='CLOSED']
    weak=[s for s in complete if finite(s['market']['btc_300']) and s['market']['btc_300']<0
          and finite(s['market']['breadth_60']) and s['market']['breadth_60']<50]
    return dict(version=VERSION,recorded=len(pairs),complete=len(complete),
                pending=sum(s['finished'] is None for s in pairs),
                incomplete=sum(any(s[k]['status']=='INCOMPLETE' for k in ('A','B')) for s in pairs),
                variants=total(complete),missed_winners=sum(s['A']['net']>0 for s in skipped),
                missed_profit=sum(max(0,s['A']['net'])*.5 for s in skipped),
                avoided_losers=sum(s['A']['net']<0 for s in skipped),
                avoided_loss=-sum(min(0,s['A']['net'])*.5 for s in skipped),
                weak_market_pairs=len(weak),weak_market=total(weak),pairs=pairs[-100:])


def report_text(db):
    d=report_data(db)
    lines=['⚖️ Устойчивое восстановление — тень A/B',
           f"Версия {VERSION}. Новых пар {d['recorded']}; полных {d['complete']}; наблюдаются {d['pending']}; неполных {d['incomplete']}.",
           'А: момент фактической покупки. Б: два соседних окна по 5с, в каждом покупки > продаж; bid сейчас выше bid 5с назад.',
           'Если условие уже выполнено — тот же вход. Иначе наблюдаем до 90с без нового AI/20с; сохраняем свежесть, рыночное качество и границы цены/спреда.']
    for k,v in d['variants'].items():
        lines.append(f"• {k}: закрыто {v['closed']} (плюс {v['wins']}), итог {v['realized']:+.3f} USDT; открыто на горизонте {v['marked']} на {v['unrealized']:+.3f}; без входа {v['no_entry']}.")
    lines.append(f"Б пропустил прибыльных А: {d['missed_winners']} на +{d['missed_profit']:.3f}; убыточных: {d['avoided_losers']} на −{d['avoided_loss']:.3f} USDT.")
    weak=d['weak_market']
    lines.append(f"BTC за 5м <0 и ширина рынка <50%: пар {d['weak_market_pairs']}; закрытые А {weak['A']['realized']:+.3f}, Б {weak['B']['realized']:+.3f} USDT. Это разрез анализа, не торговый фильтр.")
    lines.extend(['Только новые фактические входы ракет. По 50 USDT, общий горизонт 60 мин от А; одинаковые стоп и комиссии зафиксированы на старте. Защита +1%, откат 1 п.п., вся позиция.',
                  'Вход Б по доступному ask, выходы обеих веток по записанному bid. Открытые оценки не считаются закрытой прибылью. Нет моделирования глубины, проскальзывания и занятости банка.',
                  'Пробелы bid >5с или проверки ожидания >2с исключают затронутые пары. BTC/ширина рынка — снимок исходного анализа; нет данных не значит слабый рынок. Автовключения нет.'])
    return '\n'.join(lines)
