"""Incremental, bounded-memory lifetime bid summaries for new anomaly positions."""
import json
from bot.exit_policy import policy_for
from bot.recording_gaps import read_gaps

def build_anomaly_card(db, row, now):
    from bot.rocket_cards import exists, window_result, WINDOWS, MAX_GAP
    row=dict(row);entry=row['entry_price'];opened=row['opened_at'];closed=row['closed_at']
    end=now if closed is None else min(now,closed+3600)
    card=dict(position_id=row['id'],symbol=row['symbol'],opened_at=opened,
        entry_price=entry,closed_at=closed,reason=row['close_reason'],
        actual_pnl_usdt=row['realized_pnl_usdt'],actual_net_percent=row['realized_pnl_usdt']/row['position_usdt']*100,
        exit_policy=policy_for(row),windows={},comparisons={},source='bid',
        status='open' if closed is None else ('observing' if now<closed+3600 else 'finished'))
    saved=db.execute('SELECT payload FROM rocket_trade_cards WHERE position_id=?',(row['id'],)).fetchone() if exists(db,'rocket_trade_cards') else None
    old=json.loads(saved[0]) if saved else {}
    buckets=old.get('lifetime_minute_path',[])
    summary=old.get('path_summary',dict(count=0,last_at=opened,max_gap_seconds=0.,low=None,high=None))
    last=summary['last_at']
    if exists(db,'rocket_bid_path'):
        for at,bid in db.execute('SELECT timestamp,bid FROM rocket_bid_path WHERE symbol=? AND timestamp>? AND timestamp<=? ORDER BY timestamp',(row['symbol'],last,end)):
            summary['max_gap_seconds']=max(summary['max_gap_seconds'],at-last)
            if summary['low'] is None or bid<summary['low']:summary.update(low=bid,low_at=at)
            if summary['high'] is None or bid>summary['high']:summary.update(high=bid,high_at=at)
            index=int((at-opened)//60)
            if not buckets or buckets[-1]['minute_from_entry']!=index:
                buckets.append(dict(minute_from_entry=index,first_at=at,last_at=at,open=bid,high=bid,low=bid,close=bid,count=0))
            b=buckets[-1];b.update(high=max(b['high'],bid),low=min(b['low'],bid),close=bid,last_at=at,count=b['count']+1)
            summary['count']+=1;last=at
    summary['last_at']=last
    gaps=read_gaps(db,row['symbol'],opened,end)
    summary['status']='complete_observed' if summary['count'] and summary['max_gap_seconds']<=MAX_GAP and end-last<=MAX_GAP and not gaps else 'incomplete'
    if summary['count']:
        summary['low_from_entry_pct']=(summary['low']/entry-1)*100
        summary['high_from_entry_pct']=(summary['high']/entry-1)*100
    card.update(lifetime_minute_path=buckets,path_summary=summary,recording_gaps=[list(g)for g in gaps],
        observation_end=end,cost_percent=card['exit_policy'].get('cost_percent'))
    if exists(db,'rocket_entry_probes'):
        probe=db.execute('SELECT payload FROM rocket_entry_probes WHERE position_id=?',(row['id'],)).fetchone()
        card['entry_probe']=json.loads(probe[0]) if probe else None
    if closed is None:return card
    fill=db.execute("SELECT price FROM paper_fills WHERE position_id=? AND side='SELL' ORDER BY timestamp DESC,id DESC LIMIT 1",(row['id'],)).fetchone()
    if not fill:card['status']='missing_exit';return card
    price=float(fill[0]);card.update(exit_price=price,exit_change_percent=(price/entry-1)*100,seconds_to_exit=closed-opened)
    points=list(db.execute('SELECT timestamp,bid FROM rocket_bid_path WHERE symbol=? AND timestamp>? AND timestamp<=? ORDER BY timestamp',(row['symbol'],closed,end))) if exists(db,'rocket_bid_path') else []
    for minutes in WINDOWS:
        result=window_result(points,entry,closed,price,minutes,now,card['cost_percent'])
        if any(b>=closed and a<=closed+minutes*60 for a,b in gaps) and result['status']=='complete':result['status']='incomplete'
        card['windows'][str(minutes)]=result
    return card
