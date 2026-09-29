"""Deterministic retrospective screening; no execution calls or AI decisions."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import random
from bot.idea_rules import matches, replay, finite

# Fixed in code before collection. Changing these creates a new protocol version.
PROTOCOL = dict(version='rocket-ideas-screen-v1', min_affected=30,
                positive_weeks=2, bootstrap_samples=2000, seed=7413,
                confidence=.95, remove_best_symbols=2)

def week_start(at):
    d=datetime.fromtimestamp(at,timezone.utc)
    return int(at)-d.weekday()*86400-d.hour*3600-d.minute*60-d.second

def evaluate(rule, events, source_keys, now):
    pairs=[];excluded=Counter()
    for e in events:
        if set(e.get('identity_keys',[e['key']])) & set(source_keys):
            excluded['source_event']+=1;continue
        hit=matches(rule,e['features'])
        if hit is None:excluded['missing_entry_features']+=1;continue
        if not e.get('complete') or not finite(e.get('net_percent')):
            excluded['incomplete_outcome']+=1;continue
        before=e['net_percent'];after=before
        if rule['type']=='entry_filter':
            if hit:after=0.
        elif rule['type']=='exit_params':
            # A common complete path and price basis are required in BOTH branches.
            if not e.get('path_complete'):
                excluded['incomplete_path']+=1;continue
            before=replay(e.get('minutes'),e.get('entry_ask'),e.get('policy',{}),e.get('cost'))
            after=replay(e.get('minutes'),e.get('entry_ask'),rule['action'] if hit else e['policy'],e.get('cost'))
            if before is None or after is None:excluded['unclosed_or_invalid_replay']+=1;continue
        elif rule['type']=='entry_delay':
            from bot.idea_path import delayed_replay
            if not e.get('path_complete'):
                excluded['incomplete_path']+=1;continue
            before=replay(e.get('minutes'),e.get('entry_ask'),e.get('policy',{}),e.get('cost'))
            after=delayed_replay(e,rule['action']['wait_seconds']) if hit else before
            if before is None or after is None:
                excluded['missing_timed_ask_or_unclosed_replay']+=1;continue
        pairs.append(dict(symbol=e['symbol'],at=e['at'],hit=hit,before=before,after=after,delta=after-before,
                          version=e.get('policy',{}).get('version','unknown'),source=e.get('source','trade')))
    n=len(pairs);affected=sum(p['hit'] for p in pairs)
    by_symbol=defaultdict(list);by_week=defaultdict(list);by_version=defaultdict(list);by_source=defaultdict(list)
    for p in pairs:
        by_symbol[p['symbol']].append(p['delta']);by_week[week_start(p['at'])].append(p['delta'])
        by_version[p['version']].append(p['delta']);by_source[p['source']].append(p['delta'])
    total=sum(p['delta'] for p in pairs)
    best=sorted(by_symbol,key=lambda s:sum(by_symbol[s]),reverse=True)[:2]
    remainder=[p for p in pairs if p['symbol'] not in best]
    ci=None
    if len(by_symbol)>=3:
        rng=random.Random(PROTOCOL['seed']);symbols=sorted(by_symbol);estimates=[]
        for _ in range(PROTOCOL['bootstrap_samples']):
            chosen=[rng.choice(symbols) for _ in symbols]
            estimates.append(sum(sum(by_symbol[s]) for s in chosen)/sum(len(by_symbol[s]) for s in chosen))
        estimates.sort();ci=[estimates[int(.025*len(estimates))],estimates[int(.975*len(estimates))]]
    boundary=week_start(now);required=[boundary-14*86400,boundary-7*86400]
    weeks_ok=all(w in by_week and sum(by_week[w])>0 for w in required)
    enough=affected>=PROTOCOL['min_affected'] and ci is not None and all(w in by_week for w in required) and bool(remainder)
    passed=enough and total>0 and weeks_ok and sum(p['delta'] for p in remainder)>0 and ci[0]>0
    def average(values):return sum(values)/len(values) if values else None
    return dict(status='passed' if passed else 'failed' if enough else 'insufficient_data',
        candidate_for_shadow=bool(passed), retrospective_only=True, events=n,affected=affected,
        delta_sum_percent=total,delta_mean_percent=total/n if n else None,
        delta_usdt_at_50=total*.5,bootstrap95=ci,
        skipped_mean_percent=average([p['before'] for p in pairs if p['hit']]),
        kept_mean_percent=average([p['before'] for p in pairs if not p['hit']]),
        weeks={str(k):dict(n=len(v),delta=sum(v)) for k,v in by_week.items()},
        strategy_versions={k:dict(n=len(v),delta=sum(v)) for k,v in by_version.items()},
        sources={k:dict(n=len(v),delta=sum(v)) for k,v in by_source.items()},
        excluded=dict(excluded),without_best2_delta=sum(p['delta'] for p in remainder),
        removed_symbols=best,required_complete_weeks=required,protocol=PROTOCOL)
