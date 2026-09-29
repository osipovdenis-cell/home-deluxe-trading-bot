"""Strict declarative shadow rules. Never imported by execution or trading code."""
import hashlib
import json
import math
from decimal import Decimal, ROUND_HALF_UP

# Only decision-time numeric inputs. Symbols and future outcomes are never predicates.
FEATURES = {
    'utc_hour':1, 'utc_weekday':1, 'signal_type':1,
    'growth_10m_from_low_percent':.1, 'change_5s_percent':.05,
    'change_15s_percent':.05, 'change_60s_percent':.1, 'change_300s_percent':.1,
    'trend_change_15m_percent':.1, 'trend_change_60m_percent':.1,
    'trend_change_240m_percent':.1, 'change_12h_percent':.1, 'change_24h_percent':.1,
    'volume_ratio_5m':.1, 'taker_buy_ratio_percent':1,
    'flow_cvd_60s_percent':1, 'flow_trade_rate_acceleration':.1,
    'spread_bps':.1, 'flow_spread_change_bps':.1, 'tick_percent':.001,
    'pullback_from_5m_high_percent':.1, 'btc_change_300s_percent':.05,
    'btc_change_3600s_percent':.1, 'market_breadth_60s_percent':1,
    'confirmation_accepted':1, 'filter_c_passed':1, 'ai_score':1,
    'ai_decision_code':1, 'prior_entries_24h':1, 'prior_closed_pnl_24h':.1,
}
OPS = {'>':lambda a,b:a>b, '<':lambda a,b:a<b, '>=':lambda a,b:a>=b,
       '<=':lambda a,b:a<=b, '==':lambda a,b:a==b}

def finite(value):
    return type(value) in (int,float) and math.isfinite(value)

def validate(raw):
    if not isinstance(raw,dict) or set(raw)!={'type','condition','action','rationale'}:
        raise ValueError('idea shape')
    kind=raw['type']; action=raw['action']; conditions=raw['condition']
    if kind not in ('entry_filter','exit_params','entry_delay') or not isinstance(action,dict):
        raise ValueError('idea type')
    if not isinstance(conditions,list) or not 1<=len(conditions)<=5:
        raise ValueError('conditions')
    normalized=[]
    for c in conditions:
        if not isinstance(c,dict) or set(c)!={'feature','op','value'}:
            raise ValueError('predicate shape')
        if c['feature'] not in FEATURES or c['op'] not in OPS or not finite(c['value']):
            raise ValueError('forbidden feature/operator/number')
        if abs(c['value'])>1e9: raise ValueError('threshold range')
        step=Decimal(str(FEATURES[c['feature']]))
        value=float((Decimal(str(c['value']))/step).quantize(Decimal('1'),rounding=ROUND_HALF_UP)*step)
        normalized.append(dict(feature=c['feature'],op=c['op'],value=value))
    if kind=='entry_filter':
        if set(action)!={'skip'} or action['skip'] is not True: raise ValueError('filter action')
    elif kind=='exit_params':
        if set(action)!={'stop_percent','protect_percent','trail_pp'}: raise ValueError('exit action')
        if not all(finite(v) and .05<=v<=30 for v in action.values()): raise ValueError('exit bounds')
    else:
        if set(action)!={'wait_seconds'} or type(action['wait_seconds']) is not int or not 1<=action['wait_seconds']<=3600:
            raise ValueError('delay bounds')
    if not isinstance(raw['rationale'],str) or len(raw['rationale'])>1000: raise ValueError('rationale')
    normalized.sort(key=lambda c:(c['feature'],c['op'],c['value']))
    result=dict(type=kind,condition=normalized,action=action,rationale=raw['rationale'])
    key=hashlib.sha256(json.dumps({k:result[k] for k in ('type','condition','action')},sort_keys=True).encode()).hexdigest()
    return result,key

def matches(rule, features):
    if any(not finite(features.get(c['feature'])) for c in rule['condition']): return None
    return all(OPS[c['op']](features[c['feature']],c['value']) for c in rule['condition'])


def replay(minutes, entry_ask, policy, cost):
    """Bid OHLC in both possible minute orders; no forced exit at data end."""
    if not minutes or not finite(entry_ask) or entry_ask<=0 or not finite(cost) or cost<0:
        return None
    if not all(finite(policy.get(k)) and policy[k]>0 for k in ('stop_percent','protect_percent','trail_pp')):
        return None
    results=[]
    for order in (('open','high','low','close'),('open','low','high','close')):
        peak=0.;armed=False;answer=None
        for bar in minutes:
            if not all(finite(bar.get(k)) and bar[k]>0 for k in order): return None
            if not bar['low']<=min(bar['open'],bar['close'])<=max(bar['open'],bar['close'])<=bar['high']: return None
            for k in order:
                change=(bar[k]/entry_ask-1)*100;peak=max(peak,change)
                if change<=-policy['stop_percent']+1e-9:
                    answer=change-cost;break
                armed=armed or peak+1e-9>=policy['protect_percent']
                floor=max([policy['protect_percent']]+[v for v in policy.get('protect_steps',[]) if peak+1e-9>=v])
                if armed and change+1e-9<peak and change<=max(floor,peak-policy['trail_pp'])+1e-9:
                    answer=change-cost;break
            if answer is not None: break
        if answer is None:return None
        results.append(answer)
    return min(results)
