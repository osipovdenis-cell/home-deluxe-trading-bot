"""Frozen, prospective low-volume rocket hypothesis. No orders or network calls."""
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from statistics import median
from types import SimpleNamespace

from bot.rocket_entry_guard import finite, fading_buy_guard
from bot.rocket_cards import entry_probe

VERSION = 'rocket-volume-continuation-v2-entry-C'


def number(value):
    return value if finite(value) else None


def missing(at, reason):
    return dict(version=VERSION, evaluated_at=at, state='UNKNOWN', reasons=[reason],
                hypothesis=None, features={})


def capture(market, signal, context, started):
    """Read only already buffered market data; a diagnostic failure cannot buy."""
    at = time.time()
    try:
        dynamics = market.entry_dynamics(signal.symbol, at)
        probe = entry_probe(market, signal, context, dynamics, started,
                            probe_provider=market.__dict__.get('rocket_volume_probe')) or {}
        return evaluate(probe, context, signal.price,
                        market.tick_sizes.get(signal.symbol),
                        market.change_12h_percent.get(signal.symbol), time.time())
    except Exception:
        return missing(at, 'ошибка снимка')


def evaluate(probe, context, signal_price, tick_size, change12h, at):
    flow = probe.get('after_flow') or probe.get('observed_flow') or {}
    changes = probe.get('changes') or {}
    f = {f'price_{seconds}s': number(changes.get(str(seconds))) for seconds in (5, 10, 20, 60)}
    for field in ('buy_5s_usdt', 'sell_5s_usdt', 'buy_60s_usdt', 'sell_60s_usdt',
                  'cvd_60s_percent', 'trade_rate_acceleration', 'price_efficiency_per_10k',
                  'spread_bps', 'spread_change_bps'):
        f[field] = number(flow.get(field))
    f.update(volume_ratio=number(context.volume_ratio_5m), signal_price=number(signal_price),
             tick_size=number(tick_size), change_12h=number(change12h),
             context_buy_5s=number((probe.get('before_context') or {}).get('flow_buy_5s_usdt')),
             trade_age=number(probe.get('trade_age')), quote_age=number(probe.get('quote_age')))
    for window in (15, 60, 240):
        f[f'trend_{window}m'] = number(getattr(context, f'trend_change_{window}m_percent', None))
    windows = (probe.get('recovery_windows') or {}).get('windows') or []
    for field in ('buy', 'sell'):
        f[f'previous_{field}_5s'] = number(windows[0].get(field)) if windows else None
    result = dict(version=VERSION, evaluated_at=number(probe.get('at')), state='UNKNOWN',
                  features=f, hypothesis=None, reasons=[], feed_version=probe.get('feed_version',1))
    required = ('price_5s', 'price_10s', 'price_20s', 'price_60s', 'buy_5s_usdt',
                'sell_5s_usdt', 'buy_60s_usdt', 'sell_60s_usdt', 'spread_bps',
                'volume_ratio', 'signal_price', 'tick_size', 'change_12h', 'context_buy_5s')
    absent = [key for key in required if f[key] is None]
    stamp = result['evaluated_at']
    fresh = (stamp is not None and 0 <= at-stamp <= 2 and probe.get('fresh') is True
             and all(f[key] is not None and 0 <= f[key] <= 2 for key in ('trade_age', 'quote_age'))
             and (probe.get('recovery_windows') or {}).get('complete') is True)
    if absent or not fresh:
        result['reasons'] = ['неполный/устаревший снимок'] + list(probe.get('freshness_reasons') or []) + absent
        return result
    if (not 0 <= f['volume_ratio'] < 1 or min(f['signal_price'], f['tick_size']) <= 0
            or any(f[k] < 0 for k in ('buy_5s_usdt','sell_5s_usdt','buy_60s_usdt',
                                      'sell_60s_usdt','context_buy_5s','spread_bps'))):
        result['reasons'] = ['некорректные признаки']
        return result
    result['hypothesis'] = (all(f[f'price_{w}s'] > 0 for w in (5,20,60))
                            and all(f[f'buy_{w}s_usdt'] > f[f'sell_{w}s_usdt'] for w in (5,60)))
    if not result['hypothesis']:
        result['reasons'].append('нет роста 5/20/60с или преобладания покупок 5/60с')
    guard, reason = fading_buy_guard(probe)
    if not guard:
        result['reasons'].append(reason)
    if f['change_12h'] <= 0:
        result['reasons'].append('нет роста за 12ч')
    # Match existing execution limits, using the fresh spread rather than REST context.
    from bot.market import MarketMonitor
    monitor = SimpleNamespace(tick_sizes={'candidate': f['tick_size']})
    safe, reason, _ = MarketMonitor.execution_safety(
        monitor, 'candidate', f['signal_price'], replace(context, spread_bps=f['spread_bps']),
        max_spread_percent=.25)
    if not safe:
        result['reasons'].append(reason)
    result['state'] = 'ELIGIBLE' if not result['reasons'] else 'NO_ENTRY'
    return result


def first_quote(snapshot, quote, stop):
    """Evaluate only the first quote. Never wait for a more favourable fill."""
    at, bid, ask = quote
    f = snapshot['features']
    if not 0 <= at-snapshot['evaluated_at'] <= 2:
        return dict(state='UNKNOWN', reason='снимок старше 2с к первой котировке')
    spread, drift = (ask/bid-1)*100, (ask/f['signal_price']-1)*100
    reason = ('спред первой котировки' if spread > .25 else
              'шаг цены первой котировки' if f['tick_size']/ask*100 > .1 else
              'цена ушла от сигнала на расстояние стопа' if abs(drift) >= stop else None)
    return dict(state='NO_ENTRY' if reason else 'ENTERED', at=at,
                reason=reason, spread_percent=spread, drift_percent=drift)


def leg_stats(items):
    closed = [s for s in items if s['leg']['status'] == 'CLOSED']
    pnl = [s['leg']['net']*.5 for s in closed]
    positive, negative = sum(max(p,0) for p in pnl), -sum(min(p,0) for p in pnl)
    return dict(count=len(items), closed=len(closed), wins=sum(p>0 for p in pnl),
                losses=sum(p<0 for p in pnl), flat=sum(p==0 for p in pnl), pnl=sum(pnl),
                profit_factor=positive/negative if negative else None,
                pending=sum(s['leg']['status'] in ('WAIT','OPEN') for s in items),
                incomplete=sum(s['leg']['status']=='INCOMPLETE' for s in items),
                marked=sum(s['leg']['status']=='MARKED' for s in items),
                marked_pnl=sum(s['leg']['net']*.5 for s in items if s['leg']['status']=='MARKED'))


def summarize(items, now, seconds):
    rows = [s for s in items if now-seconds <= s['at'] <= now
            and (s.get('volume_experiment') or {}).get('version') == VERSION]
    selected = [s for s in rows if s.get('volume_execution',{}).get('state')=='ENTERED']
    new_feed = [s for s in rows if s['volume_experiment'].get('feed_version')==2]
    known = [s for s in rows if s['volume_experiment']['state'] != 'UNKNOWN']
    groups = {}
    for name, key in [('symbols', lambda s:s['symbol']),
                      ('days_utc', lambda s:datetime.fromtimestamp(s['at'],timezone.utc).strftime('%Y-%m-%d'))]:
        groups[name] = {label:leg_stats([s for s in selected if key(s)==label])
                        for label in sorted({key(s) for s in selected})}
    best = max((g['pnl'] for g in groups['symbols'].values()), default=0)
    comparison = {}
    for name, sign in [('profitable',1), ('losing',-1)]:
        cases = [s for s in known if s['leg']['status']=='CLOSED' and s['leg']['net']*sign>0]
        comparison[name] = {}
        for key in ('price_5s','price_20s','price_60s','volume_ratio','buy_5s_usdt',
                    'sell_5s_usdt','cvd_60s_percent','trade_rate_acceleration','spread_bps',
                    'trend_15m','trend_60m','trend_240m'):
            values = [s['volume_experiment']['features'].get(key) for s in cases]
            values = [v for v in values if finite(v)]
            comparison[name][key] = dict(n=len(values),median=median(values) if values else None)
    return dict(since=now-seconds, until=now, candidates=len(rows),
                feed_versions=dict(Counter(s['volume_experiment'].get('feed_version',1) for s in rows)),
                new_feed=dict(candidates=len(new_feed),
                    unknown_features=sum(s['volume_experiment']['state']=='UNKNOWN' for s in new_feed),
                    selected=leg_stats([s for s in new_feed if s.get('volume_execution',{}).get('state')=='ENTERED'])),
                unknown_features=len(rows)-len(known), baseline_pnl=0,
                all_low_volume=leg_stats(rows), selected=leg_stats(selected),
                hypothesis_passed=sum(s['volume_experiment'].get('hypothesis') is True for s in rows),
                eligible=sum(s['volume_experiment']['state']=='ELIGIBLE' for s in rows),
                execution_states=dict(Counter(s.get('volume_execution',{}).get('state','UNKNOWN') for s in rows)),
                rejection_reasons=dict(Counter(reason for s in rows for reason in s['volume_experiment']['reasons'])),
                execution_reasons=dict(Counter(s['volume_execution']['reason'] for s in rows
                    if s.get('volume_execution',{}).get('reason'))),
                pnl_without_best_symbol=leg_stats(selected)['pnl']-best,
                feature_comparison=comparison, **groups)


def report_data(items, now):
    return dict(version=VERSION, shadow_only=True, daily=summarize(items,now,86400),
                seven_days=summarize(items,now,7*86400))


def report_text(data):
    lines=['🧪 Ракеты: объём ниже ×1 — только тень (v2, с фильтром В)',
           'B: цена растёт за 5/20/60с; покупки > продаж за 5 и 60с. Сохраняются проверки свежести, рыночного качества, роста за 12ч, спреда и шага цены.']
    for key,label in [('daily','24 часа'),('seven_days','7 дней')]:
        d=data[key]; b=d['selected']; states=d['execution_states']; all_=d['all_low_volume']
        lines.extend([f"{label}: отказов по объёму {d['candidates']}; неполных снимков {d['unknown_features']}; гипотезу прошли {d['hypothesis_passed']}, все проверки снимка {d['eligible']}.",
            f"A, оставить отказ: 0 USDT. B: входов {b['count']}; закрыто {b['closed']} (плюс/минус/ноль {b['wins']}/{b['losses']}/{b['flat']}), {b['pnl']:+.3f} USDT.",
            f"B: наблюдаются {b['pending']}; разрывы пути {b['incomplete']}; открыты через час {b['marked']} ({b['marked_pnl']:+.3f} USDT). Без входа {states.get('NO_ENTRY',0)}, первая котировка ожидается {states.get('PENDING',0)}, неизвестно {states.get('UNKNOWN',0)}.",
            f"Для сравнения, купить все отказы по объёму: закрыто {all_['closed']}, плюс/минус {all_['wins']}/{all_['losses']}, {all_['pnl']:+.3f} USDT; неполных путей {all_['incomplete']}."])
    d=data['seven_days']
    nf=d['new_feed']
    lines.append(f"Новый поток v2: снимков {nf['candidates']}, полных {nf['candidates']-nf['unknown_features']}, неполных {nf['unknown_features']}; условных входов {nf['selected']['count']}.")
    for reason,count in sorted(d['rejection_reasons'].items(),key=lambda item:-item[1])[:3]:
        lines.append(f"Причина отказа B: {str(reason)[:110]} — {count}.")
    if d['selected']['closed']:
        lines.append(f"B без лучшей монеты: {d['pnl_without_best_symbol']:+.3f} USDT; монет со входом {len(d['symbols'])}.")
    lines.extend(['По 50 USDT, первая ask после отказа, выход по bid, горизонт 60 мин. Стоп и комиссия фиксируются на сигнале; защита +1%, откат 1 п.п. Незакрытые позиции — отдельно.',
                  'AI, история монеты, лимиты банка/слотов, глубина и проскальзывание здесь не моделируются. Это проверка рыночной гипотезы, не полная замена торгового алгоритма. Старые отказы без снимков не включаются. Автовключения торговли нет.'])
    return '\n'.join(lines)
