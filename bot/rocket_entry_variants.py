"""Prospective, read-only comparison of filters on the same actual rocket entries."""
import json
import math
from collections import defaultdict
from statistics import median

VERSION = 'rocket-entry-four-v3-fading-buy-gate'
FIRST_REVIEW = 50


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def evaluate(probe):
    """Freeze decisions at entry; never reconstruct old decisions using outcomes."""
    context = probe.get('before_context') or {}
    flow = probe.get('after_flow') or {}
    changes = probe.get('changes') or {}
    volume = context.get('volume_ratio_5m')
    before_spread, spread = context.get('spread_bps'), flow.get('spread_bps')
    buys, sells = flow.get('buy_5s_usdt'), flow.get('sell_5s_usdt')
    checks = {}
    checks['volume'] = volume >= 1 if number(volume) and volume >= 0 else None
    fresh = probe.get('fresh') is True
    for seconds in ('5', '15', '60'):
        change = changes.get(seconds)
        checks['price_' + seconds] = change > 0 if fresh and number(change) else None
    checks['buys_5'] = buys > sells if fresh and all(number(v) and v >= 0 for v in (buys, sells)) else None
    checks['spread'] = spread <= before_spread if fresh and all(number(v) and v >= 0 for v in (spread, before_spread)) else None
    required = [checks[k] for k in ('price_5', 'price_15', 'price_60', 'buys_5', 'spread')]
    momentum = all(required) if all(v is not None for v in required) else None
    volume_ok = checks['volume']
    together = volume_ok and momentum if volume_ok is not None and momentum is not None else None
    return dict(version=VERSION, decisions=dict(A=True, B=volume_ok, C=momentum, D=together),
                checks=checks,
                # Volume is the already available analysis value, not a new REST request.
                volume_source='analysis_snapshot', volume_ratio=volume,
                before_spread_bps=before_spread, entry_spread_bps=spread)


def report_data(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='rocket_entry_probes' AND type='table'").fetchone() is None:
        rows = []
    else:
        rows = db.execute('''SELECT e.payload,p.id,p.symbol,p.status,p.closed_at,
            p.realized_pnl_usdt,p.position_usdt FROM rocket_entry_probes e
            JOIN paper_positions p ON p.id=e.position_id WHERE p.signal_kind LIKE '%лидер%'
            ORDER BY p.closed_at,p.id''').fetchall()
    versioned, paired, durations = [], [], []
    for payload, ident, symbol, status, closed, pnl, stake in rows:
        try:
            probe = json.loads(payload)
        except (ValueError, TypeError):
            continue
        variant = probe.get('entry_variants') or {}
        if variant.get('version') != VERSION:
            continue
        decisions = variant.get('decisions') or {}
        complete = all(isinstance(decisions.get(k), bool) for k in 'ABCD')
        versioned.append((status, complete))
        duration = probe.get('calculation_ms')
        if number(duration) and duration >= 0:
            durations.append(duration)
        if status == 'CLOSED' and complete and number(pnl) and number(stake) and stake > 0 and number(closed):
            paired.append(dict(id=ident, symbol=symbol, closed_at=closed,
                               pnl=50*pnl/stake, decisions=decisions))
    variants = {}
    for name in 'ABCD':
        selected = [r for r in paired if r['decisions'][name]]
        rejected = [r for r in paired if not r['decisions'][name]]
        total = peak = drawdown = 0.0
        by_symbol = defaultdict(float)
        gross_wins = defaultdict(float)
        for row in selected:
            total += row['pnl']
            peak = max(peak, total)
            drawdown = max(drawdown, peak-total)
            by_symbol[row['symbol']] += row['pnl']
            gross_wins[row['symbol']] += max(0, row['pnl'])
        gross_profit = sum(gross_wins.values())
        gross_loss = -sum(min(0, r['pnl']) for r in selected)
        top = max(gross_wins, key=gross_wins.get) if gross_profit else None
        variants[name] = dict(entries=len(selected), wins=sum(r['pnl']>0 for r in selected),
            losses=sum(r['pnl']<0 for r in selected), pnl_usdt=total,
            profit_factor=gross_profit/gross_loss if gross_loss else None,
            closed_pnl_drawdown_usdt=drawdown,
            skipped=len(rejected), avoided_losses_usdt=-sum(min(0,r['pnl']) for r in rejected),
            missed_profit_usdt=sum(max(0,r['pnl']) for r in rejected),
            skipped_winners=sum(r['pnl']>0 for r in rejected),
            skipped_losers=sum(r['pnl']<0 for r in rejected),
            symbols=len(by_symbol), positive_symbols=sum(v>0 for v in by_symbol.values()),
            top_profit_symbol=top, top_profit_share_percent=100*gross_wins[top]/gross_profit if top else None,
            symbol_pnl=dict(by_symbol))
    durations.sort()
    return dict(version=VERSION, recorded=len(versioned), paired_closed=len(paired),
                open=sum(status!='CLOSED' for status,complete in versioned),
                incomplete=sum(not complete for status,complete in versioned),
                review_target=FIRST_REVIEW, review_ready=len(paired)>=FIRST_REVIEW,
                variants=variants, calculation_samples=len(durations),
                calculation_p50_ms=median(durations) if durations else None,
                calculation_p95_ms=durations[max(0,math.ceil(.95*len(durations))-1)] if durations else None,
                calculation_max_ms=max(durations) if durations else None)


def report_text(db):
    data = report_data(db)
    lines = ['⚖️ Четыре варианта входа ракет — только тень',
             f"Версия {VERSION}; только новые фактические входы после включения версии.",
             f"Записано {data['recorded']}; общих закрытых {data['paired_closed']}; открытых {data['open']}; неполных {data['incomplete']}.",
             'А — текущие; Б — объём ≥×1; В — свежий импульс; Г — оба условия.',
             'В: рост за 5/15/60 с >0, покупки за 5 с > продаж, спред не шире снимка анализа. Б: объём из анализа.']
    for name, label in zip('ABCD', 'АБВГ'):
        v = data['variants'][name]
        lines.append(f"• {label}: входов {v['entries']} (плюс {v['wins']}, минус {v['losses']}); итог {v['pnl_usdt']:+.3f} USDT; просадка закрытого PnL {v['closed_pnl_drawdown_usdt']:.3f}.")
        if name != 'A':
            lines.append(f"  Пропущено {v['skipped']}: прибыльных {v['skipped_winners']} на +{v['missed_profit_usdt']:.3f}, убыточных {v['skipped_losers']} на −{v['avoided_losses_usdt']:.3f} USDT.")
        if v['top_profit_symbol']:
            lines.append(f"  Монет {v['symbols']}, с положительным итогом {v['positive_symbols']}; {v['top_profit_symbol']} даёт {v['top_profit_share_percent']:.1f}% суммы прибылей.")
    if data['calculation_samples']:
        lines.append(f"Время перепроверки, n={data['calculation_samples']}: медиана {data['calculation_p50_ms']:.3f}, p95 {data['calculation_p95_ms']:.3f}, максимум {data['calculation_max_ms']:.3f} мс.")
    else:
        lines.append('Замер задержки появится с первым новым входом; дополнительных запросов AI/REST нет.')
    lines.append(f"Предварительный разбор: {data['paired_closed']}/{FIRST_REVIEW} общих закрытых; " + ('выборка для первого разбора накоплена.' if data['review_ready'] else 'сбор продолжается.'))
    lines.append('Все варианты на одинаковых полных примерах: условно 50 USDT, фактические выходы и издержки; отказ = 0. Неизвестные условия исключены из всех четырёх. Просадка — по закрытым результатам, без открытых позиций. Занятость банка и замещающие сделки не моделируются. Старые примеры не переоцениваются. Автовключения торговли нет.')
    return '\n'.join(lines)
