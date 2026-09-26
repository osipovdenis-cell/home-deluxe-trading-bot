"""Final paper-entry checks, including the prospectively compared C policy."""
import math
from bot.rocket_entry_variants import evaluate, EXECUTION_POLICY


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def fresh_quality_guard(probe):
    """Reuse the existing leader-quality verdict computed from fresh order flow."""
    if not isinstance(probe, dict) or probe.get('fresh') is not True:
        return False, 'нет свежих данных рыночного качества'
    if probe.get('allowed') is not True:
        return False, str(probe.get('reason') or 'рыночное качество не подтверждено')
    return True, None


def fading_buy_guard(probe):
    if not isinstance(probe, dict) or probe.get('fresh') is not True:
        detail = probe.get('reason', 'нет снимка') if isinstance(probe, dict) else 'нет снимка'
        return False, 'перепроверка ракет: нет свежих данных; вход отложен: ' + str(detail)
    before = (probe.get('before_context') or {}).get('flow_buy_5s_usdt')
    after = (probe.get('after_flow') or {}).get('buy_5s_usdt')
    changes = probe.get('changes') or {}
    r5, r10 = changes.get('5'), changes.get('10')
    if not all(finite(v) for v in (before, after, r5, r10)) or min(before, after) < 0:
        return False, 'перепроверка ракет: неполные данные покупок/цены; вход отложен'
    if r5 <= 0:
        return False, f'рост за последние 5с не подтверждён ({r5:+.4f}%); вход отложен'
    if after < before and r10 < 0:
        return False, (f'ослабление покупок: 5с {before:.2f}→{after:.2f} USDT; '
                       f'цена 5с {r5:+.4f}%, 10с {r10:+.4f}%; вход отложен')
    quality_ok, quality_reason = fresh_quality_guard(probe)
    if not quality_ok:
        return False, 'свежее рыночное качество: ' + quality_reason
    # Same deterministic C conditions as the prospective comparison. Never trust
    # cached decisions in a caller-provided probe; evaluate its current numbers.
    variant = evaluate(probe)
    probe['entry_variants'] = variant
    probe['entry_policy'] = EXECUTION_POLICY
    labels = {'price_5': 'цена за 5с не растёт',
              'price_15': 'цена за 15с не растёт',
              'price_60': 'цена за 60с не растёт',
              'buys_5': 'покупки за 5с не превышают продажи',
              'spread': 'спред расширился относительно анализа'}
    for key, label in labels.items():
        value = variant['checks'][key]
        if value is not True:
            return False, 'фильтр В: ' + (label if value is False else 'нет полных данных: ' + key)
    return True, None
