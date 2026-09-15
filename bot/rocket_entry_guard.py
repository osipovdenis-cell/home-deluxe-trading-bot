"""Final paper-entry veto for fading buy activity, using an existing fresh probe."""
import math


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


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
    if after < before and (r5 <= 0 or r10 < 0):
        return False, (f'ослабление покупок: 5с {before:.2f}→{after:.2f} USDT; '
                       f'цена 5с {r5:+.4f}%, 10с {r10:+.4f}%; вход отложен')
    return True, None
