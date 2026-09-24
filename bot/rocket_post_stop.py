"""Post-stop summary from the same bounded bid windows as individual cards."""
import json
import math

from bot.rocket_cards import build_card, exists, WINDOWS, MAX_GAP


def report_text(db, now, since, horizon_seconds=3600):
    minutes = horizon_seconds // 60
    if horizon_seconds != minutes * 60 or minutes not in WINDOWS:
        raise ValueError('Unsupported post-stop horizon')
    cursor = db.execute(
        "SELECT * FROM paper_positions WHERE status='CLOSED' "
        "AND close_reason='стоп-лосс' AND signal_kind LIKE '%лидер%' "
        "AND closed_at>=? AND closed_at<=? ORDER BY closed_at,id", (since, now))
    names = [c[0] for c in cursor.description]
    rows = [dict(zip(names, r)) for r in cursor.fetchall()]
    if not rows:
        return '📉 После стопа: стопов ракет за период пока нет.'
    complete, partial = [], []
    pending = missing = 0
    for row in rows:
        end = row['closed_at'] + horizon_seconds
        if now < end:
            pending += 1
            continue
        saved = db.execute('SELECT payload FROM rocket_trade_cards WHERE position_id=?',
                           (row['id'],)).fetchone() if exists(db, 'rocket_trade_cards') else None
        try:
            card = json.loads(saved[0]) if saved else None
        except (TypeError, ValueError):
            card = None
        valid = isinstance(card, dict) and all(card.get(k) == row[v] for k,v in
            [('position_id','id'),('symbol','symbol'),('entry_price','entry_price'),
             ('opened_at','opened_at'),('closed_at','closed_at')])
        if not valid or card.get('windows', {}).get(str(minutes), {}).get('status') == 'pending':
            card = build_card(db, row, now)
        w = card.get('windows', {}).get(str(minutes), {})
        if card.get('source') != 'bid' or w.get('status') not in ('complete','incomplete'):
            missing += 1
            continue
        # A stale/future cached endpoint cannot certify a complete observation.
        stamp = w.get('end_quote_at')
        if not isinstance(stamp, (int, float)) or not math.isfinite(stamp) or stamp > end:
            missing += 1
            continue
        if w['status'] == 'complete' and end-stamp <= MAX_GAP:
            complete.append(w)
        else:
            partial.append(w)
    lines = ['📉 Что происходило после стопа — bid-карточки v2',
             f'Ракет со стопом: {len(rows)}; окно после выхода {minutes} мин.',
             f'Полных путей: {len(complete)}; неполных: {len(partial)}; '
             f'нет проверяемых bid-данных: {missing}; ещё наблюдаются: {pending}.']

    def recovered(items, key):
        # First return anywhere in the window, including BEFORE its deepest low.
        return sum(isinstance(w.get(key), (int,float)) and math.isfinite(w[key])
                   and 0 <= w[key] <= minutes for w in items)

    if complete:
        n = len(complete)
        avg = lambda key: sum(w[key] for w in complete) / n
        entry = recovered(complete, 'return_to_entry_minutes')
        lines += [
            f"По полным путям: минимум после выхода в среднем {avg('low_from_exit'):+.2f}%; "
            f"наиболее глубокий {min(w['low_from_exit'] for w in complete):+.2f}%.",
            f"Дно от входа в среднем {avg('low_from_entry'):+.2f}%; "
            f"время до дна {avg('minutes_to_low'):.1f} мин; "
            f"отскок именно после дна {avg('rebound_from_low'):.2f}%.",
            f"Вернулись к цене выхода: {recovered(complete,'return_to_exit_minutes')}/{n}; "
            f"к цене входа: {entry}/{n}; достигли +0,7% от входа: "
            f"{recovered(complete,'target07_minutes')}/{n}.",
            f'Не вернулись к входу за полное окно: {n-entry}/{n}.',
        ]
    if partial:
        lines.append(f"На неполных путях возврат к входу зафиксирован: "
                     f"{recovered(partial,'return_to_entry_minutes')}/{len(partial)}; "
                     f"+0,7% зафиксировано: {recovered(partial,'target07_minutes')}/{len(partial)}. "
                     'Остальные исходы неизвестны; экстремумы не включены в средние.')
    lines += ['Возврат учитывается во всём окне, даже до нового дна. '
              'Он не доказывает, что более широкий стоп сохранил бы позицию.',
              'Для выбора стопа — общий пересчёт от покупки на одинаковых полных путях. '
              'Только аналитика; торговые правила не меняются.']
    return '\n'.join(lines)
