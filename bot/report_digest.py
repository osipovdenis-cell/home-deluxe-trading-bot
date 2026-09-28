"""One persistent 12-hour Telegram digest, delivered off the trading loop."""
from datetime import datetime
from zoneinfo import ZoneInfo
import sqlite3
import time

INTERVAL = 12 * 3600


def render_digest(bundle):
    now = bundle['generated_at_unix']
    stamp = datetime.fromtimestamp(now, ZoneInfo('Europe/Kyiv')).strftime('%d.%m.%Y %H:%M')
    summary = bundle.get('notification_summary', {})
    caption = f'📊 Единый отчёт бота · {stamp} (Киев)\n'
    if summary:
        caption += (f"За последние 12 ч: закрыто {summary['closed']}, "
                    f"прибыльных {summary['wins']}, убыточных {summary['losses']}.\n"
                    f"Результат закрытых сделок: {summary['pnl']:+.3f} USDT.\n"
                    f"Банк: {summary['equity']:.3f} USDT.\n")
    caption += 'Все разделы и подробности — в одном файле. Виртуальная торговля.'
    sections = list(bundle.get('reports', [])) + list(bundle.get('digest_extra_reports', []))
    body = (caption + '\n\nЧастота отправки — раз в 12 часов. '
            'Окно каждого подробного раздела указано в его тексте; '
            'накопительные и суточные разделы сохраняют свои периоды.\n\n'
            + '\n\n'.join(sections))
    return caption, body.encode('utf-8')


class DigestSender:
    def __init__(self, db_path, send, clock=time.time, on_sent=None):
        self.db_path, self.send, self.clock = db_path, send, clock
        self.on_sent = on_sent

    def __call__(self, bundle):
        now = self.clock()
        with sqlite3.connect(self.db_path, timeout=5) as db:
            db.execute('CREATE TABLE IF NOT EXISTS telegram_digest_schedule '
                       '(id INTEGER PRIMARY KEY CHECK(id=1), next_at REAL NOT NULL, last_sent_at REAL)')
            db.execute('INSERT OR IGNORE INTO telegram_digest_schedule(id,next_at) VALUES(1,?)',
                       (now + INTERVAL,))
            next_at = db.execute('SELECT next_at FROM telegram_digest_schedule WHERE id=1').fetchone()[0]
        if now < next_at:
            return next_at - now
        caption, document = render_digest(bundle)
        self.send(caption, document)  # Failed sends leave the due timestamp intact.
        sent_at = self.clock()
        with sqlite3.connect(self.db_path, timeout=5) as db:
            db.execute('UPDATE telegram_digest_schedule SET next_at=?,last_sent_at=? WHERE id=1',
                       (sent_at + INTERVAL, sent_at))
        if self.on_sent is not None:
            try:
                self.on_sent(bundle)
            except Exception as error:
                # Delivery is already committed; maintenance cannot resend it.
                print('Digest maintenance failed: ' + type(error).__name__, flush=True)
        return INTERVAL
