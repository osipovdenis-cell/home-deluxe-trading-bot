"""Fresh paper entries and an exit loop independent of AI/Telegram/network scans."""
import json
import math
from queue import Empty, SimpleQueue
import threading
import time

import httpx


def fetch_quote(client, symbol, timeout=1.0):
    started = time.monotonic()
    response = client.get('/api/v3/ticker/bookTicker', params={'symbol':symbol},
                          timeout=timeout)
    response.raise_for_status()
    data = response.json()
    received = time.time()
    bid, ask = float(data['bidPrice']), float(data['askPrice'])
    if data.get('symbol') != symbol or not all(math.isfinite(p) and p > 0 for p in (bid,ask)) or ask < bid:
        raise ValueError('некорректная котировка bid/ask')
    if time.monotonic()-started > timeout:
        raise ValueError('получение котировки заняло слишком долго')
    return received, bid, ask


def fresh_entry(client, symbol, signal_price, stop_percent, max_spread_percent):
    received, bid, ask = fetch_quote(client, symbol, timeout=2.0)
    spread = (ask/bid-1)*100
    drift = (ask/signal_price-1)*100
    if spread > max_spread_percent:
        raise ValueError(f'спред перед покупкой {spread:.3f}% > {max_spread_percent:g}%')
    # Reuse the existing risk distance: a materially moved signal needs a new decision.
    if abs(drift) >= stop_percent:
        raise ValueError(f'цена после анализа изменилась на {drift:+.2f}%; нужен новый сигнал')
    return received, bid, ask


class PositionExitWorker:
    """Own SQLite connection, ordered best bids; reports delivered by main later.

    This is the only runtime caller of update_positions. No AI or Telegram call
    runs here. REST is a bounded fallback for stale per-symbol book streams.
    """
    def __init__(self, trader_factory, stream, base_url, client_factory=None):
        self.trader_factory, self.stream, self.base_url = trader_factory, stream, base_url
        self.client_factory = client_factory or (lambda: httpx.Client(base_url=base_url, timeout=1.0))
        self.messages, self.errors = SimpleQueue(), SimpleQueue()
        self._stop, self._ready = threading.Event(), threading.Event()
        self._thread = None
        self._heartbeat = 0.0
        self._previous = {}
        self._error_at = {}
        self._fallback_at = {}

    def healthy(self):
        return bool(self._thread and self._thread.is_alive()
                    and self._ready.is_set() and time.monotonic()-self._heartbeat < 3)

    def start(self):
        self._thread = threading.Thread(target=self._run, name='position-exits', daemon=True)
        self._thread.start()
        if not self._ready.wait(5):
            raise RuntimeError('Не запустился отдельный обработчик выходов')

    def _error(self, key, text):
        now = time.monotonic()
        if now-self._error_at.get(key, -math.inf) >= 30:
            self._error_at[key] = now
            self.errors.put(text)
            print(f'Контроль выходов: {text}', flush=True)

    def handle_quote(self, trader, observed_at, symbol, bid, source='bookTicker'):
        processed = time.time()
        if not math.isfinite(bid) or bid <= 0 or observed_at > processed:
            return
        if processed-observed_at > 2:
            self._error('stale:'+symbol, f'{symbol}: устаревшая цена выхода, возраст {processed-observed_at:.2f} с')
            return  # Never retrospectively fill a stale quote as if it were executable now.
        row = trader.connection.execute(
            "SELECT * FROM paper_positions WHERE symbol=? AND status='OPEN' ORDER BY id LIMIT 1",
            (symbol,),
        ).fetchone()
        if row is None or observed_at < row['opened_at']:
            return
        previous = self._previous.get(row['id'])
        if previous and observed_at < previous[0]:
            return
        self._previous[row['id']] = (observed_at,bid)
        notices = trader.update_positions({symbol:bid}, processed)
        for notice in notices:
            recorded = time.time()
            change = (bid/row['entry_price']-1)*100
            diagnostic = dict(position_id=row['id'], observed_at=observed_at,
                              processed_at=processed, recorded_at=recorded, source=source, bid=bid,
                              previous=previous, stop_percent=trader.stop_loss_percent,
                              price_change_percent=change,
                              handling_delay_ms=(recorded-observed_at)*1000,
                              cost_percent=trader.round_trip_cost_percent,
                              overshoot_percent=max(0,-change-trader.stop_loss_percent))
            try:
                trader.connection.execute(
                    'INSERT INTO paper_exit_diagnostics(position_id,timestamp,payload) VALUES(?,?,?)',
                    (row['id'],processed,json.dumps(diagnostic)),
                )
                trader.connection.commit()
            except Exception as error:
                trader.connection.rollback()
                self._error('diagnostics',f'не записана диагностика выхода {symbol}: {error}')
            valuation=self.stream.latest()
            valuation[symbol]=bid
            text=trader.notice_telegram_text(notice,valuation,processed)
            if notice.reason == 'стоп-лосс':
                text += (f"\nКонтроль стопа: порог −{trader.stop_loss_percent:g}%; "
                         f"движение цены {change:+.2f}%; издержки {trader.round_trip_cost_percent:g}%. "
                         f"Обработка котировки: {(recorded-observed_at)*1000:.0f} мс ({source}).")
            self.messages.put(text)
            if notice.remaining_percent == 0:
                self._previous.pop(row['id'],None)

    def _run(self):
        trader = client = None
        try:
            trader = self.trader_factory()
            trader.connection.execute('''CREATE TABLE IF NOT EXISTS paper_exit_diagnostics (
                id INTEGER PRIMARY KEY,position_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,payload TEXT NOT NULL)''')
            trader.connection.commit()
            trader.connection.execute('PRAGMA busy_timeout=250')
            client = self.client_factory()
            self._heartbeat = time.monotonic()
            self._ready.set()
            while not self._stop.is_set():
                try:
                    self.stream.set_symbols(trader.open_symbols())
                    events, overflow = self.stream.drain_batch()
                    if overflow:
                        self._error('overflow','переполнение очереди котировок; история неполна')
                        # Do not replay the surviving suffix as a complete path.
                        latest={symbol:(at,symbol,bid) for at,symbol,bid in events}
                        events=sorted(latest.values())
                    for at,symbol,bid in events:
                        self.handle_quote(trader,at,symbol,bid)
                    now=time.time()
                    # At most one short REST call per iteration, then drain bids again.
                    for symbol in self.stream.stale_symbols(now):
                        if now-self._fallback_at.get(symbol,0) < 2:
                            continue
                        self._fallback_at[symbol]=now
                        try:
                            at,bid,_=fetch_quote(client,symbol)
                            self.handle_quote(trader,at,symbol,bid,'REST bookTicker')
                        except Exception as error:
                            self._error('rest:'+symbol,f'{symbol}: нет свежей резервной котировки: {error}')
                        break
                    self._heartbeat=time.monotonic()
                except Exception as error:
                    trader.connection.rollback()
                    self._error('worker',str(error))
                self._stop.wait(.02)
        except Exception as error:
            self._error('startup',str(error))
        finally:
            if client is not None:
                client.close()
            if trader is not None:
                trader.close()

    def drain(self):
        def items(queue):
            result=[]
            while True:
                try:
                    result.append(queue.get_nowait())
                except Empty:
                    return result
        return items(self.messages), items(self.errors)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=7)
