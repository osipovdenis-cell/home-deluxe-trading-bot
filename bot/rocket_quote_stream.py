"""Continuous quotes for rocket analytics; never connected to the order engine.

bookTicker preserves intrasecond changes. Actual depth5 snapshots supply a
once-per-second quote even when the best prices don't change. No forward fill,
REST backfill or interpolation. Known connection interruptions stay explicit.
"""
import json
import math
import threading
import time
from collections import deque

from websockets.sync.client import connect
from bot.streams import BINANCE_STREAM_BASE_URL


class RocketQuoteStream:
    max_symbols = 100

    def __init__(self):
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._symbols = set()
        self._updates = {}
        self._quotes = deque(maxlen=100000)
        self._gaps = deque(maxlen=10000)
        self._overflow = False
        self._stats = dict(book_quotes=0, depth_quotes=0, reconnects=0,
                           connected=False, last_quote_at=0.0, invalid_messages=0)

    def set_symbols(self, symbols):
        wanted = {str(s).upper() for s in symbols}
        if len(wanted) > self.max_symbols:
            raise ValueError('too many rocket quote subscriptions')
        with self._lock:
            for symbol in self._symbols - wanted:
                self._updates.pop(symbol, None)
            self._symbols = wanted

    @staticmethod
    def streams(symbols):
        return [stream for s in sorted(symbols) for stream in
                (s.lower() + '@bookTicker', s.lower() + '@depth5')]

    def sync_subscriptions(self, ws, subscribed, request_id, pending):
        with self._lock:
            wanted = set(self._symbols)
        for method, symbols in (('UNSUBSCRIBE', subscribed - wanted),
                                ('SUBSCRIBE', wanted - subscribed)):
            if symbols:
                request_id += 1
                ws.send(json.dumps(dict(method=method, params=self.streams(symbols), id=request_id)))
                pending[request_id] = time.monotonic()
        return wanted, request_id

    def ingest(self, payload, received_at=None):
        now = time.time() if received_at is None else received_at
        item = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        data = item.get('data', item)
        depth = 'lastUpdateId' in data
        symbol = item.get('stream', '').split('@')[0].upper() if depth else data.get('s')
        if not symbol:
            return
        try:
            if depth:
                bid, ask = float(data['bids'][0][0]), float(data['asks'][0][0])
                update = int(data['lastUpdateId'])
            else:
                bid, ask = float(data['b']), float(data['a'])
                update = int(data['u'])
            if not all(math.isfinite(x) and x > 0 for x in (bid, ask)) or ask < bid:
                raise ValueError('invalid quote')
        except (KeyError, IndexError, TypeError, ValueError, OverflowError):
            with self._lock:
                self._stats['invalid_messages'] += 1
            self.interrupted([symbol], now)
            return
        with self._lock:
            if symbol not in self._symbols or update < self._updates.get(symbol, -1):
                return  # A delayed depth snapshot cannot undo a newer book tick.
            self._updates[symbol] = update
            if len(self._quotes) == self._quotes.maxlen:
                self._overflow = True
            self._quotes.append((now, symbol, bid, ask))
            self._stats['depth_quotes' if depth else 'book_quotes'] += 1
            self._stats['last_quote_at'] = now
        self.on_quote(now, symbol, bid, ask, update)

    def on_quote(self, at, symbol, bid, ask, update):
        """Optional diagnostic consumer; called only for validated, ordered quotes."""
        pass

    def interrupted(self, symbols, at=None):
        at = time.time() if at is None else at
        with self._lock:
            for symbol in symbols:
                if len(self._gaps) == self._gaps.maxlen:
                    self._overflow = True
                self._gaps.append((at, symbol))
                self._updates.pop(symbol, None)

    def drain_quotes(self):
        with self._lock:
            result = list(self._quotes), self._overflow, list(self._gaps)
            self._quotes.clear()
            self._gaps.clear()
            self._overflow = False
            return result

    def health(self):
        with self._lock:
            return dict(self._stats, subscribed_symbols=len(self._symbols))

    def start(self):
        self._thread = threading.Thread(target=self.run, name='rocket-daily-quotes', daemon=True)
        self._thread.start()

    def run(self):
        delay = 1
        while not self._stop.is_set():
            subscribed, pending, request_id = set(), {}, 0
            try:
                with connect(BINANCE_STREAM_BASE_URL + '/stream', open_timeout=10,
                             close_timeout=2, ping_interval=20, ping_timeout=10) as ws:
                    with self._lock:
                        self._stats['connected'] = True
                    last_sync = -math.inf
                    while not self._stop.is_set():
                        now = time.monotonic()
                        # At most two control messages/sec, leaving room for pong.
                        if now - last_sync >= 1:
                            subscribed, request_id = self.sync_subscriptions(ws, subscribed, request_id, pending)
                            last_sync = now
                        if any(now - sent > 5 for sent in pending.values()):
                            raise TimeoutError('subscription acknowledgement missing')
                        try:
                            message = json.loads(ws.recv(timeout=.2))
                        except TimeoutError:
                            continue
                        if 'id' in message:
                            if message.get('result', 'error') is not None:
                                raise ValueError('subscription rejected')
                            pending.pop(message['id'], None)
                            delay = 1
                        else:
                            self.ingest(message)
            except Exception:
                # The public log must never include payloads or account data.
                with self._lock:
                    self._stats['reconnects'] += 1
            finally:
                self.interrupted(subscribed)
                with self._lock:
                    self._stats['connected'] = False
            if self._stop.wait(delay):
                break
            delay = min(30, delay * 2)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
