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
from urllib.parse import quote

from websockets.sync.client import connect
from bot.streams import BINANCE_STREAM_BASE_URL


class QuoteIngestQueue:
    """Keep subscription replies readable while consumers calculate diagnostics.

    Data and gap markers share one FIFO and retain the receive timestamp. A
    bounded overflow is an explicit unknown interval, never a silently lost tick.
    """
    def __init__(self, stream, capacity=50000):
        self.stream, self.capacity = stream, capacity
        self.pending = deque()
        self.ready = threading.Condition()
        self.stopping = False
        self.overflows = self.errors = 0
        self.max_lag = 0.
        self.thread = threading.Thread(target=self.run, name='rocket-quote-ingest', daemon=True)

    def put(self, kind, payload, at):
        with self.ready:
            if len(self.pending) >= self.capacity:
                first = self.pending[0][2]
                with self.stream._lock:
                    affected = set(self.stream._symbols)
                for previous_kind, previous_payload, _ in self.pending:
                    if previous_kind == 'gap':
                        affected.update(previous_payload)
                    elif isinstance(previous_payload, dict):
                        data = previous_payload.get('data', previous_payload)
                        symbol = data.get('s') or previous_payload.get('stream', '').split('@')[0]
                        if symbol:
                            affected.add(str(symbol).upper())
                self.pending.clear()
                self.pending.append(('gap', affected, first))
                self.overflows += 1
            self.pending.append((kind, payload, at))
            self.ready.notify()

    def health(self):
        with self.ready:
            return dict(ingest_queued=len(self.pending), ingest_overflows=self.overflows,
                        ingest_errors=self.errors, ingest_max_lag_seconds=self.max_lag)

    def run(self):
        while True:
            with self.ready:
                self.ready.wait_for(lambda: self.pending or self.stopping)
                if not self.pending:
                    return
                kind, payload, at = self.pending.popleft()
            try:
                if kind == 'gap':
                    self.stream.interrupted(payload, at)
                else:
                    self.stream.ingest(payload, at)
                with self.ready:
                    self.max_lag = max(self.max_lag, time.time()-at)
            except Exception:
                with self.ready:
                    self.errors += 1
                with self.stream._lock:
                    affected = set(self.stream._symbols)
                self.stream.interrupted(affected, at)

    def close(self):
        with self.ready:
            self.stopping = True
            self.ready.notify()
        self.thread.join(timeout=3)


class RocketQuoteStream:
    max_symbols = 100
    idle_timeout = 30
    subscription_timeout = 30

    def __init__(self):
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._symbols = set()
        self._updates = {}
        self._quotes = deque(maxlen=100000)
        self._gaps = deque(maxlen=10000)
        self._overflow = False
        self._ingest_worker = None
        self._stats = dict(book_quotes=0, depth_quotes=0, reconnects=0,
                           connected=False, last_quote_at=0.0, invalid_messages=0,
                           version='rocket-quotes-v5', subscription_reconciliations=0,
                           subscription_replies=0, subscription_ack_max_seconds=0.,
                           confirmed_symbols=0, pending_subscription_requests=0, started_at=time.time(),
                           last_disconnect_at=None, last_error_type=None,
                           last_error_phase=None, last_close_code=None,
                           disconnect_reasons={})

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
        # Only one outstanding mutation. Membership may change while its ACK is
        # in flight; reconcile the new desired set after that ACK, not optimistically.
        if pending:
            return subscribed, request_id
        with self._lock:
            wanted = set(self._symbols)
        for method, symbols in (('UNSUBSCRIBE', subscribed - wanted),
                                ('SUBSCRIBE', wanted - subscribed)):
            if symbols:
                request_id += 1
                ws.send(json.dumps(dict(method=method, params=self.streams(symbols), id=request_id)))
                pending[request_id] = dict(sent=time.monotonic(), method=method, symbols=set(symbols))
                break
        return subscribed, request_id

    def reconcile_timeout(self, ws, subscribed, request_id, pending, now):
        expired = [p for p in pending.values() if now-p['sent'] > self.subscription_timeout]
        if not expired:
            return request_id
        if any(p['method']=='LIST_SUBSCRIPTIONS' for p in pending.values()):
            if any(p['method']=='LIST_SUBSCRIPTIONS' for p in expired):
                raise TimeoutError('subscription reconciliation missing')
            return request_id
        # An ACK can be lost while quotes keep flowing. Verify server state once;
        # do not tear down every healthy subscription just to repeat the request.
        request_id += 1
        ws.send(json.dumps(dict(method='LIST_SUBSCRIPTIONS', id=request_id)))
        pending[request_id] = dict(sent=now, method='LIST_SUBSCRIPTIONS', symbols=set())
        with self._lock:
            self._stats['subscription_reconciliations'] += 1
        return request_id

    def subscription_reply(self, message, subscribed, pending):
        request = pending.get(message.get('id'))
        if request is None:
            return subscribed  # Late reply after an authoritative reconciliation.
        with self._lock:
            self._stats['subscription_replies'] += 1
            self._stats['subscription_ack_max_seconds'] = max(
                self._stats['subscription_ack_max_seconds'], time.monotonic()-request['sent'])
        if request['method']=='LIST_SUBSCRIPTIONS':
            streams = message.get('result')
            if not isinstance(streams,list) or not all(isinstance(s,str) for s in streams):
                raise ValueError('subscription reconciliation rejected')
            actual = set(streams)
            with self._lock:
                wanted = set(self._symbols)
            affected = subscribed | wanted | set().union(*(p['symbols'] for p in pending.values()))
            confirmed = {s for s in affected if set(self.streams([s])) <= actual}
            # A missing channel means an unknown interval even if another channel
            # for that symbol was still delivering quotes.
            self.queue_interruption(affected-confirmed)
            pending.clear()
            return confirmed
        if message.get('result','error') is not None:
            raise ValueError('subscription rejected')
        pending.pop(message['id'])
        if request['method']=='SUBSCRIBE':
            return subscribed | request['symbols']
        return subscribed - request['symbols']

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
        # Lock order is queue -> stream; never hold stream while taking the queue.
        ingest = self._ingest_worker.health() if self._ingest_worker else {}
        with self._lock:
            return dict(self._stats, disconnect_reasons=dict(self._stats['disconnect_reasons']),
                        subscribed_symbols=len(self._symbols), **ingest)

    def queue_interruption(self, symbols, at=None):
        at = time.time() if at is None else at
        if self._ingest_worker is None:
            self.interrupted(symbols, at)
        else:
            self._ingest_worker.put('gap', set(symbols), at)

    def record_error(self, error, phase):
        # Exception text may contain URLs/payloads; export only bounded metadata.
        name = type(error).__name__
        if name not in ('TimeoutError', 'ConnectionClosedError', 'ConnectionClosedOK',
                        'OSError', 'ValueError', 'JSONDecodeError'):
            name = 'OtherError'
        code = getattr(getattr(error, 'rcvd', None), 'code', None)
        with self._lock:
            reasons = self._stats['disconnect_reasons']
            key = phase + ':' + name
            reasons[key] = reasons.get(key, 0) + 1
            self._stats.update(reconnects=self._stats['reconnects']+1,
                               last_disconnect_at=time.time(), last_error_type=name,
                               last_error_phase=phase,
                               last_close_code=code if isinstance(code, int) else None)

    def start(self):
        self._thread = threading.Thread(target=self.run, name='rocket-daily-quotes', daemon=True)
        self._thread.start()

    def run(self):
        worker = self._ingest_worker = QuoteIngestQueue(self)
        worker.thread.start()
        try:
            self.run_socket()
        finally:
            worker.close()

    def run_socket(self):
        delay = 1
        while not self._stop.is_set():
            with self._lock:
                subscribed = set(self._symbols)
            if not subscribed:
                self._stop.wait(.2)
                continue
            pending, request_id = {}, 0
            phase = 'connect'
            connected_at = None
            try:
                # Binance sends server PINGs; websockets answers them automatically.
                # Avoid a second, short client heartbeat deadline during bursts.
                # A separate idle watchdog still reconnects a stalled data stream.
                # Establish initial subscriptions in the handshake. No initial ACK
                # is required; subsequent membership changes stay on this socket.
                url = BINANCE_STREAM_BASE_URL + '/stream?streams=' + quote('/'.join(self.streams(subscribed)), safe='/@')
                with connect(url, open_timeout=10,
                             close_timeout=2, ping_interval=None, max_queue=256,
                             compression=None) as ws:
                    connected_at = last_received = time.monotonic()
                    with self._lock:
                        self._stats['connected'] = True
                    last_sync = -math.inf
                    while not self._stop.is_set():
                        now = time.monotonic()
                        # At most one mutation/sec, leaving room for ping/pong.
                        if now - last_sync >= 1:
                            phase = 'subscribe'
                            subscribed, request_id = self.sync_subscriptions(ws, subscribed, request_id, pending)
                            last_sync = now
                        phase = 'receive'
                        try:
                            message = json.loads(ws.recv(timeout=.2))
                        except TimeoutError:
                            now = time.monotonic()
                            phase = 'subscription_ack'
                            request_id = self.reconcile_timeout(ws, subscribed, request_id, pending, now)
                            if subscribed and now - last_received > self.idle_timeout:
                                phase = 'idle'
                                raise TimeoutError('market stream idle')
                            continue
                        last_received = time.monotonic()
                        if 'id' in message:
                            phase = 'subscription_ack'
                            subscribed = self.subscription_reply(message, subscribed, pending)
                        else:
                            phase = 'ingest'
                            self._ingest_worker.put('data', message, time.time())
                        phase = 'subscription_ack'
                        request_id = self.reconcile_timeout(ws, subscribed, request_id, pending, last_received)
                        with self._lock:
                            self._stats.update(confirmed_symbols=len(subscribed),
                                               pending_subscription_requests=len(pending))
            except Exception as error:
                if not self._stop.is_set():
                    self.record_error(error, phase)
            finally:
                affected = subscribed | set().union(*(p['symbols'] for p in pending.values()))
                self.queue_interruption(affected)
                with self._lock:
                    self._stats['connected'] = False
            if connected_at is not None and time.monotonic() - connected_at >= 60:
                delay = 1
            if self._stop.wait(delay):
                break
            delay = min(30, delay * 2)

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
