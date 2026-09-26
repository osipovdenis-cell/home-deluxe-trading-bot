"""Bounded, stable symbol groups with independent quote/trade/depth channels."""
import threading
import time


class ShardedMarketStream:
    symbols_per_group = 3

    def __init__(self, owner):
        self.owner = owner
        self._lock = threading.RLock()
        self.running = False
        self.parts = []
        self.assignments = {}
        self.families = sorted({self.family(s) for s in owner.streams(['X'])})
        if 'quotes' in self.families:
            self.families.insert(self.families.index('quotes'), 'quotes')

    @staticmethod
    def family(stream):
        channel = stream.split('@', 1)[1]
        return 'trades' if channel == 'aggTrade' else ('depth' if channel.startswith('depth@') else 'quotes')

    def _make_part(self, kind, group, replica):
        from bot.rocket_quote_stream import RocketQuoteStream
        owner, family, shards = self.owner, self.family, self
        class Partition(RocketQuoteStream):
            @staticmethod
            def streams(symbols):
                channels = [s for s in owner.streams(symbols) if family(s) == kind]
                if kind == 'quotes':
                    channels += [s.lower()+'@kline_1s' for s in sorted(symbols)]
                return channels
            def run(self):
                self._ingest_worker = owner._ingest_worker
                self.run_socket()
            def ingest(self, payload, received_at=None):
                owner.ingest(payload, received_at)
            def interrupted(self, symbols, at=None):
                owner.interrupted(symbols, at)
            def delayed(self, symbols, at=None):
                owner.delayed(symbols, at)
            def timely_message(self, message, received_at):
                timely = super().timely_message(message, received_at)
                data = message.get('data', message)
                if timely and kind == 'quotes' and 'lastUpdateId' in data:
                    symbol = message.get('stream', '').split('@')[0].upper()
                    with self._lock:
                        self._good_quotes[symbol] = received_at
                return timely
            def queue_delay(self, symbols, at):
                uncovered = [s for s in symbols if not shards.quote_peer_ready(self, s, at)]
                if uncovered:
                    super().queue_delay(uncovered, at)
            def queue_interruption(self, symbols, at=None):
                at = time.time() if at is None else at
                uncovered = [s for s in symbols if kind != 'quotes' or not shards.quote_peer_ready(self, s, at)]
                if uncovered:
                    super().queue_interruption(uncovered, at)
        part = Partition()
        part.max_symbols = owner.max_symbols
        part.require_clock = kind == 'quotes'
        part._family, part._group = kind, group
        part._good_quotes = {}
        if kind == 'quotes':
            part._endpoint_index = replica % len(part.base_urls)
        return part

    def quote_peer_ready(self, source, symbol, at):
        with self._lock:
            peers = tuple(p for p in self.parts if p is not source
                          and p._group == source._group and p._family == 'quotes')
        for peer in peers:
            if peer.subscription_confirmed(symbol):
                with peer._lock:
                    if 0 <= at-peer._good_quotes.get(symbol, -float('inf')) <= 1:
                        return True
        return False

    def _start(self, part):
        part._thread = threading.Thread(target=part.run, name='market-partition', daemon=True)
        part._thread.start()

    def index(self, symbol):
        return self.assignments[symbol]*len(self.families)

    def set_symbols(self, symbols):
        with self._lock:
            wanted = set(symbols)
            self.assignments = {s:g for s,g in self.assignments.items() if s in wanted}
            groups = [set() for _ in range(len(self.parts)//len(self.families))]
            for symbol, group in self.assignments.items():
                groups[group].add(symbol)
            for symbol in sorted(wanted-self.assignments.keys()):
                available = [i for i,g in enumerate(groups) if len(g) < self.symbols_per_group]
                if available:
                    group = min(available, key=lambda i: len(groups[i]))
                else:
                    group = len(groups)
                    groups.append(set())
                    new = [self._make_part(kind, group, self.families[:i].count(kind))
                           for i, kind in enumerate(self.families)]
                    self.parts.extend(new)
                    if self.running:
                        for part in new:
                            self._start(part)
                groups[group].add(symbol)
                self.assignments[symbol] = group
            for i, group in enumerate(groups):
                for offset in range(len(self.families)):
                    self.parts[i*len(self.families)+offset].set_symbols(group)

    def confirmed(self, symbol):
        with self._lock:
            if symbol not in self.assignments:
                return False
            start = self.index(symbol)
            parts = tuple(self.parts[start:start+len(self.families)])
        return all(any(p.subscription_confirmed(symbol) for p in parts if p._family == kind)
                   for kind in set(self.families))

    def run(self):
        with self._lock:
            self.running = True
            for part in self.parts:
                self._start(part)
        try:
            self.owner._stop.wait()
        finally:
            with self._lock:
                self.running = False
                parts = tuple(self.parts)
            for part in parts:
                part._stop.set()
            for part in parts:
                part.close()

    def health(self):
        with self._lock:
            parts = tuple(self.parts)
            symbols = tuple(self.assignments)
        rows = [p.health() for p in parts]
        active = [r for r in rows if r['subscribed_symbols']]
        groups = {(p._group, p._family) for p in parts if p._symbols}
        connected = bool(groups) and all(any(r['connected'] for p,r in zip(parts,rows)
                          if (p._group,p._family)==group) for group in groups)
        summed = ['reconnects','subscription_reconciliations','control_timeouts',
                  'unmatched_replies','wrapped_replies','subscription_replies',
                  'pending_subscription_requests','stale_data_messages','idle_liveness_checks']
        result = {k: sum(r.get(k,0) for r in rows) for k in summed}
        for key in ['subscription_ack_max_seconds','max_event_lag_seconds']:
            result[key] = max((r.get(key,0) for r in rows), default=0)
        reasons = {}
        for r in rows:
            for key, value in r['disconnect_reasons'].items():
                reasons[key] = reasons.get(key,0)+value
        result['confirmed_symbols'] = sum(self.confirmed(s) for s in symbols)
        latest = max(rows, key=lambda r: r.get('last_disconnect_at') or 0, default={})
        for key in ['last_disconnect_at','last_error_type','last_error_phase','last_close_code','last_sent_close_code']:
            result[key] = latest.get(key)
        result.update(connected=connected,
                      disconnect_reasons=reasons, partitions=rows,
                      symbols_per_group=self.symbols_per_group, quote_sampling_ms=100, quote_replicas=2)
        return result
