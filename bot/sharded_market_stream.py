"""Stable symbol partitions: unrelated symbols keep their own socket history."""
import threading
import zlib


class ShardedMarketStream:
    count = 4

    def __init__(self, owner):
        from bot.rocket_quote_stream import RocketQuoteStream
        self.owner = owner
        def family(stream):
            channel = stream.split('@', 1)[1]
            return 'trades' if channel == 'aggTrade' else ('depth' if channel.startswith('depth@') else 'quotes')
        self.families = sorted({family(s) for s in owner.streams(['X'])})
        self.parts = []
        def make_part(kind):
            class Partition(RocketQuoteStream):
                @staticmethod
                def streams(symbols):
                    return [s for s in owner.streams(symbols) if family(s) == kind]
                def run(self):
                    # All channels share one receive-time FIFO and gap ordering.
                    self._ingest_worker = owner._ingest_worker
                    self.run_socket()
                def ingest(self, payload, received_at=None):
                    owner.ingest(payload, received_at)
                def interrupted(self, symbols, at=None):
                    owner.interrupted(symbols, at)
            part = Partition()
            part.max_symbols = owner.max_symbols
            return part
        for _ in range(self.count):
            self.parts.extend(make_part(kind) for kind in self.families)

    def index(self, symbol):
        return (zlib.crc32(symbol.encode('utf-8')) % self.count)*len(self.families)

    def set_symbols(self, symbols):
        groups = [set() for _ in self.parts]
        for symbol in symbols:
            for offset in range(len(self.families)):
                groups[self.index(symbol)+offset].add(symbol)
        for part, group in zip(self.parts, groups):
            part.set_symbols(group)

    def confirmed(self, symbol):
        return all(self.parts[self.index(symbol)+offset].subscription_confirmed(symbol)
                   for offset in range(len(self.families)))

    def run(self):
        for i, part in enumerate(self.parts):
            part._thread = threading.Thread(target=part.run, name=f'market-partition-{i}', daemon=True)
            part._thread.start()
        try:
            self.owner._stop.wait()
        finally:
            # Signal every partition before joining any one of them.
            for part in self.parts:
                part._stop.set()
            for part in self.parts:
                part.close()

    def health(self):
        rows = [p.health() for p in self.parts]
        active = [r for r in rows if r['subscribed_symbols']]
        summed = ['reconnects','subscription_reconciliations','control_timeouts',
                  'unmatched_replies','wrapped_replies','subscription_replies',
                  'pending_subscription_requests','stale_data_messages']
        result = {k: sum(r.get(k,0) for r in rows) for k in summed}
        for key in ['subscription_ack_max_seconds','max_event_lag_seconds']:
            result[key] = max((r.get(key,0) for r in rows), default=0)
        reasons = {}
        for r in rows:
            for key, value in r['disconnect_reasons'].items():
                reasons[key] = reasons.get(key,0)+value
        with self.owner._lock:
            symbols = set(self.owner._symbols)
        result['confirmed_symbols'] = sum(self.confirmed(s) for s in symbols)
        latest = max(rows, key=lambda r: r.get('last_disconnect_at') or 0)
        for key in ['last_disconnect_at','last_error_type','last_error_phase','last_close_code','last_sent_close_code']:
            result[key] = latest.get(key)
        result.update(connected=bool(active) and all(r['connected'] for r in active),
                      disconnect_reasons=reasons, partitions=rows)
        return result
