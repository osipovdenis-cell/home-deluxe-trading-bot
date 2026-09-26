"""Stable symbol partitions: unrelated symbols keep their own socket history."""
import threading
import zlib


class ShardedMarketStream:
    count = 4

    def __init__(self, owner):
        from bot.rocket_quote_stream import RocketQuoteStream
        self.owner = owner
        class Partition(RocketQuoteStream):
            streams = staticmethod(owner.streams)
            def ingest(self, payload, received_at=None):
                owner.ingest(payload, received_at)
            def interrupted(self, symbols, at=None):
                owner.interrupted(symbols, at)
        self.parts = [Partition() for _ in range(self.count)]
        for part in self.parts:
            part.max_symbols = owner.max_symbols

    @classmethod
    def index(cls, symbol):
        return zlib.crc32(symbol.encode('utf-8')) % cls.count

    def set_symbols(self, symbols):
        groups = [set() for _ in self.parts]
        for symbol in symbols:
            groups[self.index(symbol)].add(symbol)
        for part, group in zip(self.parts, groups):
            part.set_symbols(group)

    def confirmed(self, symbol):
        return self.parts[self.index(symbol)].subscription_confirmed(symbol)

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
                  'confirmed_symbols','pending_subscription_requests','ingest_queued',
                  'ingest_overflows','ingest_errors','stale_data_messages']
        result = {k: sum(r.get(k,0) for r in rows) for k in summed}
        for key in ['subscription_ack_max_seconds','ingest_max_lag_seconds','max_event_lag_seconds']:
            result[key] = max((r.get(key,0) for r in rows), default=0)
        reasons = {}
        for r in rows:
            for key, value in r['disconnect_reasons'].items():
                reasons[key] = reasons.get(key,0)+value
        result.update(connected=bool(active) and all(r['connected'] for r in active),
                      disconnect_reasons=reasons, partitions=rows)
        return result
