"""Public-data transport for the leader flow; never submits orders."""
import json
import math
import time

from bot.rocket_quote_stream import RocketQuoteStream


class OrderFlowTransport(RocketQuoteStream):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner
        self.max_symbols = owner.max_symbols

    @staticmethod
    def streams(symbols):
        return [channel for s in sorted(symbols) for channel in
                (s.lower()+'@aggTrade', s.lower()+'@bookTicker',
                 s.lower()+'@depth5', s.lower()+'@depth@100ms')]

    def ingest(self, payload, received_at=None):
        at = time.time() if received_at is None else received_at
        item = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        data = item.get('data', item)
        if data.get('e') in ('aggTrade', 'depthUpdate'):
            if data['e'] == 'aggTrade':
                price, quantity = float(data['p']), float(data['q'])
                if not all(math.isfinite(v) and v > 0 for v in (price, quantity, price*quantity)) or not isinstance(data['m'], bool):
                    raise ValueError('invalid trade')
            self.owner.ingest(data, at)
        else:
            super().ingest(item, at)

    def on_quote(self, at, symbol, bid, ask, update):
        self.owner.ingest(dict(s=symbol, b=str(bid), a=str(ask), B='0', A='0'), at)
        # This transport feeds flow directly. Do not retain a second unused
        # quote history (the analytics transports still retain every tick).
        with self._lock:
            self._quotes.clear()

    def interrupted(self, symbols, at=None):
        symbols = tuple(symbols)
        super().interrupted(symbols, at)
        self.owner.interrupted(symbols, at)

