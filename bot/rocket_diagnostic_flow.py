"""Persistent public-data subscriptions for diagnostics, never for order entry."""
import json
import math
import threading
import time
from dataclasses import asdict

from bot.rocket_quote_stream import RocketQuoteStream
from bot.streams import LeaderOrderFlowStream


class DiagnosticFlowStream(RocketQuoteStream):
    max_symbols = 40

    def __init__(self):
        super().__init__()
        self.flow = LeaderOrderFlowStream(max_symbols=self.max_symbols)
        # Hold a single snapshot across price windows and flow calculations.
        self.flow._lock = threading.RLock()
        self._ingest_lock = threading.RLock()
        self._trade_ids = {}
        self._last_gaps = {}

    @staticmethod
    def streams(symbols):
        return [channel for s in sorted(symbols) for channel in
                (s.lower()+'@aggTrade', s.lower()+'@bookTicker',
                 s.lower()+'@depth5', s.lower()+'@depth@100ms')]

    def set_symbols(self, symbols):
        wanted=tuple(dict.fromkeys(str(s).upper() for s in symbols))[:self.max_symbols]
        with self._ingest_lock:
            super().set_symbols(wanted)
            self.flow.set_symbols(wanted)  # No flow thread is started: one socket owns subscriptions.
            self._trade_ids={s:i for s,i in self._trade_ids.items() if s in wanted}

    def on_quote(self, at, symbol, bid, ask, update):
        self.flow.ingest(dict(s=symbol,b=str(bid),a=str(ask),B='0',A='0'),at)

    def ingest(self, payload, received_at=None):
        now=time.time() if received_at is None else received_at
        item=json.loads(payload) if isinstance(payload,(str,bytes)) else payload
        data=item.get('data',item)
        event,symbol=data.get('e'),str(data.get('s','')).upper()
        with self._ingest_lock:
            if event in ('aggTrade','depthUpdate'):
                with self._lock:
                    if symbol not in self._symbols:
                        return
                if event=='aggTrade':
                    price,quantity=float(data['p']),float(data['q'])
                    if not all(math.isfinite(v) and v>0 for v in (price,quantity,price*quantity)):
                        self.interrupted([symbol],now)
                        return
                    ident=int(data['a'])
                    if ident<=self._trade_ids.get(symbol,-1):
                        return
                    previous=self._trade_ids.get(symbol)
                    if previous is not None and ident != previous+1:
                        self.interrupted([symbol],now)
                    self._trade_ids[symbol]=ident
                self.flow.ingest(data,now)
            else:
                super().ingest(item,now)

    def interrupted(self, symbols, at=None):
        at=time.time() if at is None else at
        symbols=tuple(symbols)
        with self._ingest_lock:
            super().interrupted(symbols,at)
            self.flow.interrupted(symbols,at)
            for symbol in symbols:
                self._trade_ids.pop(symbol,None)
                self._last_gaps[symbol]=at

    def entry_probe(self, symbol, now):
        symbol=symbol.upper()
        with self._ingest_lock, self.flow._lock:
            # Caller timestamps can precede a just-arrived event by microseconds.
            # Copy only the prefix known at `now` into the snapshot calculator.
            view=LeaderOrderFlowStream(max_symbols=1)
            for key in ('_trades','_quotes','_depth_changes'):
                getattr(view,key)[symbol].extend(r for r in getattr(self.flow,key).get(symbol,()) if r[0]<=now)
            probe=view.entry_probe(symbol,now)
            observed=view.snapshot(symbol,now)
            probe['observed_flow']=asdict(observed) if observed else None
            probe['feed_version']=2
            reasons=probe['freshness_reasons']
            if symbol not in self.flow._symbols:
                reasons.append('нет подписки на монету')
            gap=self._last_gaps.get(symbol)
            if gap is not None and now-gap<60:
                reasons.append('окно 60с после разрыва ещё не накоплено')
                probe['fresh']=False
                probe['snapshot']=None
            if not probe['recovery_windows']['complete']:
                reasons.append('неполное окно котировок/сделок за 10с')
            return probe

    def drain(self):
        quotes,overflow,gaps=self.drain_quotes()
        return [(at,symbol,bid) for at,symbol,bid,ask in quotes],overflow,gaps
