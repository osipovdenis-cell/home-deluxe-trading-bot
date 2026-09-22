"""Frozen pre-decision price/flow structure. Diagnostics only, no order access."""
import json
import math
import time
from statistics import median

from bot.rocket_quote_stream import RocketQuoteStream

VERSION = 'rocket-structure-v1'
LOOKBACK = 300


def unknown(at, reason):
    return dict(version=VERSION, at=at, state='UNKNOWN', reason=reason)


def describe(chart, at):
    """5-second completed bars: time, bid OHLC, executed buy/sell USDT.

    A low is confirmed by its right neighbour, already closed at decision time.
    Always use the latest two confirmed lows; never choose the nicest pattern.
    """
    lows = [i for i in range(1, len(chart)-1)
            if chart[i][3] < chart[i-1][3] and chart[i][3] < chart[i+1][3]]
    last, prior = chart[-1], chart[-2]
    f = dict(price_5s=(last[4]/prior[4]-1)*100,
             two_buy_windows=all(b[5] > b[6] for b in (prior, last)),
             higher_low_percent=None, pullback_percent=None,
             impulse_percent=None, distance_to_peak_percent=None,
             sell_at_first_low=None, sell_at_second_low=None,
             selling_weakens=None, reclaim=False)
    pattern = False
    pivots = []
    if len(lows) >= 2:
        a, b = lows[-2:]
        low1, low2 = chart[a][3], chart[b][3]
        peak_index = max(range(a), key=lambda i: chart[i][2])
        peak = chart[peak_index][2]
        base = min((row[3] for row in chart[:peak_index]), default=peak)
        rebound = max(row[2] for row in chart[a+1:b])
        f.update(higher_low_percent=(low2/low1-1)*100,
                 pullback_percent=(low1/peak-1)*100,
                 impulse_percent=(peak/base-1)*100,
                 distance_to_peak_percent=(last[4]/peak-1)*100,
                 sell_at_first_low=chart[a][6], sell_at_second_low=chart[b][6],
                 selling_weakens=chart[b][6] < chart[a][6],
                 reclaim=last[4] > rebound)
        # The first low must follow a rise, not simply be the start of a fall.
        pattern = (f['impulse_percent'] > 0 and f['pullback_percent'] < 0
                   and low2 > low1 and f['reclaim'] and f['price_5s'] > 0
                   and all(row[3] >= low2 for row in chart[b+1:]))
        pivots = [chart[a][0], chart[b][0]]
    return dict(version=VERSION, at=at, state='KNOWN', features=f,
                pattern=pattern,
                supported=bool(pattern and f['selling_weakens'] and f['two_buy_windows']),
                lows_at=pivots, chart_columns=['at','open','high','low','close','buy','sell'],
                chart=chart)


class StructureStream(RocketQuoteStream):
    """Reuse daily quote socket; add public trades and a bounded 10-minute buffer.

    The trading thread never reads this buffer. Capture happens in DailyWorker.
    Whole current seconds are excluded so later arrivals cannot leak into a
    queued decision's snapshot. Restarts/gaps require a fresh five-minute window.
    """
    def __init__(self):
        super().__init__()
        self.bars = {}
        self.first_trade = {}
        self.trade_ids = {}

    @staticmethod
    def streams(symbols):
        return RocketQuoteStream.streams(symbols) + [s.lower()+'@aggTrade' for s in sorted(symbols)]

    def set_symbols(self, symbols):
        super().set_symbols(symbols)
        with self._lock:
            for cache in (self.bars, self.first_trade, self.trade_ids):
                for symbol in list(cache):
                    if symbol not in self._symbols:
                        del cache[symbol]

    def _bar(self, symbol, at):
        rows = self.bars.setdefault(symbol, {})
        sec = int(at)
        if sec not in rows:
            rows[sec] = [sec, None, None, None, None, 0., 0., None, None]
            while len(rows) > 610:
                del rows[next(iter(rows))]
        return rows[sec]

    def on_quote(self, at, symbol, bid, ask, update):
        with self._lock:
            if symbol not in self._symbols:
                return
            row = self._bar(symbol, at)
            if row[1] is None:
                row[1:5] = [bid]*4
                row[7] = at
            row[2], row[3], row[4], row[8] = max(row[2], bid), min(row[3], bid), bid, at

    def ingest(self, payload, received_at=None):
        at = time.time() if received_at is None else received_at
        item = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        data = item.get('data', item)
        if data.get('e') != 'aggTrade':
            return super().ingest(item, at)
        symbol = str(data.get('s', '')).upper()
        try:
            price, quantity, ident = float(data['p']), float(data['q']), int(data['a'])
            if not all(math.isfinite(v) and v > 0 for v in (price, quantity, price*quantity)) or not isinstance(data['m'], bool):
                raise ValueError('invalid trade')
        except (KeyError, TypeError, ValueError, OverflowError):
            self.interrupted([symbol], at)
            return
        with self._lock:
            if symbol not in self._symbols or ident <= self.trade_ids.get(symbol, -1):
                return
            previous = self.trade_ids.get(symbol)
            if previous is not None and ident != previous+1:
                self.bars.pop(symbol, None)
                self.first_trade.pop(symbol, None)
            self.trade_ids[symbol] = ident
            self.first_trade.setdefault(symbol, at)
            self._bar(symbol, at)[6 if data['m'] else 5] += price*quantity

    def interrupted(self, symbols, at=None):
        symbols = tuple(symbols)
        super().interrupted(symbols, at)
        with self._lock:
            for symbol in symbols:
                for cache in (self.bars, self.first_trade, self.trade_ids):
                    cache.pop(symbol, None)

    def snapshot(self, symbol, at):
        end, start = int(at), int(at)-LOOKBACK
        with self._lock:
            rows = [list(r) for sec,r in self.bars.get(symbol, {}).items() if start <= sec < end]
            first_trade = self.first_trade.get(symbol)
        rows.sort(key=lambda r:r[0])
        quotes = [r for r in rows if r[1] is not None]
        if (first_trade is None or first_trade > start or not quotes
                or quotes[0][7] > start+1 or at-quotes[-1][8] > 2
                or any(b[7]-a[8] > 2 for a,b in zip(quotes, quotes[1:]))):
            return unknown(at, 'нет полного свежего окна 5 минут')
        chart = []
        for low in range(start, end, 5):
            part = [r for r in rows if low <= r[0] < low+5]
            prices = [r for r in part if r[1] is not None]
            if not prices:
                return unknown(at, 'разрыв цены')
            chart.append([low, prices[0][1], max(r[2] for r in prices),
                          min(r[3] for r in prices), prices[-1][4],
                          sum(r[5] for r in part), sum(r[6] for r in part)])
        return describe(chart, at)


def report_text(episodes):
    rows = [s for s in episodes if s.get('structure', {}).get('version') == VERSION]
    rejected = [s for s in rows if s.get('classification') == 'REJECTED']
    known = [s for s in rejected if s['structure']['state'] == 'KNOWN']
    closed = [s for s in known if s['leg']['status'] == 'CLOSED']
    lines = ['🔎 Структура до входа — тень v1, те же 24ч',
             f'Новых снимков {len(rows)}; отказов {len(rejected)}; полных до решения {len(known)}; без истории {len(rejected)-len(known)}.',
             f'Из полных: исходов {len(closed)}; неполных путей {sum(s["leg"]["status"]=="INCOMPLETE" for s in known)}; прочие ещё не закрыты.']
    groups = [('Откат → выше минимум → пробой', lambda s:s['structure']['pattern']),
              ('Из них ослабли продажи + покупки 2×5с', lambda s:s['structure']['supported']),
              ('Остальные', lambda s:not s['structure']['pattern'])]
    for label, test in groups:
        selected = [s for s in closed if test(s)]
        net = [s['leg']['net']*.5 for s in selected]
        lines.append(f'• {label}: {len(net)}; плюс/минус {sum(v>0 for v in net)}/{sum(v<0 for v in net)}; {sum(net):+.3f} USDT.')
    for key,label in [('higher_low_percent','Подъём второго минимума'), ('pullback_percent','Откат от вершины')]:
        values = [[s['structure']['features'][key] for s in closed
                   if s['leg']['net']*sign>0 and s['structure']['features'].get(key) is not None]
                  for sign in (1,-1)]
        display = [f'{median(v):+.2f}% (n={len(v)})' if v else 'нет данных' for v in values]
        lines.append(f'{label}, медиана плюс/минус: {display[0]} / {display[1]}.')
    lines.append('5 мин до решения; закрытые 5с-свечи bid + исполненные покупки/продажи. Без будущих свечей. Второй ряд — часть первого, не складывать. По 50 USDT с издержками, без глубины/лимита банка. Правила торговли не меняются.')
    return '\n'.join(lines)
