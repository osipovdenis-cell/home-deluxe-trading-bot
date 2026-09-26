"""Bounded public-only channel comparison; not a deployment acceptance gate."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from websockets.sync.client import connect

SYMBOLS = ['btcusdt','ethusdt','solusdt','bnbusdt','dogeusdt']
CASES = [('all-compressed',SYMBOLS,['bookTicker','depth5','aggTrade','depth@100ms'],'deflate'),
         ('one-symbol',['btcusdt'],['bookTicker','depth5','aggTrade','depth@100ms'],None),
         ('quotes-only',SYMBOLS,['bookTicker','depth5'],None),
         ('trades-only',SYMBOLS,['aggTrade'],None)]

def probe(case):
    name,symbols,channels,compression = case
    result = dict(case=name, messages=0, channels={}, acks=[], max_event_lag=0., last_event_lag=None)
    sent = None
    started = time.monotonic()
    try:
        streams = [s+'@'+c for s in symbols for c in channels]
        with connect('wss://stream.binance.com:443/stream?streams='+'/'.join(streams),
                     compression=compression, open_timeout=10, close_timeout=2,
                     ping_interval=None, max_queue=256) as ws:
            result['extensions'] = [type(e).__name__ for e in ws.protocol.extensions]
            while time.monotonic()-started < 50:
                if sent is None and time.monotonic()-started >= 10:
                    sent = time.monotonic()
                    ws.send(json.dumps(dict(method='LIST_SUBSCRIPTIONS',id=1)))
                try:
                    msg = json.loads(ws.recv(timeout=.2))
                except TimeoutError:
                    continue
                result['messages'] += 1
                if 'id' in msg:
                    result['acks'].append(round(time.monotonic()-sent,3))
                channel = msg.get('stream','control').split('@')[-1]
                result['channels'][channel] = result['channels'].get(channel,0)+1
                data = msg.get('data',msg)
                if data.get('E'):
                    lag = round(time.time()-data['E']/1000,3)
                    result['last_event_lag'] = lag
                    result['max_event_lag'] = max(result['max_event_lag'],lag)
    except Exception as e:
        result['error'] = type(e).__name__
    return result

if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(probe, CASES):
            print(json.dumps(result),flush=True)
