"""Compare delayed control messages under load; public market data only."""
import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from websockets.sync.client import connect
from websockets.asyncio.client import connect as async_connect

BASE = 'wss://stream.binance.com:443/stream?streams='
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'dogeusdt']

def channels(kind):
    return [s+'@'+c for s in SYMBOLS for c in
            (['depth5'] if kind == 'light' else ['bookTicker', 'depth5', 'aggTrade', 'depth@100ms'])]

def record(r, msg, sent):
    r['messages'] += 1
    if 'id' in msg:
        r['acks'].append([msg['id'], round(time.monotonic()-sent.get(msg['id'], time.monotonic()), 3)])
    d = msg.get('data', msg)
    if 'E' in d:
        r['max_event_lag'] = max(r['max_event_lag'], round(time.time()-d['E']/1000, 3))

def request(ident):
    return json.dumps(dict(method='SUBSCRIBE' if ident == 1 else 'LIST_SUBSCRIPTIONS',
                           **({'params':['arbusdt@depth5']} if ident == 1 else {}), id=ident))

def sync_probe(kind):
    r = dict(client='sync', kind=kind, messages=0, acks=[], max_event_lag=0.)
    started = time.monotonic()
    sent = {}
    try:
        with connect(BASE+'/'.join(channels(kind)), open_timeout=10, close_timeout=2,
                     ping_interval=None, compression=None, max_queue=256) as ws:
            while time.monotonic()-started < 130:
                elapsed = time.monotonic()-started
                for ident, at in [(1,20),(2,60)]:
                    if elapsed >= at and ident not in sent:
                        sent[ident] = time.monotonic()
                        ws.send(request(ident))
                try:
                    record(r, json.loads(ws.recv(timeout=.2)), sent)
                except TimeoutError:
                    pass
    except Exception as e:
        r['error'] = type(e).__name__
        r['cause'] = type(e.__cause__).__name__
    r['elapsed'] = round(time.monotonic()-started, 2)
    return r

async def async_probe():
    r = dict(client='async', kind='heavy', messages=0, acks=[], max_event_lag=0.)
    started = time.monotonic()
    sent = {}
    try:
        async with async_connect(BASE+'/'.join(channels('heavy')), open_timeout=10, close_timeout=2,
                                 ping_interval=None, compression=None, max_queue=256) as ws:
            while time.monotonic()-started < 130:
                elapsed = time.monotonic()-started
                for ident, at in [(1,20),(2,60)]:
                    if elapsed >= at and ident not in sent:
                        sent[ident] = time.monotonic()
                        await ws.send(request(ident))
                try:
                    record(r, json.loads(await asyncio.wait_for(ws.recv(), .2)), sent)
                except TimeoutError:
                    pass
    except Exception as e:
        r['error'] = type(e).__name__
        r['cause'] = type(e.__cause__).__name__
    r['elapsed'] = round(time.monotonic()-started, 2)
    return r

if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(sync_probe, k) for k in ['light','heavy']]
        futures.append(pool.submit(lambda: asyncio.run(async_probe())))
        for f in futures:
            print(json.dumps(f.result()), flush=True)
