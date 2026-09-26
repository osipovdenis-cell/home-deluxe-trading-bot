"""Same-load direct/default/wrapper public transport comparison."""
import asyncio
import json
import os
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from websockets.asyncio.client import connect
from bot.async_market_socket import AsyncMarketSocket

STREAMS='/'.join(s+'@'+c for s in ['btcusdt','ethusdt','bnbusdt'] for c in ['depth5@100ms','kline_1s'])

def summarize(values):
    values.sort()
    return dict(n=len(values),median=values[len(values)//2],p95=values[int(len(values)*.95)],maximum=values[-1]) if values else {}

def record(raw, lag, counts):
    data=json.loads(raw)['data']
    counts[0]+=1
    if 'E' in data:
        lag.append(time.time()-data['E']/1000)

async def native(label,endpoint,options):
    lag=[]; counts=[0]
    try:
        async with connect(endpoint+'/stream?streams='+STREAMS,compression=None,max_queue=256,ping_interval=None,open_timeout=10,close_timeout=2,**options) as ws:
            deadline=time.monotonic()+65
            while time.monotonic()<deadline:
                try: record(await asyncio.wait_for(ws.recv(),1),lag,counts)
                except TimeoutError: pass
        print(json.dumps(dict(label=label,messages=counts[0],lag=summarize(lag))),flush=True)
    except Exception as exc:
        print(json.dumps(dict(label=label,error=type(exc).__name__)),flush=True)

def wrapper():
    lag=[]; counts=[0]
    try:
        with AsyncMarketSocket('wss://stream.binance.com:443/stream?streams='+STREAMS,compression=None,max_queue=256,ping_interval=None,open_timeout=10,close_timeout=2) as ws:
            deadline=time.monotonic()+65
            while time.monotonic()<deadline:
                try: record(ws.recv(timeout=.2),lag,counts)
                except TimeoutError: pass
        print(json.dumps(dict(label='wrapper-default',messages=counts[0],lag=summarize(lag))),flush=True)
    except Exception as exc:
        print(json.dumps(dict(label='wrapper-default',error=type(exc).__name__)),flush=True)

async def main():
    print(json.dumps(dict(proxy_env_present={k:bool(os.environ.get(k)) for k in ['HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','https_proxy','http_proxy','all_proxy']})),flush=True)
    await asyncio.gather(native('native-direct','wss://stream.binance.com:443',dict(proxy=None)),native('native-default','wss://stream.binance.com:443',{}),native('vision-direct','wss://data-stream.binance.vision:443',dict(proxy=None)),asyncio.to_thread(wrapper))

asyncio.run(main())
