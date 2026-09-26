"""Read-only delivery/host-clock probe, without bot collectors or credentials."""
import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path
import httpx
from websockets.asyncio.client import connect


def summary(values):
    values = sorted(values)
    return dict(n=len(values), median=values[len(values)//2], p95=values[min(len(values)-1,int(len(values)*.95))], maximum=values[-1]) if values else {}


def cpu():
    return [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]


async def main():
    deadline = time.monotonic()+95
    initial_cpu = cpu()
    drift = []
    async def ticker():
        while time.monotonic()<deadline:
            start=time.monotonic()
            await asyncio.sleep(.1)
            drift.append(time.monotonic()-start-.1)
    async def clock():
        async with httpx.AsyncClient(timeout=5) as client:
            while time.monotonic()<deadline:
                before=time.time()
                try:
                    response=await client.get('https://api.binance.com/api/v3/time')
                    after=time.time()
                    server=response.json()['serverTime']/1000
                    print(json.dumps(dict(clock_rtt=after-before, local_minus_binance_midpoint=(before+after)/2-server, offset_bounds=[before-server,after-server])),flush=True)
                except Exception as exc:
                    print(json.dumps(dict(clock_error=type(exc).__name__)),flush=True)
                await asyncio.sleep(10)
    async def receiver(endpoint):
        lag=defaultdict(list)
        intervals=defaultdict(list)
        last={}
        counts=defaultdict(int)
        error=None
        try:
            async with connect(endpoint+'/stream?streams=btcusdt@depth5@100ms/btcusdt@aggTrade/btcusdt@kline_1s', compression=None, max_queue=4096, proxy=None, open_timeout=10, close_timeout=2) as ws:
                while time.monotonic()<deadline:
                    try:
                        payload=await asyncio.wait_for(ws.recv(),1)
                    except TimeoutError:
                        continue
                    now=time.time()
                    data=json.loads(payload)
                    channel=data['stream'].split('@',1)[1]
                    row=data['data']
                    counts[channel]+=1
                    if 'E' in row:
                        lag[channel].append(now-row['E']/1000)
                    if channel in last:
                        intervals[channel].append(now-last[channel])
                    last[channel]=now
        except Exception as exc:
            error=type(exc).__name__
        print(json.dumps(dict(endpoint=endpoint, counts=counts, event_lag={k:summary(v) for k,v in lag.items()}, receive_intervals={k:summary(v) for k,v in intervals.items()}, error=error)),flush=True)
    await asyncio.gather(ticker(),clock(),*(receiver(e) for e in ['wss://stream.binance.com:443','wss://stream.binance.com:9443','wss://data-stream.binance.vision:443']))
    delta=[b-a for a,b in zip(initial_cpu,cpu())]
    print(json.dumps(dict(event_loop_drift=summary(drift),cpu_jiffy_delta=delta,steal_fraction=delta[7]/sum(delta[:8]) if sum(delta[:8]) else None)),flush=True)

asyncio.run(main())
