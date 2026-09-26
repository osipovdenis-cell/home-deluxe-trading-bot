"""Read-only coverage comparison; isolated public feeds, no production access."""
import json
import multiprocessing
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bot.rocket_structure import StructureStream
from bot.sharded_market_stream import ShardedMarketStream


def probe(group_size):
    ShardedMarketStream.symbols_per_group = group_size
    stream = StructureStream()
    symbols=['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','DOGEUSDT']
    stream.set_symbols(symbols)
    stream.start()
    began=time.time()
    last={}; pauses={s:[] for s in symbols}; counts={s:0 for s in symbols}
    interruptions=[]
    try:
        while time.time()-began<100:
            quotes,overflow,gaps=stream.drain_quotes()
            if overflow: raise RuntimeError('quote overflow')
            interruptions.extend(gaps)
            for at,s,bid,ask in quotes:
                if s in last and at-last[s]>2:
                    pauses[s].append([round(last[s]-began,2),round(at-began,2),round(at-last[s],3)])
                last[s]=max(last.get(s,at),at)
                counts[s]+=1
            time.sleep(.1)
        now=time.time()
        coverage={}
        with stream._lock:
            for s in symbols:
                rows=sorted(stream.bars.get(s,{}).values(),key=lambda r:r[0])
                coverage[s]=dict(first_trade_after_start=(stream.first_trade[s]-began if s in stream.first_trade else None),quote_seconds=len(rows),first_quote_after_start=(rows[0][7]-began if rows else None),last_quote_age=(now-rows[-1][8] if rows else None),pauses_over_2s=pauses[s],quotes=counts[s],trades=len(stream.trades.get(s,())))
        health=stream.health()
        for part,stats in zip(stream._shards.parts,health['partitions']):
            stats['symbols']=sorted(part._symbols)
            stats['family']=part._family
        print(json.dumps(dict(group_size=group_size,coverage=coverage,interruptions=interruptions,health=health)),flush=True)
    finally:
        stream.close()

if __name__=='__main__':
    children=[multiprocessing.Process(target=probe,args=(n,)) for n in (3,1)]
    for p in children:p.start()
    for p in children:p.join(125)
    for p in children:
        if p.is_alive():p.terminate();raise RuntimeError('probe timeout')
        if p.exitcode:raise RuntimeError('probe failed')
