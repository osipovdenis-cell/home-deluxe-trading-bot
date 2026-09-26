"""Public-data integration: warm minute history before any trade candidate exists."""
import json
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bot.market import MarketMonitor
from bot.streams import LeaderOrderFlowStream

m=MarketMonitor('https://api.binance.com',(),300,3,1800)
s=LeaderOrderFlowStream(max_symbols=20)
try:
    symbols=['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','DOGEUSDT']
    # Ranking fixture only: quotes/trades below come directly from public Binance.
    m.market_stats={symbol:(1e9,10-i) for i,symbol in enumerate(symbols)}
    m.change_12h_percent={symbol:1 for symbol in symbols}
    selected=m.order_flow_symbols()
    assert set(selected)==set(symbols) and not m.leaders and not m.pending_candidates
    s.set_symbols(selected);s.start()
    began=time.monotonic();fresh=0
    while time.monotonic()-began<100:
        now=time.time()
        fresh+=sum(s.entry_probe(symbol,now)['fresh'] for symbol in symbols[:2])
        time.sleep(.5)
    health=s.health();transport=health['transport']
    print(json.dumps(dict(prewarmed=list(selected),candidates=len(m.pending_candidates),fresh_samples=fresh,health=health)),flush=True)
    assert fresh>=10, 'minute history did not warm before a candidate'
    assert transport['connected'] and not transport['ingest_errors'] and not transport['ingest_overflows']
finally:
    s.close();m.client.close()

# An unsubscribed public connection deliberately has no market events.
from bot.async_market_socket import AsyncMarketSocket
with AsyncMarketSocket('wss://stream.binance.com:443/ws',open_timeout=10,close_timeout=2,ping_interval=None) as quiet:
    try:
        quiet.recv(timeout=.2)
    except TimeoutError:
        pass
    pending=quiet.receive
    quiet.heartbeat(timeout=5)
    assert quiet.receive is pending, 'heartbeat consumed market receive'
    print(json.dumps(dict(quiet_public_socket_pong=True)),flush=True)
