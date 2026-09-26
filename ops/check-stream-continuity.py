"""Six-minute public-data gate. No credentials, orders or production databases."""
import json
import os
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot.streams import LeaderOrderFlowStream
from bot.rocket_structure import StructureStream


def resources():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    data = dict(cpus=os.cpu_count(), load_average=os.getloadavg(),
                cpu_seconds=usage.ru_utime+usage.ru_stime, max_rss_kb=usage.ru_maxrss)
    for name in ('cpu.max', 'cpu.stat', 'cpu.pressure'):
        try:
            data[name] = Path('/sys/fs/cgroup', name).read_text().strip()
        except OSError:
            pass
    return data


def main():
    print(json.dumps(dict(resources_start=resources())), flush=True)
    base = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT']
    flow, structure = LeaderOrderFlowStream(), StructureStream()
    for stream in (flow, structure):
        stream.set_symbols(base)
        stream.start()
    start = time.monotonic()
    changes = [(20, base+['ARBUSDT']), (50, list(reversed(base))+['ARBUSDT']),
               (80, base+['NEARUSDT']), (110, base)]
    fresh = known = quotes = 0
    phase = 0
    next_log = 30
    try:
        while time.monotonic()-start < 370:
            elapsed = time.monotonic()-start
            if phase < len(changes) and elapsed >= changes[phase][0]:
                for stream in (flow, structure):
                    stream.set_symbols(changes[phase][1])
                phase += 1
            now = time.time()
            fresh += sum(flow.entry_probe(s, now)['fresh'] for s in base[:2])
            if elapsed >= 305:
                known += sum(structure.snapshot(s, now)['state']=='KNOWN' for s in base[:2])
            received, overflow, gaps = structure.drain_quotes()
            quotes += len(received)
            if overflow:
                raise RuntimeError('public stream queue overflow')
            if elapsed >= next_log:
                print(json.dumps(dict(elapsed=round(elapsed), fresh=fresh,
                                      known_structure=known, quotes=quotes)), flush=True)
                next_log += 30
            time.sleep(.5)
        result = dict(fresh=fresh, known_structure=known, quotes=quotes,
                      flow=flow.health(), structure=structure.health())
        result['resources_end'] = resources()
        print(json.dumps(result), flush=True)
        transports = [result['flow']['transport'], result['structure']]
        if fresh < 20 or known < 2 or quotes < 100:
            raise RuntimeError('insufficient continuous 60-second / 5-minute observations')
        for health in transports:
            if not health or not health['connected'] or health['ingest_errors'] or health['ingest_overflows']:
                raise RuntimeError('stream health gate failed')
            if health['subscription_replies'] < 4 or health['pending_subscription_requests']:
                raise RuntimeError('dynamic subscriptions not confirmed')
    finally:
        flow.close()
        structure.close()


if __name__ == '__main__':
    main()
