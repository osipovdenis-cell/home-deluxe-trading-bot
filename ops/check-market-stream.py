"""Bounded public-data probe: no account keys, orders, or production state."""
import json
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot.rocket_structure import StructureStream

from websockets.sync.client import connect


def main():
    counts = {'messages': 0, 'controls': 0, 'wrapped_controls': 0}
    start = time.monotonic()
    try:
        with connect('wss://stream.binance.com:9443/stream?streams=btcusdt@depth5',
                     open_timeout=10, close_timeout=2, ping_interval=None,
                     compression=None) as ws:
            ws.send(json.dumps({'method': 'SUBSCRIBE', 'params': ['ethusdt@depth5'], 'id': 1}))
            listed = False
            while time.monotonic()-start < 24:
                if not listed and time.monotonic()-start >= 8:
                    ws.send(json.dumps({'method': 'LIST_SUBSCRIPTIONS', 'id': 2}))
                    listed = True
                try:
                    message = json.loads(ws.recv(timeout=.5))
                except TimeoutError:
                    continue
                counts['messages'] += 1
                inner = message.get('data', {})
                control = message if 'id' in message or 'result' in message or 'code' in message else inner
                if isinstance(control, dict) and any(k in control for k in ('id', 'result', 'code')):
                    counts['controls'] += 1
                    counts['wrapped_controls'] += int(control is inner)
                    print(json.dumps({'control': control, 'envelope_keys': sorted(message)}, ensure_ascii=False), flush=True)
            print(json.dumps(counts), flush=True)
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__, **counts}), flush=True)

    class ObservedStream(StructureStream):
        def sync_subscriptions(self, ws, subscribed, request_id, pending):
            before = set(pending)
            result = super().sync_subscriptions(ws, subscribed, request_id, pending)
            for ident in set(pending)-before:
                print(json.dumps({'sent_id': ident, 'method': pending[ident]['method']}), flush=True)
            return result

        def subscription_reply(self, message, subscribed, pending):
            print(json.dumps({'reply_id': message.get('id'), 'reply_keys': sorted(message)}), flush=True)
            return super().subscription_reply(message, subscribed, pending)

    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT',
               'LINKUSDT', 'AVAXUSDT', 'DOGEUSDT', 'LTCUSDT', 'UNIUSDT', 'ATOMUSDT',
               'FILUSDT', 'NEARUSDT', 'SAGAUSDT', 'LSKUSDT', 'QIUSDT', 'MUBARAKUSDT']
    stream = ObservedStream()
    stream.set_symbols(symbols)
    stream.start()
    start = time.monotonic()
    phase = 0
    try:
        while time.monotonic()-start < 85:
            elapsed = time.monotonic()-start
            if elapsed > 10 and phase == 0:
                stream.set_symbols(symbols[1:]+['ARBUSDT'])
                phase = 1
            if elapsed > 35 and phase == 1:
                stream.set_symbols(symbols[2:]+['ARBUSDT', 'ONDOUSDT'])
                phase = 2
            stream.drain_quotes()
            time.sleep(.2)
        print(json.dumps({'application_stream': stream.health()}), flush=True)
    finally:
        stream.close()


if __name__ == '__main__':
    main()
