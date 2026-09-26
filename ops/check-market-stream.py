"""Bounded public-data probe: no account keys, orders, or production state."""
import json
import time

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


if __name__ == '__main__':
    main()
