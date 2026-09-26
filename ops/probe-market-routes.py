"""Compare official public WebSocket routes without credentials or saved state."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from websockets.sync.client import connect


def probe(base):
    result = dict(endpoint=base, messages=0, acks=0, max_event_lag=0.)
    started = time.monotonic()
    try:
        with connect(base+'/stream?streams=btcusdt@depth5/btcusdt@aggTrade',
                     open_timeout=10, close_timeout=2, ping_interval=None,
                     compression=None) as ws:
            sent = time.monotonic()
            ws.send(json.dumps(dict(method='SUBSCRIBE', params=['ethusdt@depth5'], id=1)))
            while time.monotonic()-started < 90:
                try:
                    message = json.loads(ws.recv(timeout=.5))
                except TimeoutError:
                    continue
                result['messages'] += 1
                if 'id' in message:
                    result['acks'] += 1
                    result['ack_seconds'] = round(time.monotonic()-sent, 3)
                data = message.get('data', message)
                if data.get('E'):
                    result['max_event_lag'] = max(result['max_event_lag'], round(time.time()-data['E']/1000, 3))
    except Exception as error:
        result['error_type'] = type(error).__name__
        result['received_close_code'] = getattr(getattr(error, 'rcvd', None), 'code', None)
        result['sent_close_code'] = getattr(getattr(error, 'sent', None), 'code', None)
    result['elapsed'] = round(time.monotonic()-started, 3)
    return result


if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=3) as pool:
        for result in pool.map(probe, ['wss://stream.binance.com:9443',
                                      'wss://stream.binance.com:443',
                                      'wss://data-stream.binance.vision:443']):
            print(json.dumps(result), flush=True)
