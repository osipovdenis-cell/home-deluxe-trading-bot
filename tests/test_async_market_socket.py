import asyncio
import unittest
import threading
from contextlib import ExitStack
from websockets.sync.server import serve
from unittest.mock import patch
from bot.async_market_socket import AsyncMarketSocket


class FakeSocket:
    def __init__(self):
        self.messages = asyncio.Queue()
        self.closed = False

    async def send(self, payload):
        await self.messages.put(payload)

    async def recv(self):
        return await self.messages.get()

    async def close(self):
        self.closed = True


class AsyncSocketTests(unittest.TestCase):
    def test_many_sockets_share_io_and_closing_one_keeps_peers_alive(self):
        async def opened(*args, **kwargs):
            return FakeSocket()
        with patch('bot.async_market_socket.async_connect', opened), ExitStack() as stack:
            sockets = [stack.enter_context(AsyncMarketSocket('wss://example.invalid')) for _ in range(64)]
            self.assertEqual(len({s.thread for s in sockets}), 1)
            self.assertEqual(len({s.loop for s in sockets}), 1)
            with self.assertRaises(TimeoutError):
                sockets[0].recv(timeout=.001)
            sockets[0].__exit__(None, None, None)
            # Release is idempotent when ExitStack later closes this socket again.
            self.assertTrue(sockets[-1].thread.is_alive())
            for i, socket in enumerate(sockets[1:]):
                socket.send(str(i))
                self.assertEqual(socket.recv(timeout=1), str(i))
        self.assertFalse(sockets[-1].thread.is_alive())

    def test_failed_open_does_not_stop_existing_peer(self):
        async def opened(url, **kwargs):
            if url.endswith('bad'):
                raise OSError('failure')
            return FakeSocket()
        with patch('bot.async_market_socket.async_connect', opened):
            with AsyncMarketSocket('wss://ok') as socket:
                with self.assertRaises(OSError):
                    with AsyncMarketSocket('wss://bad'):
                        pass
                socket.send('alive')
                self.assertEqual(socket.recv(timeout=1), 'alive')

    def test_timeout_preserves_pending_receive_and_message_order(self):
        ws = FakeSocket()
        async def open_socket(*args, **kwargs):
            return ws
        with patch('bot.async_market_socket.async_connect', open_socket):
            with AsyncMarketSocket('wss://example.invalid') as socket:
                with self.assertRaises(TimeoutError):
                    socket.recv(timeout=.01)
                pending = socket.receive
                with self.assertRaises(TimeoutError):
                    socket.recv(timeout=.01)
                self.assertIs(socket.receive, pending)
                socket.send('first')
                socket.send('second')
                self.assertEqual(socket.recv(timeout=1), 'first')
                self.assertEqual(socket.recv(timeout=1), 'second')
                with self.assertRaises(TimeoutError):
                    socket.recv(timeout=.01)
            self.assertTrue(ws.closed)
            self.assertFalse(socket.thread.is_alive())
            self.assertTrue(socket.loop.is_closed())

    def test_open_failure_stops_io_thread(self):
        async def fail(*args, **kwargs):
            raise OSError('connection failed')
        socket = AsyncMarketSocket('wss://example.invalid')
        with patch('bot.async_market_socket.async_connect', fail):
            with self.assertRaises(OSError):
                with socket:
                    self.fail('unexpected connection')
        self.assertFalse(socket.thread.is_alive())

    def test_real_socket_echo_and_server_ping_survive_receive_timeout(self):
        ping_answered = threading.Event()
        def handler(ws):
            if ws.ping(b'continuity').wait(2):
                ping_answered.set()
            for message in ws:
                ws.send(message)
        with serve(handler, '127.0.0.1', 0) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.socket.getsockname()[1]
                with AsyncMarketSocket(f'ws://127.0.0.1:{port}', proxy=None,
                                       ping_interval=None, close_timeout=1) as socket:
                    self.assertTrue(ping_answered.wait(2))
                    with self.assertRaises(TimeoutError):
                        socket.recv(timeout=.01)
                    pending = socket.receive
                    socket.heartbeat(timeout=1)
                    self.assertIs(socket.receive,pending)
                    socket.send('subscribe')
                    self.assertEqual(socket.recv(timeout=2), 'subscribe')
            finally:
                server.shutdown()
                thread.join(timeout=2)

class HeartbeatTimeoutTests(unittest.TestCase):
    def test_missing_pong_times_out_without_consuming_pending_data(self):
        class Silent(FakeSocket):
            async def ping(self):
                return asyncio.get_running_loop().create_future()
        ws=Silent()
        async def opened(*args,**kwargs):return ws
        with patch('bot.async_market_socket.async_connect',opened):
            with AsyncMarketSocket('wss://example.invalid') as socket:
                with self.assertRaises(TimeoutError):socket.recv(timeout=.01)
                pending=socket.receive
                with self.assertRaises(TimeoutError):socket.heartbeat(timeout=.01)
                self.assertIs(socket.receive,pending)
                socket.send('still intact')
                self.assertEqual(socket.recv(timeout=1),'still intact')
