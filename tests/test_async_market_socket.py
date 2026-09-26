import asyncio
import unittest
import threading
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
                    socket.send('subscribe')
                    self.assertEqual(socket.recv(timeout=2), 'subscribe')
            finally:
                server.shutdown()
                thread.join(timeout=2)
