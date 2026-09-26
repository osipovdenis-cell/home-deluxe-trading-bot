"""Synchronous collector interface backed by a single asyncio socket owner.

Experimental transport: caller timeouts keep the pending receive alive, avoiding
message loss between a future completing and the synchronous timeout expiring.
"""
import asyncio
import threading
from concurrent.futures import TimeoutError as FutureTimeout

from websockets.asyncio.client import connect as async_connect


class AsyncMarketSocket:
    def __init__(self, url, **options):
        self.url, self.options = url, options
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, name='market-socket-io', daemon=True)
        self.ws = None
        self.receive = None

    def _run(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self.loop.close()

    async def _open(self):
        self.ws = await async_connect(self.url, **self.options)

    def __enter__(self):
        self.thread.start()
        try:
            asyncio.run_coroutine_threadsafe(self._open(), self.loop).result(
                timeout=self.options.get('open_timeout', 10)+2)
            return self
        except BaseException:
            self._stop()
            raise

    def send(self, payload):
        asyncio.run_coroutine_threadsafe(self.ws.send(payload), self.loop).result(timeout=10)

    def recv(self, timeout=None):
        if self.receive is None:
            self.receive = asyncio.run_coroutine_threadsafe(self.ws.recv(), self.loop)
        try:
            result = self.receive.result(timeout=timeout)
        except FutureTimeout:
            # Do not cancel: a frame may have arrived at the timeout boundary.
            raise TimeoutError('market receive deadline') from None
        except BaseException:
            self.receive = None
            raise
        self.receive = None
        return result

    def _stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=3)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.ws is not None:
                asyncio.run_coroutine_threadsafe(self.ws.close(), self.loop).result(
                    timeout=self.options.get('close_timeout', 2)+1)
        finally:
            self._stop()


connect = AsyncMarketSocket
