"""Synchronous collector interface backed by a single asyncio socket owner.

Experimental transport: caller timeouts keep the pending receive alive, avoiding
message loss between a future completing and the synchronous timeout expiring.
"""
import asyncio
import threading
from concurrent.futures import TimeoutError as FutureTimeout, ThreadPoolExecutor

from websockets.asyncio.client import connect as async_connect


class SharedSocketLoop:
    """One I/O thread and bounded DNS executor for all public market sockets."""
    lock = threading.Lock()
    current = None

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.loop.set_default_executor(ThreadPoolExecutor(max_workers=4, thread_name_prefix='market-dns'))
        self.thread = threading.Thread(target=self._run, name='market-socket-io', daemon=True)
        self.users = 0
        self.thread.start()

    @classmethod
    def acquire(cls):
        with cls.lock:
            if cls.current is None:
                cls.current = cls()
            cls.current.users += 1
            return cls.current

    def release(self):
        with self.lock:
            self.users -= 1
            if self.users:
                return
            type(self).current = None
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=3)

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


class AsyncMarketSocket:
    def __init__(self, url, **options):
        self.url, self.options = url, options
        self.ws = None
        self.receive = None
        self._owner = None
        self.loop = self.thread = None

    async def _open(self):
        self.ws = await async_connect(self.url, **self.options)

    def __enter__(self):
        self._owner = SharedSocketLoop.acquire()
        self.loop, self.thread = self._owner.loop, self._owner.thread
        opening = asyncio.run_coroutine_threadsafe(self._open(), self.loop)
        try:
            opening.result(timeout=self.options.get('open_timeout', 10)+2)
            return self
        except BaseException:
            opening.cancel()
            self._stop()
            raise

    def send(self, payload):
        asyncio.run_coroutine_threadsafe(self.ws.send(payload), self.loop).result(timeout=10)

    async def _heartbeat(self, timeout):
        pong = await self.ws.ping()
        await asyncio.wait_for(pong, timeout)

    def heartbeat(self, timeout=5):
        future = asyncio.run_coroutine_threadsafe(self._heartbeat(timeout), self.loop)
        try:
            future.result(timeout=timeout+1)
        except FutureTimeout:
            future.cancel()
            raise TimeoutError('market heartbeat deadline') from None

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
        if self.receive is not None:
            self.receive.cancel()
            self.receive = None
        if self._owner is not None:
            owner, self._owner = self._owner, None
            owner.release()

    def __exit__(self, exc_type, exc, tb):
        if self._owner is None:
            return
        try:
            if self.ws is not None:
                asyncio.run_coroutine_threadsafe(self.ws.close(), self.loop).result(
                    timeout=self.options.get('close_timeout', 2)+1)
        finally:
            self._stop()


connect = AsyncMarketSocket
