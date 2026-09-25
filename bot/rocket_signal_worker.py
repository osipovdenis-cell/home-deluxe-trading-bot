"""Ready rocket decisions, isolated from slow confirmation/report analytics."""
from copy import copy
from dataclasses import dataclass
from queue import Queue, SimpleQueue, Empty, Full
import threading


@dataclass(frozen=True)
class SignalJob:
    signal: object
    prices: dict
    at: float
    market: object
    confirmation: object = None


def freeze_market(market, symbol, at):
    """Capture decision-time dynamics on the scanner thread, before enqueueing.

    The worker must never iterate deques mutated by MarketMonitor.update().
    Live order-flow callbacks and the existing short-wait worker remain shared.
    """
    view = copy(market)
    dynamics = market.entry_dynamics(symbol, at)
    view.entry_dynamics = lambda requested_symbol, requested_at: dynamics
    view.tick_sizes = dict(market.tick_sizes)
    view.change_12h_percent = dict(market.change_12h_percent)
    return view


class QueuedTelegram:
    def __init__(self, messages):
        self.messages = messages

    def send(self, chat_id, text):
        self.messages.put((chat_id, text))


class RocketSignalWorker:
    """One FIFO consumer; own DB and HTTP handles, no concurrent AI burst.

    Admission still runs through PaperTrader's shared entry lock and the same
    process_signal checks. Only the place where that work runs changes.
    """
    def __init__(self, resources, handle, capacity=32):
        self.resources, self.handle = resources, handle
        self.queue = Queue(maxsize=capacity)
        self.messages, self.errors = SimpleQueue(), SimpleQueue()
        self._pending = set()
        self._lock = threading.Lock()
        self._stop, self._ready = threading.Event(), threading.Event()
        self._thread = None
        self._startup_error = None

    def submit(self, signal, prices, at, market, confirmation=None):
        with self._lock:
            if self._stop.is_set():
                return False
            if signal.symbol in self._pending:
                return False  # Caller records an explicit duplicate/queue rejection.
            view = freeze_market(market, signal.symbol, at)
            view._entry_cancelled = self._stop.is_set
            try:
                self.queue.put_nowait(SignalJob(signal, dict(prices), at, view, confirmation))
            except Full:
                return False
            self._pending.add(signal.symbol)
        return True

    def start(self):
        self._thread = threading.Thread(target=self._run, name='rocket-signal-decisions', daemon=True)
        self._thread.start()
        if not self._ready.wait(10) or self._startup_error:
            self._stop.set()
            raise RuntimeError('Не запустился обработчик сигналов ракет')

    def _run(self):
        owned = []
        try:
            audit, trader, client = self.resources()
            owned = [audit, trader, client]
            self._ready.set()
            while not self._stop.is_set():
                try:
                    job = self.queue.get(timeout=.2)
                except Empty:
                    continue
                try:
                    job.market.client = client
                    self.handle(job, audit, trader, QueuedTelegram(self.messages))
                except Exception as error:
                    # Never replay a possibly committed purchase on failure.
                    for resource in (audit, trader):
                        if resource is not None:
                            resource.connection.rollback()
                    self.errors.put('Сигнал ракеты: ' + type(error).__name__)
                finally:
                    with self._lock:
                        self._pending.discard(job.signal.symbol)
                    self.queue.task_done()
        except Exception as error:
            self._startup_error = type(error).__name__
            self.errors.put('Обработчик сигналов ракет: ' + type(error).__name__)
            self._ready.set()
        finally:
            for resource in reversed(owned):
                if resource is not None:
                    resource.close()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
