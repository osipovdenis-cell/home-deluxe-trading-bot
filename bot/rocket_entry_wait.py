"""Short independent observation of already-approved paper rocket entries."""
from dataclasses import dataclass
from queue import SimpleQueue
import threading
import time
import httpx

from bot.execution import fresh_entry
from bot.rocket_cards import entry_probe
from bot.rocket_entry_guard import finite, fading_buy_guard


@dataclass(frozen=True)
class WaitingEntry:
    signal: object
    context: object
    dynamics: object
    score: int
    signal_at: float
    queued_at: float


class RocketEntryWaitWorker:
    def __init__(self, trader_factory, market, healthy, base_url, client_factory=None,
                 ttl=90, max_pending=20):
        self.trader_factory, self.market, self.healthy = trader_factory, market, healthy
        self.client_factory = client_factory or (lambda: httpx.Client(base_url=base_url, timeout=2))
        self.ttl, self.max_pending = ttl, max_pending
        self._pending, self._lock = {}, threading.Lock()
        self._last_block = {}
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = None
        self.messages, self.errors = SimpleQueue(), SimpleQueue()

    def symbols(self):
        with self._lock:
            return tuple(self._pending)

    def submit(self, signal, context, dynamics, score, signal_at):
        with self._lock:
            if signal.symbol in self._pending:
                return True
            if len(self._pending) >= self.max_pending:
                return False
            self._pending[signal.symbol] = WaitingEntry(signal, context, dynamics, score,
                                                        signal_at, time.time())
        return True

    @staticmethod
    def recovery_reason(probe):
        # Delayed entries must satisfy the same final veto as immediate entries.
        allowed, reason = fading_buy_guard(probe)
        if not allowed:
            return reason
        r5 = (probe.get('changes') or {}).get('5')
        flow = probe.get('after_flow') or {}
        buy, sell = flow.get('buy_5s_usdt'), flow.get('sell_5s_usdt')
        if not all(finite(v) for v in (r5, buy, sell)):
            return 'неполные данные цены/покупок за 5с'
        if r5 <= 0:
            return 'цена за 5с не растёт'
        if not buy > sell >= 0:
            return 'покупки за 5с не превышают продажи'
        return None

    @staticmethod
    def recovered(probe):
        return RocketEntryWaitWorker.recovery_reason(probe) is None

    def finish(self, trader, job, state, detail, now):
        blocked = self._last_block.get(job.signal.symbol)
        if state == 'EXPIRED' and blocked:
            detail += '; последняя причина: ' + blocked
        trader.connection.execute('''INSERT INTO rocket_entry_waits
            (symbol,signal_at,queued_at,finished_at,state,detail) VALUES(?,?,?,?,?,?)''',
            (job.signal.symbol,job.signal_at,job.queued_at,now,state,detail))
        trader.connection.commit()
        with self._lock:
            if self._pending.get(job.signal.symbol) is job:
                del self._pending[job.signal.symbol]
                self._last_block.pop(job.signal.symbol, None)

    @staticmethod
    def prepare(trader):
        trader.connection.execute('''CREATE TABLE IF NOT EXISTS rocket_entry_waits(
            id INTEGER PRIMARY KEY,symbol TEXT,signal_at REAL,queued_at REAL,
            finished_at REAL,state TEXT,detail TEXT)''')
        trader.connection.commit()

    def step(self, trader, client, now):
        with self._lock:
            jobs = tuple(self._pending.values())
        for job in jobs:
            symbol = job.signal.symbol
            if now-job.queued_at >= self.ttl:
                self.finish(trader,job,'EXPIRED','90 секунд без подходящего восстановления',now)
                continue
            if symbol in trader.open_symbols():
                self.finish(trader,job,'CANCELLED','позиция уже открыта',now)
                continue
            if not self.healthy():
                self._last_block[symbol] = "обработчик выходов недоступен"
                continue
            if self.market.change_12h_percent.get(symbol, 0) <= 0:
                self.finish(trader,job,'CANCELLED','рост за 12ч больше не подтверждён',now)
                continue
            probe = entry_probe(self.market,job.signal,job.context,job.dynamics,job.signal_at)
            if not self.recovered(probe):
                self._last_block[symbol] = self.recovery_reason(probe)
                continue
            try:
                at,bid,ask = fresh_entry(client,symbol,job.signal.price,trader.stop_loss_percent,.25)
            except ValueError:
                # Retain the existing execution boundary; do not chase a moved price.
                self.finish(trader,job,'CANCELLED','цена/спред вышли за границы свежего входа',time.time())
                continue
            except httpx.HTTPError:
                self._last_block[symbol] = "ошибка получения свежей цены"
                continue
            # The market may change during the quote request. Recheck without AI/REST.
            probe = entry_probe(self.market,job.signal,job.context,job.dynamics,job.signal_at)
            if not self.recovered(probe) or not self.healthy():
                self._last_block[symbol] = self.recovery_reason(probe) or "обработчик выходов недоступен"
                continue
            if self._stop.is_set():
                return
            if time.time()-at > 2:
                self._last_block[symbol] = "свежая цена устарела за время проверки"
                continue
            if time.time()-job.queued_at >= self.ttl:
                self.finish(trader,job,'EXPIRED','срок наблюдения истёк',time.time())
                continue
            kind=job.signal.kind + (' · повторный вход' if job.signal.is_leader_reentry else '')
            kind += ' · после короткого ожидания'
            notice = trader.open_on_signal(symbol,ask,kind,job.score,at,
                                           bypass_min_score=True,signal_timestamp=job.signal_at)
            if notice is None:
                self._last_block[symbol] = "нет доступного слота/баланса или ограничение исполнения"
                # No available slot/balance: keep observing until expiry.
                continue
            # Remove before diagnostic/notification work so failures cannot repeat a buy.
            self.finish(trader,job,'OPENED','восстановился рост; фильтр В и свежее рыночное качество подтверждены',at)
            row=trader.connection.execute('SELECT id FROM paper_positions WHERE symbol=? AND opened_at=? ORDER BY id DESC LIMIT 1',
                                         (symbol,at)).fetchone()
            sink=self.market.__dict__.get('rocket_shadow_sink')
            if sink and row:
                sink(row[0],{**probe,'entry_bid':bid,'entry_quote_at':at,
                             'wait_seconds':at-job.queued_at,'resumed_without_ai':True})
            self.messages.put(trader.notice_telegram_text(notice,{symbol:bid},at))

    def start(self):
        self._thread=threading.Thread(target=self._run,name='rocket-entry-wait',daemon=True)
        self._thread.start()
        if not self._ready.wait(5):
            self._stop.set()
            raise RuntimeError('Не запустился обработчик отложенных входов')

    def _run(self):
        trader=client=None
        try:
            trader=self.trader_factory()
            self.prepare(trader)
            client=self.client_factory()
            self._ready.set()
            while not self._stop.is_set():
                try:
                    self.step(trader,client,time.time())
                except Exception as error:
                    trader.connection.rollback()
                    self.errors.put('Короткое ожидание входа: '+type(error).__name__)
                self._stop.wait(.2)
        finally:
            if client: client.close()
            if trader: trader.close()

    def close(self):
        self._stop.set()
        if self._thread: self._thread.join(timeout=5)
