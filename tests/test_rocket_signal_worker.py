import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.audit import AuditLog
from bot.main import handle_ready_rocket
from bot.market import MarketMonitor, PumpSignal, SignalMarketContext
from bot.rocket_signal_worker import RocketSignalWorker, SignalJob, freeze_market
from bot.trading import PaperTrader


def trader(path):
    return PaperTrader(path,200,50,1,70,1,.7,1,1.5,1,0,.2)


class SignalWorkerTests(unittest.TestCase):
    def setUp(self):
        self.market=MarketMonitor('https://example.invalid', ['X','Y'],300,3,60)
        self.addCleanup(self.market.close)
        self.market.history['X'].extend([(0,100),(100,101)])
        self.market.tick_sizes={'X':.01}
        self.market.change_12h_percent={'X':2}
        self.signal=PumpSignal('X',101,3,300,'лидер')

    def test_snapshot_dynamics_do_not_change_while_scanner_advances(self):
        view=freeze_market(self.market,'X',100)
        before=view.entry_dynamics('X',100)
        self.market.history['X'].clear()
        self.market.tick_sizes['X']=5
        self.market.change_12h_percent={'X':-1}
        self.assertEqual(view.entry_dynamics('X',100),before)
        self.assertEqual(view.tick_sizes['X'],.01)
        self.assertEqual(view.change_12h_percent['X'],2)

    def test_slow_decision_does_not_block_submit_and_duplicates_are_bounded(self):
        begun,release,finished=threading.Event(),threading.Event(),threading.Event()
        seen=[];owner=[]
        def resources():
            owner.append(threading.get_ident())
            # check_same_thread stays enabled: creation/use/close must share owner.
            return SimpleNamespace(connection=sqlite3.connect(':memory:'),close=lambda:None),None,Mock()
        def handle(job,audit,trader,notices):
            self.assertEqual(threading.get_ident(),owner[0])
            audit.connection.execute('SELECT 1')
            seen.append(job.signal.symbol)
            begun.set()
            release.wait(2)
            notices.send('owner','entry notification')
            if len(seen)==2:
                audit.connection.close();finished.set()
        worker=RocketSignalWorker(resources,handle,capacity=1)
        self.addCleanup(worker.close);self.addCleanup(release.set)
        worker.start()
        self.assertTrue(worker.submit(self.signal,{'X':101},100,self.market))
        self.assertTrue(begun.wait(1))
        self.assertFalse(worker.submit(self.signal,{},100,self.market))
        self.assertTrue(worker.submit(PumpSignal('Y',1,3,300,'лидер'),{},100,self.market))
        self.assertFalse(worker.submit(PumpSignal('Z',1,3,300,'лидер'),{},100,self.market))
        self.assertEqual(seen,['X'])
        release.set();self.assertTrue(finished.wait(2))
        self.assertEqual(seen,['X','Y'])
        self.assertNotEqual(owner[0],threading.get_ident())
        self.assertEqual(worker.messages.get_nowait(),('owner','entry notification'))
        self.assertTrue(worker.errors.empty())

    def test_failed_decision_not_replayed_and_next_signal_can_run(self):
        complete=threading.Event();calls=[];audit=Mock()
        def handle(job,*args):
            calls.append(job.signal.symbol)
            if job.signal.symbol=='X':raise ValueError('private payload')
            complete.set()
        worker=RocketSignalWorker(lambda:(audit,None,Mock()),handle)
        self.addCleanup(worker.close)
        worker.submit(self.signal,{},100,self.market)
        worker.submit(PumpSignal('Y',1,3,300,'лидер'),{},100,self.market)
        worker.start();self.assertTrue(complete.wait(1))
        self.assertEqual(calls,['X','Y'])
        audit.connection.rollback.assert_called_once()
        self.assertEqual(worker.errors.get_nowait(),'Сигнал ракеты: ValueError')

    def test_closed_worker_refuses_new_jobs(self):
        worker=RocketSignalWorker(Mock(),Mock());worker.close()
        self.assertFalse(worker.submit(self.signal,{},100,self.market))

    def test_ready_handler_preserves_confirmation_context_and_timestamps(self):
        view=freeze_market(self.market,'X',100)
        view._entry_cancelled=lambda:False
        context=SignalMarketContext(1000,2,100,60)
        view.fetch_signal_context=Mock(return_value=context)
        event=Mock();audit=Mock();flow=Mock()
        flow.snapshot.return_value=None
        job=SignalJob(self.signal,{'X':101},100,view,event)
        with patch('bot.main.process_signal',return_value=False) as process:
            result=handle_ready_rocket(job,audit,Mock(),Mock(),Mock(),'owner',
                                      SimpleNamespace(estimated_round_trip_cost_percent=.2),flow)
        self.assertFalse(result)
        self.assertIs(audit.record_confirmation_event.call_args.args[0],event)
        self.assertEqual(process.call_args.args[2],100)
        self.assertEqual(process.call_args.args[-1],context)
        self.assertGreaterEqual(process.call_args.kwargs['processing_started'],100)
        self.assertIn('контекст и запись подтверждения',process.call_args.kwargs['initial_stages'])

    def test_two_admission_connections_cannot_overbook_or_duplicate_position(self):
        with tempfile.TemporaryDirectory() as folder:
            path=str(Path(folder)/'bot.db')
            base=trader(path);base.close()
            ready=threading.Barrier(2);results=[];errors=[]
            def enter():
                t=None
                try:
                    t=trader(path);ready.wait(timeout=3)
                    results.append(t.open_on_signal('X',100,'лидер',70,time.time(),bypass_min_score=True))
                except Exception as error:errors.append(error)
                finally:
                    if t:t.close()
            workers=[threading.Thread(target=enter) for _ in range(2)]
            for w in workers:w.start()
            for w in workers:w.join(timeout=5)
            self.assertFalse(errors)
            self.assertEqual(sum(r is not None for r in results),1)
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM paper_positions WHERE status='OPEN'").fetchone()[0],1)
                self.assertAlmostEqual(db.execute('SELECT cash_balance_usdt FROM paper_account').fetchone()[0],0)

    def test_queued_decision_retains_final_quote_and_fading_buy_veto(self):
        from bot.market import EntryDynamics
        from bot.main import process_signal
        settings=SimpleNamespace(early_threshold_percent=.5,
            paper_take_profit_1_percent=.7,paper_take_profit_2_percent=1,
            paper_stop_loss_percent=1,estimated_round_trip_cost_percent=.2,
            paper_min_ai_score=70,telegram_signal_alerts_enabled=False)
        context=SignalMarketContext(1000,2,100,60,spread_bps=1)
        dynamics=EntryDynamics(.1,.2,.3,.4,.5,-.1,0,0,50)
        for allow,quote_at,expected in [(True,101,True),(False,101,False),(True,98,False)]:
            with self.subTest(allow=allow,quote_at=quote_at),tempfile.TemporaryDirectory() as folder:
                path=str(Path(folder)/'bot.db');audit=AuditLog(path);t=trader(path)
                market=Mock()
                market.entry_dynamics.return_value=dynamics
                market.execution_safety.return_value=(True,None,.01)
                market.leader_entry_quality.return_value=(True,None)
                market.change_12h_percent={'X':2}
                t.exit_monitor_healthy=lambda:True
                try:
                    with patch('bot.main.time.time',return_value=101), \
                         patch('bot.main.fresh_entry',return_value=(quote_at,101,101.01)) as fresh, \
                         patch('bot.main.entry_probe',return_value={'allowed':allow}), \
                         patch('bot.main.fading_buy_guard',return_value=(allow,'test veto')):
                        result=process_signal(self.signal,{'X':101},100,market,audit,t,None,
                                              Mock(),'owner',settings,context)
                    self.assertEqual(result,expected)
                    self.assertEqual(bool(t.open_symbols()),expected)
                    fresh.assert_called_once()
                finally:t.close();audit.close()
