import json
import sqlite3
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from bot.audit import AuditLog
from bot.main import timed_entry_call, process_signal
from bot.market import PumpSignal, SignalMarketContext


class EntryLatencyTests(unittest.TestCase):
    def setUp(self):
        self.audit=AuditLog(':memory:')
        self.addCleanup(self.audit.close)

    def seed_learning(self, ident, symbol, at):
        self.audit.connection.execute('''INSERT INTO learning_examples
            (signal_id,matured_at,signal_timestamp,symbol,success_before_stop,reached_second_target,
             maximum_return_percent,minimum_return_percent,setup_change_percent)
            VALUES(?,?,?,?,1,0,1,0,1)''',(ident,at+900,at,symbol))

    def seed_confirmation(self, symbol, at, evaluated=1):
        self.audit.connection.execute('''INSERT INTO confirmation_events
            (started_at,resolved_at,symbol,trigger_price,resolution_price,accepted,reason,
             evaluated_at,immediate_success) VALUES(?,?,?,100,100,1,'test',?,0)''',
            (at,at+20,symbol,evaluated))

    def test_indexed_history_preserves_duplicate_boundaries(self):
        for ident,(symbol,delta,evaluated) in enumerate([
            ('LOW',-60,1),('HIGH',60,1),('OUT_LOW',-60.001,1),
            ('OUT_HIGH',60.001,1),('PENDING',0,None)],1):
            self.seed_learning(ident,symbol,1000)
            self.seed_confirmation(symbol,1000+delta,evaluated)
        self.seed_learning(6,'OTHER',1000)
        self.seed_confirmation('DIFFERENT',1000)
        # Closed confirmations supersede only matching symbols within inclusive +/-60s.
        self.assertEqual(self.audit.build_learning_report(2000).successes,4)
        for symbol,expected in [('LOW',1),('HIGH',1),('OUT_LOW',2),('OUT_HIGH',2),('PENDING',1),('OTHER',1)]:
            p=self.audit.build_learning_profile(symbol,2000,{})
            self.assertEqual(p.symbol_examples,expected)
            self.assertEqual(p.symbol_successes,0 if symbol in ('LOW','HIGH') else 1)

    def test_large_history_has_bounded_sql_work(self):
        db=self.audit.connection
        db.executemany('''INSERT INTO confirmation_events
            (started_at,resolved_at,symbol,trigger_price,resolution_price,accepted,reason,evaluated_at,immediate_success)
            VALUES(?,?,?,100,100,1,'test',1,0)''',
            ((i*10,i*10+20,f'C{i%400}') for i in range(65000)))
        for i in range(6000): self.seed_learning(i+1,f'C{i%400}',i*130+3)
        calls=0
        def bound():
            nonlocal calls
            calls+=1
            return calls>3000
        db.set_progress_handler(bound,1000)
        try:
            self.audit.build_learning_profile('C3',800000,{})
            self.audit.build_learning_report(800000)
        finally: db.set_progress_handler(None,0)
        self.assertLess(calls,3000)  # Old correlated full scan exhausts this VM budget.

    def test_timing_keeps_result_and_exception(self):
        data={}
        with patch('bot.main.time.perf_counter',side_effect=[1,1.25,2,2.5]):
            self.assertEqual(timed_entry_call(data,'AI',lambda x:x+1,3),4)
            with self.assertRaises(ValueError):
                timed_entry_call(data,'AI',Mock(side_effect=ValueError('test')))
        self.assertEqual(data['stages'],{'AI':.75})

    def test_early_rejection_is_measured_and_daily_cutoff_applies(self):
        signal=PumpSignal('TEST',100,3,300,'лидер')
        market,ai,trader=Mock(),Mock(),Mock()
        with patch('bot.main.time.time',return_value=105):
            result=process_signal(signal,{},100,market,self.audit,trader,ai,Mock(),'owner',
                                  SimpleNamespace(),SignalMarketContext(1000,.5,100,60))
        self.assertFalse(result)
        ai.analyze_momentum.assert_not_called()
        trader.open_on_signal.assert_not_called()
        row=self.audit.connection.execute('SELECT queue_seconds,total_seconds,stages_json FROM rocket_entry_latency').fetchone()
        self.assertEqual(row[0],5)
        self.assertGreaterEqual(row[1],0)
        self.assertEqual(json.loads(row[2]),{})
        self.assertIn('Измерено решений: 1',self.audit.entry_latency_report_text(105))
        self.assertIn('Измерено решений: 0',self.audit.entry_latency_report_text(86506))
