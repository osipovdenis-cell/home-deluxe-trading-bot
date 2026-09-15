import json
import sqlite3
import unittest
from unittest.mock import patch, Mock

from bot.scalp_shadow import ScalpShadow, ScalpQuoteStream
from bot.audit import AuditLog
from bot.probability import train_probability_model


class ScalpShadowTests(unittest.TestCase):
    def test_monitor_events_reach_scalp_audit_without_manual_kind(self):
        from bot.market import MarketMonitor
        # Exercise the production producer -> audit -> shadow chain. Hand-made
        # events with kind='ранний' missed the nullable-kind contract previously.
        for name, prices, expected_accepted, leader in (
            ('accepted', [(321, 100.7)], True, False),
            ('flat', [(321, 100.61)], False, False),
            ('disappeared', [(321, 100.4)], False, False),
            ('rescue', [(321, 100.5), (326, 100.51), (331, 100.62)], True, False),
            ('rescue_expired', [(321, 100.5), (412, 100.51)], False, False),
            ('leader', [(321, 100.7)], True, True),
        ):
            with self.subTest(name=name):
                monitor = MarketMonitor('https://api.binance.com', ('AAAUSDT',),
                                        300, 3, 1800, early_threshold_percent=.5)
                audit = AuditLog(':memory:')
                try:
                    if leader:
                        monitor.market_stats['AAAUSDT'] = (2_000_000, 10)
                    for at in range(0, 301, 15):
                        monitor.update({'AAAUSDT':100 if at < 300 else 100.6}, now=at)
                    for at, price in prices:
                        monitor.update({'AAAUSDT':price}, now=at)
                    event = monitor.drain_confirmation_events()[-1]
                    self.assertEqual(event.accepted, expected_accepted)
                    if leader:
                        self.assertIn('лидер', event.signal_kind)
                    else:
                        self.assertEqual(event.signal_kind, 'скальпинг')
                    audit.record_confirmation_event(
                        event, scalp_now=event.resolved_at, scalp_cost=.2)
                    self.assertEqual(audit.scalp_shadow.active_symbols(),
                                     () if leader else ('AAAUSDT',))
                    if not leader:
                        audit.scalp_shadow.quote(event.resolved_at + 1,'AAAUSDT',100,100.01)
                        state=json.loads(audit.connection.execute('SELECT state FROM scalp_shadow').fetchone()[0])
                        self.assertEqual(state['entered'], event.resolved_at + 1)
                finally:
                    monitor.close()
                    audit.close()

    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.s = ScalpShadow(self.db)
        self.features = dict(confirmation_progress_percent=.1,
                             confirmation_change_5s_percent=.1,
                             confirmation_change_10s_percent=.1,
                             change_60s_percent=.3, pullback_from_high_percent=-.05)

    def tearDown(self):
        self.db.close()

    def candidate(self, kind='ранний', symbol='TEST', now=100, **changes):
        self.s.candidate(symbol, kind, now, dict(self.features, **changes), True, .2)

    def state(self):
        row = self.db.execute('SELECT status,state FROM scalp_shadow ORDER BY id DESC LIMIT 1').fetchone()
        return row[0], json.loads(row[1])

    def finish(self, bid=100):
        for t in range(110,1010,10):
            self.s.quote(t,'TEST',bid,bid+.01)
        self.s.quote(1001,'TEST',bid,bid+.01)

    def test_no_rocket_or_unknown_and_no_duplicates(self):
        self.candidate('аномальный лидер')
        self.candidate(None)
        self.assertEqual(self.s.active_symbols(),())
        self.candidate()
        self.candidate()
        self.assertEqual(self.s.active_symbols(),('TEST',))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM scalp_shadow').fetchone()[0],1)

    def test_actual_ask_bid_and_identical_entries_for_exit_policies(self):
        self.candidate()
        self.s.decision('TEST',100.5,allowed=True,decision='BUY')
        self.s.quote(100.4,'TEST',50,50.01)  # before final decision: cannot enter
        self.s.quote(101,'TEST',99.95,100)
        self.assertEqual(self.state()[1]['entry'],100)
        self.s.quote(102,'TEST',100.8,100.81)
        self.s.quote(103,'TEST',101.2,101.21)
        self.finish(100)
        status,s = self.state()
        self.assertEqual(status,'DONE')
        self.assertEqual(s['label'],1)
        self.assertAlmostEqual(s['legs']['current:all07']['net'],.6)
        self.assertAlmostEqual(s['legs']['current:split']['net'],.8)
        self.assertEqual(s['legs']['current:all07'],s['legs']['continuation:all07'])
        self.assertNotIn('recovery:all07',s['legs'])

    def test_overshot_stop_uses_observed_bid_and_does_not_relabel_rebound(self):
        self.candidate()
        self.s.quote(101,'TEST',99.95,100)
        self.s.quote(102,'TEST',99,99.01)
        self.s.quote(103,'TEST',102,102.01)
        self.finish(100)
        _,s=self.state()
        self.assertEqual(s['label'],0)
        self.assertAlmostEqual(s['legs']['continuation:all07']['net'],-1.2)

    def test_neutral_uses_actual_price_not_zero(self):
        self.candidate()
        self.s.quote(101,'TEST',99.95,100)
        self.finish(100.2)
        _,s=self.state()
        self.assertTrue(s['neutral'])
        self.assertAlmostEqual(s['legs']['continuation:all07']['net'],0)

    def test_gap_excludes_from_training(self):
        self.candidate()
        self.s.quote(101,'TEST',99.95,100)
        self.s.quote(132,'TEST',102,102.01)
        self.assertEqual(self.state()[0],'INCOMPLETE')
        self.assertEqual(self.s.samples(2000),[])

    def test_expiry_overflow_and_stale_decision(self):
        self.candidate()
        self.s.decision('TEST',131,allowed=True)
        self.assertEqual(self.state()[0],'INCOMPLETE')
        self.candidate(symbol='OTHER')
        self.s.expire(101,overflow=True)
        self.assertEqual(self.s.active_symbols(),())

    def test_wide_spread_and_missing_are_not_free_fills(self):
        self.candidate()
        self.s.quote(101,'TEST',99,100)
        self.assertEqual(self.state()[0],'NO_ENTRY')
        self.assertEqual(self.s.samples(2000),[])

    def test_recovery_has_separate_gate_no_ai_veto(self):
        self.candidate(pullback_from_high_percent=-.4)
        self.s.decision('TEST',100.5,decision='SKIP',reason='history')
        self.s.quote(101,'TEST',99.95,100)
        _,s=self.state()
        self.assertIn('recovery:split',s['legs'])
        self.assertNotIn('current:split',s['legs'])
        self.assertNotIn('continuation:split',s['legs'])
        self.assertEqual(s['reason'],'history')

    def test_capacity_is_counted_not_silently_discarded(self):
        for i in range(21):
            self.candidate(symbol=str(i))
        self.assertEqual(len(self.s.active_symbols()),20)
        self.assertEqual(self.state()[0],'CAPACITY')

    def test_no_label_from_after_horizon(self):
        self.candidate()
        self.s.quote(101,'TEST',99.95,100)
        for t in range(110,1000,10):
            self.s.quote(t,'TEST',100,100.01)
        self.s.quote(1001,'TEST',105,105.01)
        self.assertEqual(self.state()[1]['label'],0)
        self.assertTrue(self.state()[1]['neutral'])

    def test_persistence_and_past_only_samples(self):
        self.candidate()
        self.s.quote(101,'TEST',99.95,100)
        self.finish()
        self.assertEqual(len(ScalpShadow(self.db).samples(2000)),1)
        self.assertEqual(self.s.samples(1000),[])

    def test_new_profile_does_not_use_legacy_rocket_history(self):
        audit=AuditLog(':memory:')
        try:
            audit.connection.execute('CREATE TABLE paper_positions(symbol TEXT,status TEXT,closed_at REAL,realized_pnl_usdt REAL)')
            audit.connection.execute("INSERT INTO paper_positions VALUES('TEST','CLOSED',100,-10)")
            p=audit.build_learning_profile('TEST',200,{},strategy='scalp')
            self.assertEqual(p.consecutive_trade_losses,0)
            self.assertEqual(p.symbol_examples,0)
            self.assertEqual(p.score_adjustment,0)
        finally:
            audit.close()

    def test_ordinary_rejection_is_recorded_after_processing(self):
        from bot.main import process_signal
        from bot.market import PumpSignal, SignalMarketContext
        audit=AuditLog(':memory:')
        try:
            audit.scalp_shadow.candidate('TEST','ранний',100,self.features,True,.2)
            market=Mock()
            market.execution_safety.return_value=(False,'spread rejected',.01)
            signal=PumpSignal('TEST',100,1,300,'ранний')
            context=SignalMarketContext(1000,2,100,60,spread_bps=100)
            trader=Mock()
            with patch('bot.main.time.time',return_value=105),patch('builtins.print'):
                result=process_signal(signal,{},100,market,audit,trader,Mock(),Mock(),'chat',Mock(),context)
            self.assertFalse(result)
            trader.open_on_signal.assert_not_called()
            state=json.loads(audit.connection.execute('SELECT state FROM scalp_shadow').fetchone()[0])
            self.assertEqual(state['ready'],105)
            self.assertEqual(state['reason'],'spread rejected')
            self.assertFalse(state['baseline'])
        finally:
            audit.close()

    def test_training_label_and_history_are_the_same(self):
        audit=AuditLog(':memory:')
        try:
            audit.scalp_shadow.candidate('TEST','ранний',100,self.features,True,.2)
            audit.scalp_shadow.quote(101,'TEST',99.95,100)
            audit.scalp_shadow.quote(102,'TEST',99,99.01)
            for t in range(110,1010,10):
                audit.scalp_shadow.quote(t,'TEST',101,101.01)
            audit.scalp_shadow.quote(1001,'TEST',101,101.01)
            p=audit.build_learning_profile('TEST',2000,{},strategy='scalp')
            self.assertEqual((p.symbol_examples,p.symbol_successes),(1,0))
            self.assertEqual(audit.scalp_shadow.samples(2000)[0][2]['label'],0)
        finally:
            audit.close()

    def test_quote_stream_keeps_order_and_rejects_bad_quotes(self):
        stream=ScalpQuoteStream()
        stream.set_symbols(['TEST'])
        with patch('bot.scalp_shadow.time.time',side_effect=[1,2]):
            stream.ingest(dict(s='TEST',b='100',a='100.01'))
            stream.ingest(dict(s='TEST',b='99',a='99.01'))
        stream.ingest(dict(s='TEST',b='nan',a='100'))
        rows,overflow=stream.drain_quotes()
        self.assertEqual([r[2] for r in rows],[100,99])
        self.assertFalse(overflow)

    def test_purge_uses_label_end_not_row_count(self):
        samples=[(i%2,dict(_observed_at=i*10,_label_end=i*10+900)) for i in range(500)]
        with patch('bot.probability._fit', wraps=__import__('bot.probability',fromlist=['_fit'])._fit) as fit:
            model=train_probability_model(samples,purge_overlap=True)
        self.assertIsNotNone(model)
        training=fit.call_args_list[0].args[0]
        self.assertTrue(all(f['_label_end']<4000 for _,f in training))
        self.assertEqual(len(training),310)


if __name__ == '__main__':
    unittest.main()
