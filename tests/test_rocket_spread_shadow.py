import sqlite3
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.audit import AuditLog
from bot.main import process_signal
from bot.market import MarketMonitor, PumpSignal, SignalMarketContext, EntryDynamics
from bot.rocket_cards import RetainedBidBatch, schema


class SpreadShadowTests(unittest.TestCase):
    def setUp(self):
        self.audit = AuditLog(':memory:')
        self.shadow = self.audit.rocket_spread
        self.signal = PumpSignal('TEST', 100, 3, 300, 'лидер')
        self.context = SignalMarketContext(10000, 2, 500, 60, spread_bps=10,
            flow_cvd_60s_percent=20, flow_trade_rate_acceleration=2,
            flow_price_change_60s_percent=.5, flow_price_efficiency_per_10k=1,
            flow_spread_change_bps=0)
        self.dynamics = EntryDynamics(.1,.2,.5,1,3,0,0,0,70)

    def tearDown(self):
        self.audit.close()

    def observe(self, context=None, at=100):
        self.shadow.observe(self.signal, context or self.context, self.dynamics,
                            MarketMonitor, at, .5, .2)

    def test_stable_gate_is_shadow_only_and_counts_rejected_signal(self):
        self.assertFalse(MarketMonitor.leader_entry_quality(self.context, self.dynamics)[0])
        self.observe()
        self.shadow.tick({'TEST':100}, 101)
        statuses = self.audit.connection.execute('SELECT variant,status FROM rocket_spread_legs ORDER BY variant').fetchall()
        self.assertEqual(statuses, [('A','NO_ENTRY'),('B','OPEN')])
        self.assertEqual(self.audit.connection.execute('SELECT COUNT(*) FROM rocket_ab_episodes').fetchone()[0], 0)
        self.shadow.tick({'TEST':99}, 102)
        self.shadow.tick({}, 3700)
        report = self.shadow.report(3700)
        self.assertIn('Полных пар: 1', report)
        self.assertIn('B: закрыто 1, плюс 0', report)
        self.assertIn('не моделируются', report)

    def test_contraction_is_same_control_and_costs_are_frozen(self):
        self.observe(replace(self.context, flow_spread_change_bps=-1))
        self.shadow.tick({'TEST':100}, 101)
        self.shadow.tick({'TEST':102}, 102)
        self.shadow.tick({'TEST':101}, 103)
        legs = self.audit.connection.execute('SELECT net FROM rocket_spread_legs ORDER BY variant').fetchall()
        self.assertEqual(legs[0], legs[1])
        self.assertGreater(legs[0][0], 0)
        self.assertAlmostEqual(legs[0][0], (101*(1-10/20000)/(100*(1+10/20000))-1)*100-.2)

    def test_other_gates_missing_and_widening_are_not_relaxed(self):
        for context in [replace(self.context, spread_bps=26),
                        replace(self.context, flow_spread_change_bps=.01),
                        replace(self.context, flow_spread_change_bps=None),
                        replace(self.context, flow_spread_change_bps=float('nan')),
                        replace(self.context, flow_cvd_60s_percent=-1),
                        replace(self.context, flow_trade_rate_acceleration=.9)]:
            self.observe(context)
        self.assertEqual(self.audit.connection.execute('SELECT COUNT(*) FROM rocket_spread_episodes').fetchone()[0],0)

    def test_missing_path_excluded_and_duplicate_throttled(self):
        self.observe()
        self.observe(at=101)
        self.shadow.tick({'TEST':100}, 102)
        self.shadow.tick({'TEST':101}, 150)
        self.shadow.tick({}, 3701)
        self.assertIn('Полных пар: 0; ожидаются: 0; неполных: 1',self.shadow.report(3701))

    def test_real_rejection_creates_pair_before_return_and_no_trade(self):
        market = Mock()
        market.execution_safety.return_value = (True,None,.01)
        market.entry_dynamics.return_value = self.dynamics
        market.leader_entry_quality.side_effect = MarketMonitor.leader_entry_quality
        trader = Mock()
        settings = SimpleNamespace(paper_stop_loss_percent=.5,estimated_round_trip_cost_percent=.2)
        with patch('bot.main.time.time',return_value=100), patch('builtins.print'):
            self.assertFalse(process_signal(self.signal,{},100,market,self.audit,trader,
                              Mock(),Mock(),'chat',settings,self.context))
        trader.open_on_signal.assert_not_called()
        self.assertEqual(self.audit.connection.execute('SELECT COUNT(*) FROM rocket_spread_episodes').fetchone()[0],1)
        reason=self.audit.connection.execute('SELECT reason FROM rocket_gate_decisions').fetchone()[0]
        self.assertEqual(reason,'спред лидера не сокращается')


class RecorderRetryTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        schema(self.db)
        self.batch=RetainedBidBatch()

    def tearDown(self):
        self.db.close()

    def test_rollback_retains_all_quotes_and_dedup_state(self):
        self.batch.append([(1,'X',100),(1.1,'X',99),(1.2,'X',102)],False,0,2)
        failing=Mock(wraps=self.db)
        failing.commit.side_effect=sqlite3.OperationalError('database is locked')
        with self.assertRaises(sqlite3.OperationalError): self.batch.write(failing)
        self.db.rollback()
        self.assertEqual(len(self.batch.events),3)
        self.assertFalse(self.batch.last)
        self.batch.append([(2.1,'X',101)],False,0,3)
        self.batch.write(self.db)
        self.assertEqual(self.db.execute('SELECT bid FROM rocket_bid_path ORDER BY timestamp').fetchall(),
                         [(100,),(99,),(102,),(101,)])
        self.assertFalse(self.batch.events)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rocket_path_gaps').fetchone()[0],0)

    def test_real_overflow_remains_incomplete_even_after_retry(self):
        self.batch.append([(1,'X',99)],True,0,2)
        self.batch.write(self.db)
        self.assertEqual(self.db.execute('SELECT * FROM rocket_path_gaps').fetchall(),[(0.,2.)])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rocket_bid_path').fetchone()[0],0)

    def test_later_analytics_rollback_does_not_lose_committed_path(self):
        self.batch.append([(1,'X',99),(2,'X',102)],False,0,2)
        self.batch.write(self.db)
        self.db.execute("INSERT INTO rocket_entry_probes VALUES(1,'{}')")
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rocket_bid_path').fetchone()[0],2)
