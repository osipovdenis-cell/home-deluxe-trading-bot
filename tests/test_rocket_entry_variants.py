import json
import sqlite3
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.rocket_entry_variants import evaluate, report_data, report_text, VERSION, EXECUTION_POLICY
from bot.rocket_cards import entry_probe
from bot.market import EntryDynamics, SignalMarketContext


def probe():
    return dict(fresh=True, changes={'5': .1, '15': .2, '60': .3},
                before_context={'volume_ratio_5m': 1, 'spread_bps': 10},
                after_flow={'buy_5s_usdt': 101, 'sell_5s_usdt': 100, 'spread_bps': 10})


class RocketEntryVariantsTests(unittest.TestCase):
    def test_conditions_are_fixed_and_independent(self):
        p = probe()
        self.assertEqual(evaluate(p)['decisions'],dict(A=True,B=True,C=True,D=True))
        p['before_context']['volume_ratio_5m'] = .99
        self.assertEqual(evaluate(p)['decisions'],dict(A=True,B=False,C=True,D=False))
        p['before_context']['volume_ratio_5m'] = 2
        for key in ('5','15','60'):
            saved = p['changes'][key]
            p['changes'][key] = 0
            self.assertEqual(evaluate(p)['decisions'],dict(A=True,B=True,C=False,D=False))
            p['changes'][key] = saved
        p['after_flow']['spread_bps'] = 10.01
        self.assertFalse(evaluate(p)['decisions']['C'])
        p['after_flow']['spread_bps'] = 9
        p['after_flow']['buy_5s_usdt'] = 100
        self.assertFalse(evaluate(p)['decisions']['C'])

    def test_unknown_and_nonfinite_are_never_treated_as_a_pass(self):
        p = probe()
        p['fresh'] = False
        self.assertIsNone(evaluate(p)['decisions']['C'])
        self.assertIsNone(evaluate(p)['decisions']['D'])
        p['fresh'] = True
        p['before_context']['volume_ratio_5m'] = float('nan')
        self.assertIsNone(evaluate(p)['decisions']['B'])
        self.assertIsNone(evaluate({})['decisions']['C'])

    def test_common_cohort_costs_drawdown_missed_profit_and_legacy(self):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.executescript('''CREATE TABLE rocket_entry_probes(position_id INTEGER,payload TEXT);
            CREATE TABLE paper_positions(id INTEGER,symbol TEXT,status TEXT,closed_at REAL,
            realized_pnl_usdt REAL,position_usdt REAL,signal_kind TEXT);''')
        def add(ident,symbol,pnl,stake,decisions,version=VERSION):
            db.execute('INSERT INTO paper_positions VALUES(?,?,?,?,?,?,?)',
                       (ident,symbol,'CLOSED',ident*10,pnl,stake,'лидер'))
            db.execute('INSERT INTO rocket_entry_probes VALUES(?,?)',(ident,json.dumps(
                {'entry_variants':{'version':version,'decisions':decisions},'calculation_ms':ident})))
        add(1,'X',1,100,dict(A=True,B=True,C=False,D=False))
        add(2,'Y',-1,50,dict(A=True,B=False,C=True,D=False))
        add(3,'X',2,50,dict(A=True,B=True,C=True,D=True))
        add(4,'Z',100,50,dict(A=True,B=True,C=None,D=None))
        add(5,'Z',100,50,dict(A=True,B=True,C=True,D=True),version='old')
        before = list(db.execute('SELECT * FROM paper_positions'))
        result = report_data(db)
        self.assertEqual(result['recorded'],4)
        self.assertEqual(result['paired_closed'],3)
        self.assertEqual(result['incomplete'],1)
        self.assertEqual([result['variants'][v]['pnl_usdt'] for v in 'ABCD'],[1.5,2.5,1,2])
        self.assertEqual(result['variants']['A']['closed_pnl_drawdown_usdt'],1)
        self.assertEqual(result['variants']['B']['avoided_losses_usdt'],1)
        self.assertEqual(result['variants']['C']['missed_profit_usdt'],.5)
        self.assertEqual(result['variants']['A']['top_profit_symbol'],'X')
        self.assertEqual(result['calculation_p50_ms'],2.5)
        self.assertIn('Архив: 3 общих',report_text(db))
        for ident in range(6,53):
            add(ident,'X',.1,50,dict(A=True,B=True,C=True,D=True))
        self.assertTrue(report_data(db)['review_ready'])
        self.assertEqual(list(db.execute('SELECT * FROM paper_positions WHERE id<=5')),before)
        db.execute('INSERT INTO paper_positions VALUES(?,?,?,?,?,?,?)',
                   (100,'LIVE','CLOSED',1000,2,50,'лидер'))
        db.execute('INSERT INTO rocket_entry_probes VALUES(?,?)',(100,json.dumps(
            {'entry_policy':EXECUTION_POLICY,'entry_variants':evaluate(probe())})))
        result=report_data(db)
        self.assertEqual(result['paired_closed'],50)
        self.assertEqual(result['live_entries'],1)
        self.assertEqual(result['live_closed'],1)
        self.assertEqual(result['live_pnl_usdt'],2)

    def test_probe_measures_whole_calculation_without_rest_and_records_new_version(self):
        @dataclass
        class Flow:
            buy_5s_usdt: float = 100
            sell_5s_usdt: float = 50
            spread_bps: float = 9
        market = Mock()
        market.rocket_probe = lambda symbol,at: dict(at=at,fresh=True,snapshot=Flow(),
                                                   changes={'5':.1,'15':.2,'60':.3})
        market.leader_entry_quality.return_value = (False,'old shadow rule')
        context = SignalMarketContext(1000,2,100,60,spread_bps=10)
        dynamics = EntryDynamics(.1,.1,.2,.3,.5,-.1,0,0,50)
        with patch('bot.rocket_cards.time.perf_counter',side_effect=[10,10.002]):
            result = entry_probe(market,SimpleNamespace(symbol='R'),context,dynamics,100)
        self.assertAlmostEqual(result['calculation_ms'],2)
        self.assertEqual(result['entry_variants']['version'],VERSION)
        self.assertTrue(result['entry_variants']['decisions']['D'])
        self.assertFalse(result['allowed'])  # Previous experiment remains separate.
        self.assertFalse(market.client.mock_calls)
        self.assertFalse(market.open_on_signal.mock_calls)
        market.rocket_probe = Mock(side_effect=ValueError('unavailable'))
        failed = entry_probe(market,SimpleNamespace(symbol='R'),context,dynamics,100)
        self.assertIsNone(failed['entry_variants']['decisions']['C'])
        self.assertGreaterEqual(failed['calculation_ms'],0)


if __name__ == '__main__':
    unittest.main()
