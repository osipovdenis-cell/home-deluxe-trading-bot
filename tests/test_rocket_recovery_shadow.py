import json
import sqlite3
import unittest
from bot.rocket_recovery_shadow import (RecoveryShadow,window_snapshot,new_leg,advance,report_data,report_text)
from bot.streams import LeaderOrderFlowStream


def probe(at=100,passed=False):
    return dict(at=at,fresh=True,allowed=True,signal_price=100,
                entry_bid=99.99,entry_quote_at=100,
                before_dynamics={'btc_change_300s_percent':-.1,'market_breadth_60s_percent':25},
                recovery_windows=dict(complete=True,passed=passed,ask=100,bid=99.99,quote_at=at))


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript("""
            CREATE TABLE paper_positions(id INTEGER PRIMARY KEY,symbol TEXT,opened_at REAL,entry_price REAL);
            CREATE TABLE rocket_stop_costs(position_id INTEGER PRIMARY KEY,cost_percent REAL);
            CREATE TABLE rocket_bid_path(symbol TEXT,timestamp REAL,bid REAL);
            INSERT INTO paper_positions VALUES(1,'X',100,100);
            INSERT INTO rocket_stop_costs VALUES(1,.2);
        """)
        self.p=probe()
        self.model=RecoveryShadow(self.db,lambda symbol,original:self.p)

    def state(self):
        return json.loads(self.db.execute('SELECT payload FROM rocket_recovery_pairs WHERE position_id=1').fetchone()[0])

    def tick(self,t,bid=99.99,passed=False,ask=100):
        self.db.execute('INSERT INTO rocket_bid_path VALUES(?,?,?)',('X',t,bid))
        self.p=probe(t,passed)
        self.p['recovery_windows'].update(bid=bid,ask=ask)
        self.model.tick(t)

    def test_disjoint_windows_boundaries_and_no_future_data(self):
        quotes=[(t,100+t*.01,1,101,1) for t in range(88,102)]
        trades=[(90,100,999,False),(95,100,3,True),(96,100,2,True),(100,100,1,False),(101,100,999,False)]
        w=window_snapshot(trades,quotes,100)
        self.assertTrue(w['passed'])
        self.assertEqual([(x['buy'],x['sell']) for x in w['windows']],[(3,0),(2,1)])
        self.assertEqual(w['quote_at'],100)
        self.assertFalse(window_snapshot([(90,100,0,True),(95,100,3,False),(100,100,2,True)],quotes,100)['passed'])
        self.assertIsNone(window_snapshot(trades,quotes[:2],100)['passed'])

    def test_flat_bid_fails_despite_buying_and_missing_quotes_unknown(self):
        ts=[(89,100,1,True),(94,100,2,True),(99,100,2,True)]
        qs=[(t,100,1,100.1,1) for t in range(88,101)]
        self.assertFalse(window_snapshot(ts,qs,100)['passed'])
        self.assertIsNone(window_snapshot(ts,[qs[0],qs[-1]],100)['passed'])

    def test_stream_exports_windows_without_changing_freshness(self):
        stream=LeaderOrderFlowStream()
        stream.set_symbols(['X'])
        for t in range(40,101):
            stream.ingest({'s':'X','e':'aggTrade','p':str(100+t*.01),'q':'1','m':False},t)
            stream.ingest({'s':'X','b':str(100+t*.01),'a':str(100.02+t*.01)},t)
        p=stream.entry_probe('X',100)
        self.assertTrue(p['fresh'])
        self.assertTrue(p['recovery_windows']['passed'])

    def test_immediate_same_price_idempotent_and_stop_costs(self):
        self.model.seed(1,probe(passed=True))
        self.model.seed(1,probe())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM rocket_recovery_pairs').fetchone()[0],1)
        self.tick(101,99.4)
        d=report_data(self.db)
        self.assertEqual(d['complete'],1)
        self.assertAlmostEqual(d['variants']['A']['realized'],-.4)
        self.assertEqual(d['variants']['A'],d['variants']['B'])
        self.assertEqual(d['weak_market_pairs'],1)

    def test_delayed_entry_uses_later_ask_and_excludes_preentry_path(self):
        self.model.seed(1,probe())
        self.tick(101,99.4)  # A stops, B still waiting.
        self.tick(102,99.6,True,99.7)
        s=self.state()
        self.assertEqual(s['A']['status'],'CLOSED')
        self.assertEqual(s['B']['entry'],99.7)
        self.assertEqual(s['B']['entered'],102)
        self.tick(103,100.8)
        self.tick(104,100.69)
        s=self.state()
        self.assertEqual(s['B']['reason'],'TRAIL')
        self.assertGreater(s['B']['net'],0)
        self.assertEqual(report_data(self.db)['complete'],1)

    def test_expired_wait_counts_missed_profit(self):
        self.model.seed(1,probe())
        for t in range(101,191):
            self.tick(t,101.2 if t==101 else 100.9)
        d=report_data(self.db)
        self.assertEqual(self.state()['B']['status'],'NO_ENTRY')
        self.assertEqual(d['missed_winners'],1)
        self.assertAlmostEqual(d['missed_profit'],.35)
        self.assertIn('Б пропустил',report_text(self.db))

    def test_expired_wait_counts_avoided_loss(self):
        self.model.seed(1,probe())
        for t in range(101,191):
            self.tick(t,99.4)
        d=report_data(self.db)
        self.assertEqual(d['avoided_losers'],1)
        self.assertAlmostEqual(d['avoided_loss'],.4)

    def test_gap_and_restart_cannot_manufacture_better_entry(self):
        self.model.seed(1,probe())
        self.tick(101,99.4)
        self.model=RecoveryShadow(self.db,lambda symbol,original:self.p)
        self.tick(105,99.7,True,99.8)
        self.assertEqual(self.state()['B']['status'],'INCOMPLETE')
        self.assertEqual(report_data(self.db)['complete'],0)

    def test_unknown_window_not_a_rejection_and_not_a_success(self):
        p=probe()
        p['recovery_windows']['complete']=False
        self.model.seed(1,p)
        self.tick(101,100,True,100.01)
        self.assertEqual(self.state()['B']['status'],'INCOMPLETE')

    def test_horizon_mark_not_realized_profit_and_gap_excluded(self):
        leg=new_leg(100,100,99.99)
        advance(leg,[(t,100.4) for t in range(101,111)],110,110,.5,.2)
        self.assertEqual(leg['status'],'MARKED')
        self.assertAlmostEqual(leg['net'],.2)
        leg=new_leg(100,100,99.99)
        advance(leg,[(106,102)],106,110,.5,.2)
        self.assertEqual(leg['status'],'INCOMPLETE')

    def test_quote_staleness_spread_and_drift_do_not_enter(self):
        self.model.seed(1,probe())
        self.tick(101,100,True,101)  # wide spread
        self.assertEqual(self.state()['B']['status'],'WAIT')
        self.tick(102,100.6,True,100.61)  # drift exceeds fixed stop
        self.assertEqual(self.state()['B']['status'],'WAIT')
        self.p=probe(103,True)
        self.p['recovery_windows']['quote_at']=99
        self.model.tick(103)
        self.assertEqual(self.state()['B']['status'],'WAIT')

    def test_legacy_probe_is_not_backfilled(self):
        self.model.seed(1,{})
        self.assertEqual(report_data(self.db)['recorded'],0)


if __name__=='__main__':
    unittest.main()
