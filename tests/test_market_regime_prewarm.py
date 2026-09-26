import json
import sqlite3
import unittest
from collections import deque
from types import SimpleNamespace
from bot.market import MarketMonitor
from bot.rocket_market_regime import snapshot, freeze, report_data, VERSION

class PrewarmTests(unittest.TestCase):
    def monitor(self):
        m=MarketMonitor('https://api.binance.com',(),300,3,1800)
        self.addCleanup(m.client.close)
        m.min_quote_volume_usdt=100
        return m

    def test_watch_starts_before_leader_or_signal_and_is_bounded(self):
        m=self.monitor()
        m.market_stats={f'S{i}':(1000,20-i) for i in range(30)}
        m.change_12h_percent={s:1 for s in m.market_stats}
        self.assertEqual(m.order_flow_symbols(now=100),('S0','S1','S2','S3','S4'))
        self.assertFalse(m.leaders)
        self.assertFalse(m.pending_candidates)
        m.pending_candidates={f'ACTIVE{i}':None for i in range(20)}
        self.assertEqual(m.order_flow_symbols(now=101),tuple(m.pending_candidates))

    def test_near_anomaly_is_prewarmed_without_creating_a_signal(self):
        m=self.monitor()
        m.market_stats={f'D{i}':(1000,10-i) for i in range(5)}|{'NEAR':(1000,0),'RED':(1000,99),'THIN':(1,99)}
        m.change_12h_percent={s:1 for s in m.market_stats};m.change_12h_percent['RED']=-1
        m.anomaly_history['NEAR']=deque([(90,100),(100,102)])
        result=m.order_flow_symbols(now=100)
        self.assertIn('NEAR',result);self.assertNotIn('RED',result);self.assertNotIn('THIN',result)
        self.assertFalse(m.pending_candidates)

    def test_ranking_does_not_churn_every_tick_and_required_members_win(self):
        m=self.monitor();m.market_stats={'A':(1000,10)};m.change_12h_percent={'A':1,'B':1}
        self.assertEqual(m.order_flow_symbols(now=100),('A',))
        m.market_stats={'B':(1000,10)}
        self.assertEqual(m.order_flow_symbols(now=101),('A',))
        m.pending_candidates={'B':None}
        self.assertEqual(m.order_flow_symbols(now=102),('B','A'))
        self.assertEqual(m.order_flow_symbols(now=131),('B','A'))

class MarketRegimeTests(unittest.TestCase):
    def history(self):
        symbols=['BTCUSDT']+[f'S{i}' for i in range(9)]
        histories={s:deque((t,100+t*(.001 if i in (1,2,3,4) else -.001)) for t in range(601)) for i,s in enumerate(symbols)}
        return symbols,histories

    def test_snapshot_requires_full_fresh_coverage_and_does_not_use_future(self):
        symbols,h=self.history();s=snapshot(h,symbols,600)
        self.assertEqual(s['state'],'KNOWN');self.assertEqual(s['breadth_60'],40)
        self.assertFalse(freeze(s,605)['passed'])
        h['BTCUSDT'].append((700,200))
        self.assertEqual(snapshot(h,symbols,600),s)
        self.assertEqual(snapshot(h,symbols,620)['state'],'UNKNOWN')
        self.assertEqual(freeze(s,611)['state'],'UNKNOWN')
        self.assertEqual(freeze(s,599)['state'],'UNKNOWN')

    def test_missing_btc_and_gaps_are_not_favourable_market(self):
        symbols,h=self.history();h['BTCUSDT']=deque((t,p) for t,p in h['BTCUSDT'] if t<450 or t>470)
        self.assertEqual(snapshot(h,symbols,600)['state'],'UNKNOWN')
        symbols,h=self.history()
        for s in symbols[1:4]:h[s].clear()
        self.assertEqual(snapshot(h,symbols,600)['state'],'UNKNOWN')
        self.assertIsNone(freeze(None,600)['passed'])

    def test_filter_requires_both_adverse_conditions(self):
        for btc,breadth,passed in [(-.1,49,False),(0,49,True),(-.1,50,True),(.1,80,True)]:
            row=freeze(dict(at=600,state='KNOWN',btc_300=btc,breadth_60=breadth),600)
            self.assertIs(row['passed'],passed)
        self.assertIsNone(freeze(dict(at=600,state='KNOWN',btc_300=float('nan'),breadth_60=30),600)['passed'])

    def test_comparison_uses_frozen_new_live_decisions_and_common_closed_cohort(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE rocket_entry_probes(position_id INTEGER,payload TEXT); CREATE TABLE paper_positions(id INTEGER,signal_kind TEXT,status TEXT,realized_pnl_usdt REAL,position_usdt REAL);')
        for ident,passed,pnl,status,policy in [(1,True,2,'CLOSED','rocket-entry-C-live-v1'),(2,False,-1,'CLOSED','rocket-entry-C-live-v1'),(3,False,1,'CLOSED','rocket-entry-C-live-v1'),(4,None,99,'CLOSED','rocket-entry-C-live-v1'),(5,True,99,'OPEN','rocket-entry-C-live-v1'),(6,True,99,'CLOSED','old')]:
            db.execute('INSERT INTO paper_positions VALUES(?,?,?,?,?)',(ident,'лидер',status,pnl,100))
            db.execute('INSERT INTO rocket_entry_probes VALUES(?,?)',(ident,json.dumps(dict(entry_policy=policy,market_regime=dict(version=VERSION,state='KNOWN' if passed is not None else 'UNKNOWN',passed=passed)))))
        before=list(db.execute('SELECT * FROM paper_positions'))
        r=report_data(db)
        self.assertEqual((r['recorded'],r['paired_closed'],r['unknown'],r['open']),(5,3,1,1))
        self.assertEqual((r['base_pnl'],r['filtered_pnl'],r['avoided_losses'],r['missed_profit']),(1,1,.5,.5))
        self.assertEqual(list(db.execute('SELECT * FROM paper_positions')),before)
