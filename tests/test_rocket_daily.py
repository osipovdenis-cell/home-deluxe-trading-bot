import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from bot.rocket_daily import DailyModel, SUFFIX, report_data, report_text, outcome

class DailyTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.m=DailyModel(self.db)
    def tearDown(self): self.db.close()
    def seed(self, ident='x', at=100):
        self.m.capture(dict(id=ident,symbol='TEST',signal_at=90,at=at,stop=1.,cost=.2,
                            source='signal',reason='volume',opened=False));self.db.commit()
    def state(self, ident='x'):
        return json.loads(self.db.execute('SELECT payload FROM episodes WHERE id=?',(ident,)).fetchone()[0])
    def test_rejection_continues_to_profitable_trailing_exit(self):
        self.seed()
        self.m.tick(103,[(100,'TEST',99.9,100),(101,'TEST',102,102.1),(103,'TEST',101,101.1)])
        s=self.state();self.assertEqual(s['leg']['status'],'CLOSED')
        self.assertAlmostEqual(s['leg']['net'],.8);self.assertAlmostEqual(outcome([s])['pnl'],.4)
    def test_stop_before_rally_stays_loss(self):
        self.seed();self.m.tick(103,[(100,'TEST',100,100),(101,'TEST',98.9,99),(103,'TEST',110,111)])
        self.assertEqual(self.state()['leg']['reason'],'STOP');self.assertAlmostEqual(self.state()['leg']['net'],-1.3)
    def test_gap_never_becomes_winner(self):
        self.seed();self.m.tick(110,[(100,'TEST',100,100),(110,'TEST',105,106)])
        self.assertEqual(self.state()['leg']['status'],'INCOMPLETE')
    def test_first_quote_missing_and_overflow(self):
        self.seed();self.m.tick(106,[(106,'TEST',100,100)])
        self.assertEqual(self.state()['leg']['status'],'INCOMPLETE')
        self.seed('y',200);self.m.tick(201,[(200,'TEST',100,100)],True)
        self.assertEqual(self.state('y')['leg']['status'],'INCOMPLETE')
    def test_spread_can_trigger_immediate_stop(self):
        self.seed();self.m.tick(100,[(100,'TEST',98,100)])
        self.assertEqual(self.state()['leg']['reason'],'STOP')
    def test_horizon_is_mark_not_realized_profit(self):
        self.seed();self.m.tick(3700,[(t,'TEST',100,100) for t in range(100,3701,5)])
        s=self.state();self.assertEqual(s['leg']['status'],'MARKED')
        self.assertEqual(outcome([s])['losing'],0);self.assertAlmostEqual(outcome([s])['marked_pnl'],-.1)
    def test_duplicate_and_restart(self):
        self.seed();self.seed()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM episodes').fetchone()[0],1)
        DailyModel(self.db);self.assertEqual(self.state()['leg']['status'],'INCOMPLETE')
    def test_exact_delayed_purchase_link_and_export(self):
        with tempfile.TemporaryDirectory() as root:
            path=str(Path(root)/'main.db');main=sqlite3.connect(path)
            main.execute('CREATE TABLE paper_positions(id,symbol,signal_timestamp,opened_at,closed_at,status,realized_pnl_usdt,signal_kind)')
            main.execute("INSERT INTO paper_positions VALUES(1,'TEST',90,130,140,'CLOSED',-.6,'лидер')");main.commit()
            ledger=sqlite3.connect(path+SUFFIX);m=DailyModel(ledger)
            for ident,signal in [('a',90),('b',91)]:
                m.capture(dict(id=ident,symbol='TEST',signal_at=signal,at=100,stop=1.,cost=.2,
                    source='signal',reason='volume',opened=False))
            m.tick(103,[(100,'TEST',100,100),(101,'TEST',98.9,99)]);ledger.close()
            d=report_data(main,400)
            self.assertEqual([s['classification'] for s in d['episodes']],['BOUGHT','REJECTED'])
            self.assertEqual(d['actual']['closed'],1);self.assertEqual(d['actual']['pnl'],-.6)
            self.assertIn('отказов 1',report_text(main,400));main.close()
