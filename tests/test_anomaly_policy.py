import json
import tempfile
import unittest
from pathlib import Path
from bot.trading import PaperTrader
from bot.rocket_cards import schema, build_card, cards, prune_bid_paths, format_card
from bot.exit_policy import ANOMALY_VERSION


def trader(path=':memory:'):
    return PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,.2)


class AnomalyPolicyTests(unittest.TestCase):
    def test_activation_floor_trail_and_independent_regular_leader(self):
        t=trader();self.addCleanup(t.close)
        t.open_on_signal('A',100,'аномальный лидер',80,0)
        t.open_on_signal('B',100,'лидер',80,0)
        self.assertEqual(t.update_positions({'A':107.99,'B':101},1),[])
        notices=t.update_positions({'A':94,'B':100.9},2)
        self.assertEqual(len(notices),1)
        self.assertEqual(t.connection.execute("SELECT status FROM paper_positions WHERE symbol='A'").fetchone()[0],'OPEN')
        self.assertEqual(t.update_positions({'A':108},3),[])
        self.assertEqual(t.update_positions({'A':108.5},4),[])
        self.assertEqual(t.update_positions({'A':108},5),[])
        notices=t.update_positions({'A':107.5},5.5)
        self.assertEqual(len(notices),1)
        self.assertIn('трейлинг включён от +8%',notices[0].reason)
        t.open_on_signal('C',100,'аномальный лидер',80,6)
        t.update_positions({'C':115},7)
        self.assertEqual(t.update_positions({'C':114.1},8),[])
        self.assertEqual(len(t.update_positions({'C':114},9)),1)

    def test_stop_boundary_restart_and_legacy_position(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'db')
            t=trader(path)
            t.open_on_signal('A',100,'аномальный лидер',80,0)
            t.open_on_signal('LEGACY',100,'аномальный лидер',80,0)
            t.connection.execute("UPDATE paper_positions SET exit_policy_json=NULL WHERE symbol='LEGACY'")
            t.connection.commit();t.close()
            t=trader(path);self.addCleanup(t.close)
            self.assertEqual(len(t.update_positions({'A':93.01,'LEGACY':99.5},1)),1)
            notices=t.update_positions({'A':93},2)
            self.assertEqual(len(notices),1)
            self.assertEqual(notices[0].reason,'стоп-лосс')
            policy=json.loads(t.connection.execute("SELECT exit_policy_json FROM paper_positions WHERE symbol='A'").fetchone()[0])
            self.assertEqual(policy['version'],ANOMALY_VERSION)

    def test_incremental_full_path_and_one_hour_after_exit(self):
        t=trader();self.addCleanup(t.close);db=t.connection;schema(db)
        t.open_on_signal('A',100,'аномальный лидер',80,0)
        db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)', [('A',at,105) for at in range(1,121)])
        row=db.execute('SELECT * FROM paper_positions').fetchone()
        first=build_card(db,row,120)
        db.execute('INSERT INTO rocket_trade_cards VALUES(?,?,?)',(row['id'],120,json.dumps(first)))
        db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)', [('A',at,93 if at==180 else 102) for at in range(121,3782)])
        t.update_positions({'A':93},180)
        row=db.execute('SELECT * FROM paper_positions').fetchone()
        second=build_card(db,row,5000)
        self.assertEqual(second['path_summary']['count'],3780)
        self.assertEqual(second['observation_end'],3780)
        self.assertEqual(second['path_summary']['status'],'complete_observed')
        self.assertEqual(second['path_summary']['high'],105)
        self.assertEqual(second['path_summary']['low'],93)
        self.assertEqual(sum(b['count'] for b in second['lifetime_minute_path']),3780)
        self.assertEqual(second['windows']['60']['status'],'complete')
        self.assertEqual(t.stop_audit.collect(5000),([],0,0))
        self.assertIn('включение трейлинга от +8%',format_card(second))
        db.execute('INSERT INTO rocket_path_gaps VALUES(200,205)')
        self.assertEqual(build_card(db,row,5000)['path_summary']['status'],'incomplete')

    def test_long_open_path_survives_pruning_and_recent_limit(self):
        t=trader();self.addCleanup(t.close);db=t.connection;schema(db)
        t.open_on_signal('A',100,'аномальный лидер',80,0)
        db.execute("INSERT INTO rocket_bid_path VALUES('A',1,100)")
        t.open_on_signal('B',100,'лидер',80,1)
        t.update_positions({'B':99},2)
        prune_bid_paths(db,9*86400)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM rocket_bid_path').fetchone()[0],1)
        result=cards(db,10*86400,limit=1)
        self.assertEqual(len(result),2)
        self.assertEqual(next(c for c in result if c['symbol']=='A')['path_summary']['count'],1)


class LeaderStepTests(unittest.TestCase):
    def test_second_floor_survives_restart_and_trail_continues(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'db')
            t=trader(path)
            t.open_on_signal('L',100,'лидер',80,0)
            self.assertEqual(t.update_positions({'L':101},1),[])
            self.assertEqual(t.update_positions({'L':102},2),[])
            self.assertEqual(t.update_positions({'L':102.5},3),[])
            t.close()
            t=trader(path);self.addCleanup(t.close)
            notices=t.update_positions({'L':102},4)
            self.assertEqual(len(notices),1)
            self.assertIn('защита +2%',notices[0].reason)
            t.open_on_signal('NEXT',100,'лидер',80,5)
            self.assertEqual(t.update_positions({'NEXT':104},6),[])
            self.assertEqual(t.update_positions({'NEXT':103.1},7),[])
            self.assertEqual(len(t.update_positions({'NEXT':103},8)),1)

    def test_legacy_and_anomaly_unchanged_and_step_policy_exported(self):
        t=trader();self.addCleanup(t.close);db=t.connection;schema(db)
        t.open_on_signal('L',100,'лидер',80,0)
        row=db.execute('SELECT * FROM paper_positions').fetchone()
        card=build_card(db,row,1)
        self.assertEqual(card['exit_policy']['protect_steps'],[1.,2.])
        self.assertIn('Ступени защиты',format_card(card))
        self.assertEqual(t.stop_audit.collect(5000),([],0,0))
        t.update_positions({'L':101.5},1)
        self.assertEqual(len(t.update_positions({'L':101},2)),1)
        t.open_on_signal('OLD',100,'лидер',80,3)
        db.execute("UPDATE paper_positions SET exit_policy_json=NULL WHERE symbol='OLD'")
        t.update_positions({'OLD':102.5},4)
        self.assertEqual(t.update_positions({'OLD':102},5),[])
        t.open_on_signal('A',100,'аномальный лидер',80,6)
        t.update_positions({'A':102},7)
        self.assertEqual(t.update_positions({'A':101},8),[])
