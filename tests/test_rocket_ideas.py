import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from bot.idea_rules import validate,replay
from bot.idea_evaluation import evaluate,week_start
from bot.idea_store import connect,enqueue,finish,SUFFIX
from bot.idea_worker import IdeaWorker,Config
from bot.idea_path import append_quotes,delayed_replay


def rule(feature='btc_change_300s_percent',value=0):
    return dict(type='entry_filter',condition=[dict(feature=feature,op='<',value=value)],action={'skip':True},rationale='test')
def bar(o,h,l,c):return dict(open=o,high=h,low=l,close=c)
POLICY=dict(stop_percent=7,protect_percent=8,trail_pp=1)

class RulesTest(unittest.TestCase):
    def test_forbidden_and_nonfinite(self):
        for f in ('symbol','future_high','actual_pnl_usdt','__import__'):
            with self.assertRaises(ValueError):validate(rule(f))
        for v in (float('nan'),float('inf'),None,True):
            with self.assertRaises(ValueError):validate(rule(value=v))
    def test_threshold_bins(self):
        self.assertEqual(validate(rule(value=.011))[1],validate(rule(value=.019))[1])
        self.assertNotEqual(validate(rule(value=.11))[1],validate(rule(value=.019))[1])
    def test_stop_floor_and_trail(self):
        self.assertAlmostEqual(replay([bar(100,100,93,93)],100,POLICY,.2),-7.2)
        self.assertAlmostEqual(replay([bar(108,108,108,108),bar(108,108,108,108),bar(108,108,107.99,107.99)],100,POLICY,.2),7.79)
        self.assertAlmostEqual(replay([bar(112,112,112,112),bar(111,111,111,111)],100,POLICY,.2),10.8)
        self.assertIsNone(replay([bar(100,107,99,101)],100,POLICY,.2))
    def test_intraminute_worst_and_gaps(self):
        self.assertAlmostEqual(replay([bar(100,112,92,109)],100,POLICY,.2),-8.2)
        self.assertIsNone(replay([bar(100,99,98,100)],100,POLICY,.2))
    def event(self,key='trade:1',**extra):
        return dict(key=key,identity_keys=[key],symbol='AAA',at=1700000000,features={'btc_change_300s_percent':-1},
            complete=True,net_percent=-2,**extra)
    def test_source_excluded(self):
        result=evaluate(rule(),[self.event(),self.event('trade:2')],{'trade:1'},1701000000)
        self.assertEqual(result['events'],1);self.assertEqual(result['excluded']['source_event'],1)
    def test_incomplete_excluded_both_sides(self):
        r=rule();r.update(type='exit_params',action=POLICY)
        e=self.event(path_complete=False)
        result=evaluate(r,[e],set(),1701000000)
        self.assertEqual(result['events'],0);self.assertEqual(result['delta_sum_percent'],0)
    def test_positive_screen_and_no_two_week_data(self):
        now=1702000000;w=week_start(now);events=[]
        for i in range(60):
            e=self.event(f'trade:{i}');e.update(symbol=f'S{i%6}',at=w-(7 if i<30 else 14)*86400+60);events.append(e)
        result=evaluate(rule(),events,set(),now)
        self.assertTrue(result['candidate_for_shadow'])
        for e in events:e['at']=w+1
        self.assertFalse(evaluate(rule(),events,set(),now)['candidate_for_shadow'])
    def test_delay_requires_exact_ask_boundary(self):
        s={};append_quotes(s,[(0,100,100),(60,100,101),(61,92,93)],0,120)
        e=dict(minutes=s['idea_minutes'],quotes=s['idea_quotes'],policy=POLICY,cost=.2)
        self.assertAlmostEqual(delayed_replay(e,60),(92/101-1)*100-.2)
        self.assertIsNone(delayed_replay(e,30))

class DurableTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=str(Path(self.tmp.name)/'bot.sqlite3');self.db=connect(self.path+SUFFIX)
        self.db.execute("INSERT INTO meta VALUES('started','0')");self.db.commit()
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def test_merge_keeps_all_source_ids(self):
        for i,v in ((1,.011),(2,.019)):
            enqueue(self.db,i,{'identity_keys':[f'trade:{i}',f'signal:A:{i}']},10)
            finish(self.db,i,dict(trade_id=i,summary='test',ideas=[rule(value=v)]),20)
        self.assertEqual(self.db.execute('SELECT COUNT(*),MAX(repeats) FROM ideas').fetchone()[:],(1,2))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM idea_sources').fetchone()[0],2)
    def test_timeout_durable_retry_and_budget(self):
        enqueue(self.db,1,{'identity_keys':['trade:1']},10)
        def fail(_):raise TimeoutError()
        cfg=Config(input_usd_per_million=1,output_usd_per_million=1,daily_requests=1)
        worker=IdeaWorker(self.path,'','test',config=cfg,analyse=fail)
        worker.review_one(self.db,100)
        row=self.db.execute('SELECT status,attempts,payload,error FROM reviews').fetchone()
        self.assertEqual(row['status'],'pending');self.assertEqual(row['attempts'],1);self.assertEqual(row['error'],'TimeoutError')
        self.assertIn('trade:1',row['payload'])
        worker.review_one(self.db,1000)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM requests').fetchone()[0],1)
        self.db.close();self.db=connect(self.path+SUFFIX)
        self.assertEqual(self.db.execute('SELECT status FROM reviews').fetchone()[0],'pending')
    def test_weekly_send_retry_not_duplicate(self):
        sent=[]
        def send(text):
            if not sent:sent.append('failed');raise TimeoutError()
            sent.append(text)
        worker=IdeaWorker(self.path,'','test',send)
        with self.assertRaises(TimeoutError):worker.weekly(self.db,1702000000)
        worker.weekly(self.db,1702000001);worker.weekly(self.db,1702000002)
        self.assertEqual(len(sent),2)

    def test_archive_and_report_with_recovered_rejection(self):
        from bot.idea_data import collect
        from bot.idea_worker import report
        with sqlite3.connect(self.path) as source:
            source.executescript("""CREATE TABLE paper_positions(id INTEGER,status TEXT,signal_kind TEXT,
              symbol TEXT,signal_timestamp REAL,opened_at REAL,closed_at REAL,entry_price REAL,
              exit_policy_json TEXT,realized_pnl_usdt REAL,position_usdt REAL,close_reason TEXT,ai_score REAL);
              CREATE TABLE rocket_trade_cards(position_id INTEGER,payload TEXT);""")
            source.execute('INSERT INTO paper_positions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (1,'CLOSED','аномальный лидер','AAA',100.,110.,120.,100.,json.dumps(dict(POLICY,version='test',cost_percent=.2)),1.,50.,'trail',0))
            card=dict(entry_probe=dict(entry_quote_at=110.,idea_features={'btc_change_300s_percent':-1}),
                path_summary={'status':'complete_observed'},lifetime_minute_path=[bar(100,112,92,109)])
            source.execute('INSERT INTO rocket_trade_cards VALUES(?,?)',(1,json.dumps(card)))
        with sqlite3.connect(self.path+'.rocket_daily.sqlite3') as source:
            source.execute('CREATE TABLE episodes(id TEXT,payload TEXT)')
            event=dict(symbol='AAA',at=100.,opened=False,stop=1,cost=.2,leg=dict(status='CLOSED',net=-1))
            source.execute('INSERT INTO episodes VALUES(?,?)',('signal:AAA:100.0',json.dumps(event)))
            event.update(symbol='BBB')
            source.execute('INSERT INTO episodes VALUES(?,?)',('signal:BBB:100.0',json.dumps(event)))
        collect(self.path,self.db,4000.)
        collect(self.path,self.db,4100.)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM reviews').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM events').fetchone()[0],2)
        self.assertEqual(report(self.path)['counts'],{'pending':1})
        archived=json.loads(self.db.execute("SELECT payload FROM events WHERE key='trade:1'").fetchone()[0])
        self.assertEqual(archived['policy']['version'],'test')
        self.assertEqual(archived['features']['btc_change_300s_percent'],-1)
