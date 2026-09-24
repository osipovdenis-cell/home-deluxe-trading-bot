import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from bot.rocket_cards import schema, build_card, cards, format_card, RocketPathWorker, entry_probe, shadow_summary, RecordedBidStream, RetainedBidBatch
from bot.streams import LeaderOrderFlowStream, PositionBookTickerStream
from bot.trading import PaperTrader


def trader(path=':memory:'):
    return PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,.2)


class RocketCardsTests(unittest.TestCase):
    def setUp(self):
        self.trader=trader()
        self.db=self.trader.connection
        schema(self.db)
        self.trader.open_on_signal('R',100,'лидер',80,0)
        self.trader.update_positions({'R':99.4},10)
        self.row=self.db.execute('SELECT * FROM paper_positions').fetchone()

    def tearDown(self):
        self.trader.close()

    def path(self, wider_recovers=False):
        values=[]
        for t in range(1,3611):
            if wider_recovers:
                p=100 if t<10 else 99.4 if t<20 else 99.2 if t<30 else 101.2 if t==30 else 100.95
            else:
                p=100 if t<10 else 99.4 if t<40 else 98.8 if t<50 else 97.5 if t<60 else 100 if t<120 else 105 if t==120 else 103.8
            values.append(('R',t,p))
        self.db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',values)
        self.db.commit()

    def test_drop_then_rebound_does_not_mean_wider_stop_survived(self):
        self.path()
        card=build_card(self.db,self.row,3610)
        window=card['windows']['20']
        self.assertEqual(window['status'],'complete')
        self.assertAlmostEqual(window['low_from_entry'],-2.5)
        self.assertAlmostEqual(window['minutes_to_low'],40/60)
        self.assertAlmostEqual(window['return_to_entry_minutes'],50/60)
        self.assertAlmostEqual(window['high_from_entry'],5)
        self.assertAlmostEqual(window['rebound_from_low'],(105/97.5-1)*100)
        self.assertTrue(all(l['reason']=='STOP' for l in card['comparisons']['20'].values()))
        self.assertEqual(card['comparisons']['20']['2.0']['at'],50)
        self.assertEqual(len(card['minute_path']),60)
        self.assertIn('сделка #1',format_card(card))

    def test_wider_stop_can_recover_but_no_lookahead_before_window(self):
        self.path(True)
        card=build_card(self.db,self.row,1210)
        self.assertEqual(card['windows']['60']['status'],'pending')
        self.assertNotIn('60',card['comparisons'])
        self.assertLess(card['comparisons']['20']['0.5']['pnl'],0)
        self.assertGreater(card['comparisons']['20']['1.0']['pnl'],0)
        self.assertEqual(card['comparisons']['20']['1.0']['reason'],'TRAIL')

    def test_missing_or_dropped_events_disable_comparison(self):
        self.path()
        self.db.execute('DELETE FROM rocket_bid_path WHERE timestamp BETWEEN 500 AND 550')
        card=build_card(self.db,self.row,3610)
        self.assertEqual(card['windows']['20']['status'],'incomplete')
        self.assertNotIn('20',card['comparisons'])
        self.db.execute('INSERT INTO rocket_path_gaps VALUES(20,21)')
        card=build_card(self.db,self.row,3610)
        self.assertEqual(card['windows']['5']['status'],'incomplete')
        self.assertFalse(card['comparisons'])

    def test_legacy_and_repeated_symbol_are_distinct_and_not_fake_complete(self):
        self.db.execute('CREATE TABLE samples(symbol TEXT,timestamp REAL,price REAL)')
        self.db.executemany('INSERT INTO samples VALUES(?,?,?)',[('R',t,100) for t in range(1,3611)])
        self.trader.open_on_signal('R',102,'лидер',80,100)
        result=cards(self.db,3610)
        self.assertEqual([c['position_id'] for c in result],[2,1])
        self.assertEqual(result[0]['status'],'open')
        self.assertEqual(result[1]['source'],'legacy_last_trade')
        self.assertEqual(result[1]['windows']['20']['status'],'incomplete')
        self.assertFalse(result[1]['comparisons'])

    def test_shadow_summary_excludes_unknown_and_does_not_touch_trading(self):
        before=list(self.db.execute('SELECT * FROM paper_positions').fetchone())
        self.db.execute('INSERT INTO rocket_entry_probes VALUES(1,?)',(json.dumps({'allowed':False}),))
        text=shadow_summary(self.db)
        self.assertIn('пропущено 1',text)
        self.assertIn('результат +0.000',text)
        self.assertEqual(before,list(self.db.execute('SELECT * FROM paper_positions').fetchone()))
        self.db.execute('UPDATE rocket_entry_probes SET payload=?',(json.dumps({'allowed':None}),))
        self.assertIn('закрытых с известной оценкой 0',shadow_summary(self.db))

    def test_new_recording_preserves_earlier_legacy_path_without_certifying_it(self):
        self.db.execute('CREATE TABLE samples(symbol TEXT,timestamp REAL,price REAL)')
        self.db.executemany('INSERT INTO samples VALUES(?,?,?)',[('R',t,98) for t in range(1,901)])
        self.db.execute("INSERT INTO rocket_path_meta VALUES('started',900)")
        self.db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',[('R',t,101) for t in range(900,3611)])
        card=build_card(self.db,self.row,3610)
        self.assertEqual(card['source'],'mixed_legacy_bid')
        self.assertAlmostEqual(card['windows']['5']['low_from_entry'],-2)
        self.assertEqual(card['windows']['5']['status'],'incomplete')
        self.assertFalse(card['comparisons'])

    def test_fresh_probe_requires_recent_data_and_full_windows(self):
        stream=LeaderOrderFlowStream()
        stream.set_symbols(('R',))
        for at in range(40,101):
            stream.ingest({'e':'aggTrade','s':'R','p':str(100+at/100),'q':'10','m':False},at)
            stream.ingest({'s':'R','b':'100','a':'100.1'},at)
        fresh=stream.entry_probe('R',100)
        self.assertTrue(fresh['fresh'])
        self.assertGreater(fresh['changes']['5'],0)
        self.assertFalse(stream.entry_probe('R',110)['fresh'])
        self.assertFalse(stream.entry_probe('UNKNOWN',100)['fresh'])

    def test_recorder_observes_without_closing_position(self):
        class LocalStream(PositionBookTickerStream):
            def start(self): pass
            def close(self): pass
        with tempfile.TemporaryDirectory() as folder:
            path=str(Path(folder)/'test.db')
            live=trader(path)
            live.open_on_signal('R',100,'лидер',80,time.time())
            stream=LocalStream(128)
            worker=RocketPathWorker(path,stream)
            worker.start()
            try:
                deadline=time.time()+3
                while not stream._symbols and time.time()<deadline: time.sleep(.02)
                stream.ingest({'s':'R','b':'95'})
                deadline=time.time()+3
                while time.time()<deadline:
                    if live.connection.execute('SELECT COUNT(*) FROM rocket_bid_path').fetchone()[0]: break
                    time.sleep(.02)
                self.assertGreater(live.connection.execute('SELECT COUNT(*) FROM rocket_bid_path').fetchone()[0],0)
                self.assertEqual(live.open_symbols(),('R',))
                self.assertEqual(live.connection.execute("SELECT COUNT(*) FROM paper_fills WHERE side='SELL'").fetchone()[0],0)
            finally:
                worker.close(); live.close()

    def test_real_depth_heartbeats_record_quiet_market_and_explicit_disconnect(self):
        stream=RecordedBidStream()
        stream.set_symbols(['R'])
        for t in range(11,311):
            stream.ingest({'stream':'r@depth5','data':{'lastUpdateId':1,
                'bids':[['100','1']],'asks':[['100.1','1']]}},t)
        batch=RetainedBidBatch()
        events,overflow,gaps=stream.drain_recording_batch()
        batch.append(events,overflow,10,310,gaps)
        batch.write(self.db)
        self.assertEqual(build_card(self.db,self.row,310)['windows']['5']['status'],'complete')
        stream.interrupted(['R'],200)
        events,overflow,gaps=stream.drain_recording_batch()
        batch.append(events,overflow,310,311,gaps)
        batch.write(self.db)
        self.assertEqual(build_card(self.db,self.row,311)['windows']['5']['status'],'incomplete')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM paper_fills WHERE side="SELL"').fetchone()[0],1)
