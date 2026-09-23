import json
import tempfile
import unittest
from pathlib import Path

from bot.rocket_stops import RocketStopAudit, replay, VERSION
from bot.rocket_cards import schema, build_card
from bot.trading import PaperTrader


class RocketStopTests(unittest.TestCase):
    def test_wider_stop_can_survive_pullback_then_trail_full_position(self):
        path = [(0,100), (10,99.4), (20,98.4), (30,103), (40,102)]
        tight = replay(path,100,.5,.2)
        wide = replay(path,100,2,.2)
        self.assertEqual(tight['reason'],'STOP')
        self.assertAlmostEqual(tight['pnl'],-.4)
        self.assertEqual(wide['reason'],'TRAIL')
        self.assertAlmostEqual(wide['pnl'],.9)
        self.assertAlmostEqual(wide['mae'],-1.6)

    def test_two_percent_stop_happens_before_later_rebound_and_overshoots(self):
        path = [(0,100), (10,99.4), (20,97.6), (30,110)]
        wide = replay(path,100,2,.2)
        self.assertEqual(wide['at'],20)
        self.assertEqual(wide['reason'],'STOP')
        self.assertAlmostEqual(wide['pnl'],-1.3)
        self.assertEqual(wide['mfe'],0)

    def test_profit_floor_overshoot_and_open_mark_are_not_guaranteed_profit(self):
        trail = replay([(0,100),(10,101.3),(20,100.8)],100,2,.2)
        self.assertEqual(trail['reason'],'TRAIL')
        self.assertAlmostEqual(trail['pnl'],.3)
        open_leg = replay([(0,100),(10,100.7)],100,2,.2)
        self.assertEqual(open_leg['reason'],'OPEN')
        self.assertAlmostEqual(open_leg['pnl'],.25)

    def test_replay_matches_live_rocket_exit_rules_at_each_observation(self):
        paths = [
            [(0,100),(10,99.4),(20,98.4),(30,104),(40,102.8)],
            [(0,100),(10,101),(20,101),(30,100.9)],
            [(0,100),(10,101.5),(20,97)],
        ]
        for stop in (.5,1,1.5,2):
            for path in paths:
                trader=PaperTrader(':memory:',200,50,4,70,stop,.7,1,1.5,1,0,.2)
                try:
                    trader.open_on_signal('R',100,'лидер',80,0)
                    for t,p in path[1:]:
                        trader.update_positions({'R':p},t)
                    row=trader.connection.execute('SELECT * FROM paper_positions').fetchone()
                    result=replay(path,100,stop,.2)
                    self.assertEqual(row['status']=='CLOSED',result['reason']!='OPEN')
                    if row['status']=='CLOSED':
                        self.assertAlmostEqual(row['realized_pnl_usdt'],result['pnl'])
                        self.assertEqual(row['closed_at'],result['at'])
                finally:
                    trader.close()

    def make_case(self, path=':memory:'):
        trader=PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,.2)
        db=trader.connection
        schema(db)
        db.execute("INSERT INTO rocket_path_meta VALUES('started',0)")
        db.execute('CREATE TABLE samples(timestamp REAL,symbol TEXT,price REAL)')
        trader.open_on_signal('R',100,'лидер',80,0)
        trader.update_positions({'R':99.4},10)
        values=[('R',t,99.4 if t<20 else 98.4 if t<30 else 103 if t<40 else 102)
                for t in range(1,3611)]
        db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',values)
        db.executemany('INSERT INTO samples VALUES(?,?,?)',[(t,s,p) for s,t,p in values])
        db.commit()
        self.addCleanup(trader.close)
        return trader

    def card(self,trader,now=4000):
        row=trader.connection.execute('SELECT * FROM paper_positions WHERE symbol="R"').fetchone()
        return build_card(trader.connection,row,now)

    def save_card(self,trader,card):
        trader.connection.execute('INSERT OR REPLACE INTO rocket_trade_cards VALUES(?,?,?)',
                                  (card['position_id'],4000,json.dumps(card)))

    def test_matches_card_comparisons_in_both_horizons(self):
        trader=self.make_case()
        card=self.card(trader)
        for minutes in (20,60):
            cases,pending,incomplete=trader.stop_audit.collect(4000,minutes)
            self.assertEqual((len(cases),pending,incomplete),(1,0,0))
            self.assertEqual(cases[0]['legs'],card['comparisons'][str(minutes)])
            self.assertEqual(cases[0]['horizon_end_at'],10+minutes*60)
            self.assertEqual(cases[0]['source'],'bid')

    def test_old_incomplete_cache_and_last_trade_prices_do_not_hide_bid_card(self):
        trader=self.make_case();db=trader.connection
        self.save_card(trader,self.card(trader))
        db.execute('INSERT INTO rocket_stop_replays VALUES(?,?,?,?,?)',
                   ('rocket-stops-v1',1,3601,'INCOMPLETE','{}'))
        db.execute('DELETE FROM rocket_bid_path')
        db.execute('UPDATE samples SET price=90')
        cases,_,_=trader.stop_audit.collect(4000)
        self.assertEqual(len(cases),1)
        self.assertAlmostEqual(cases[0]['legs']['2.0']['pnl'],.9)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM rocket_stop_replays').fetchone()[0],2)

    def test_comparison_persists_and_uses_historical_cost_not_current_setting(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'trades.db')
            trader=self.make_case(path)
            trader.connection.execute('DELETE FROM rocket_stop_costs')
            trader.round_trip_cost_percent=9
            cases,_,_=trader.stop_audit.collect(4000)
            self.assertAlmostEqual(cases[0]['cost'],.2)
            trader.connection.execute('DELETE FROM samples')
            trader.connection.execute('DELETE FROM rocket_bid_path')
            trader.connection.commit()
            other=PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,9)
            try:
                self.assertEqual(other.stop_audit.collect(5000)[0],cases)
            finally: other.close()

    def test_partial_card_and_missing_path_can_be_retried_after_late_batch(self):
        trader=self.make_case();db=trader.connection
        rows=db.execute('SELECT * FROM rocket_bid_path WHERE timestamp BETWEEN 500 AND 550').fetchall()
        db.execute('DELETE FROM rocket_bid_path WHERE timestamp BETWEEN 500 AND 550')
        self.save_card(trader,self.card(trader))
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM rocket_stop_replays').fetchone()[0],0)
        db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',[tuple(r) for r in rows])
        self.assertEqual(len(trader.stop_audit.collect(4001)[0]),1)

    def test_known_recording_gap_excludes_otherwise_dense_prices(self):
        trader=self.make_case()
        trader.connection.execute('INSERT INTO rocket_path_gaps VALUES(500,550)')
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))

    def test_legacy_only_or_pre_recorder_paths_are_excluded(self):
        trader=self.make_case();db=trader.connection
        db.execute("UPDATE rocket_path_meta SET value=100 WHERE key='started'")
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))
        db.execute("UPDATE rocket_path_meta SET value=0 WHERE key='started'")
        db.execute('DELETE FROM rocket_bid_path')
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))

    def test_horizon_is_measured_after_exit_and_excludes_future_prices(self):
        trader=self.make_case();db=trader.connection
        self.save_card(trader,self.card(trader))
        self.assertEqual(trader.stop_audit.collect(3601),([],1,0))
        self.assertEqual(len(trader.stop_audit.collect(3610)[0]),1)
        db.execute('DELETE FROM rocket_stop_replays')
        db.execute('DELETE FROM rocket_trade_cards')
        db.execute('UPDATE rocket_bid_path SET bid=100.2')
        db.execute("INSERT INTO rocket_bid_path VALUES('R',3611,120)")
        leg=trader.stop_audit.collect(4000)[0][0]['legs']['2.0']
        self.assertEqual(leg['reason'],'OPEN')
        self.assertEqual(leg['at'],3610)
        self.assertAlmostEqual(leg['pnl'],0)

    def test_long_horizon_gap_does_not_discard_complete_short_horizon(self):
        trader=self.make_case()
        trader.connection.execute('DELETE FROM rocket_bid_path WHERE timestamp BETWEEN 2000 AND 2200')
        self.assertEqual(len(trader.stop_audit.collect(4000,20)[0]),1)
        self.assertEqual(trader.stop_audit.collect(4000,60),([],0,1))

    def test_missing_end_or_cost_and_scalps_are_excluded(self):
        trader=self.make_case();db=trader.connection
        db.execute('DELETE FROM rocket_bid_path WHERE timestamp>3500')
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))
        db.execute('DELETE FROM rocket_stop_costs')
        db.execute("DELETE FROM paper_fills WHERE side='SELL'")
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))
        db.execute("UPDATE paper_positions SET signal_kind='ранний'")
        self.assertEqual(trader.stop_audit.collect(4000),([],0,0))

    def test_invalid_cached_leg_is_not_accepted_as_full_path(self):
        trader=self.make_case();db=trader.connection
        card=self.card(trader);card['comparisons']['60']['2.0']['pnl']=999
        self.save_card(trader,card)
        db.execute('DELETE FROM rocket_bid_path')
        self.assertEqual(trader.stop_audit.collect(4000),([],0,1))

    def test_winning_trades_included_and_open_actual_positions_wait(self):
        trader=self.make_case();db=trader.connection
        trader.open_on_signal('WIN',100,'аномальный лидер',80,0)
        trader.update_positions({'WIN':103},20)
        trader.update_positions({'WIN':102},30)
        db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',
            [('WIN',t,100 if t<20 else 103 if t<30 else 102) for t in range(1,3631)])
        trader.open_on_signal('OPEN',100,'лидер',80,0)
        cases,pending,incomplete=trader.stop_audit.collect(4000)
        self.assertEqual((len(cases),pending,incomplete),(2,1,0))
        winner=next(c for c in cases if c['symbol']=='WIN')
        self.assertEqual(winner['legs']['0.5']['pnl'],winner['legs']['2.0']['pnl'])

    def test_one_report_separates_open_marks_and_does_not_change_trading(self):
        from unittest.mock import Mock
        from bot.audit import AuditLog
        from bot.main import send_overall_reports
        trader=self.make_case();audit=AuditLog(':memory:');self.addCleanup(audit.close)
        # A tight stop closes; wider variants remain open on the same path.
        trader.connection.execute('UPDATE rocket_bid_path SET bid=99.4')
        before=tuple(trader.connection.execute('SELECT * FROM paper_account').fetchone())
        fills=trader.connection.execute('SELECT COUNT(*) FROM paper_fills').fetchone()[0]
        telegram=Mock();send_overall_reports(4000,{},audit,trader,telegram,'owner')
        texts=[c.args[1] for c in telegram.send.call_args_list if c.args[1].startswith('🛑')]
        self.assertEqual(len(texts),1)
        text=texts[0]
        self.assertIn('Стоп −2%: закрыто 0 (+0/−0), PnL +0.000; открыто 1 (-0.400 USDT)',text)
        self.assertIn('выхода + 20 мин',text);self.assertIn('выхода + 60 мин',text)
        self.assertLess(len(text.encode('utf-16-le'))//2,3900)
        self.assertFalse(any('Последние фактические стопы' in c.args[1] for c in telegram.send.call_args_list))
        self.assertEqual(trader.stop_loss_percent,.5)
        self.assertEqual(tuple(trader.connection.execute('SELECT * FROM paper_account').fetchone()),before)
        self.assertEqual(trader.connection.execute('SELECT COUNT(*) FROM paper_fills').fetchone()[0],fills)


if __name__ == '__main__':
    unittest.main()
