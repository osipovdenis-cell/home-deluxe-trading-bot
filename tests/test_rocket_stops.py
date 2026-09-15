import tempfile
import unittest
from pathlib import Path

from bot.rocket_stops import RocketStopAudit, replay
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

    def make_case(self, path):
        trader=PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,.2)
        db=trader.connection
        db.execute('CREATE TABLE samples(timestamp REAL,symbol TEXT,price REAL)')
        trader.open_on_signal('R',100,'лидер',80,0)
        trader.update_positions({'R':99.4},10)
        # Dense observations: dip to -1.6%, recover above +3%, exit at +2%.
        for t in range(10,3601,10):
            p = 99.4 if t<20 else 98.4 if t<30 else 103 if t<40 else 102
            db.execute('INSERT INTO samples VALUES(?,?,?)',(t,'R',p))
        db.commit()
        return trader

    def test_comparison_persists_and_uses_historical_cost_not_current_setting(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'trades.db')
            trader=self.make_case(path)
            # Legacy row: infer cost from the actual full exit, not configuration.
            trader.connection.execute('DELETE FROM rocket_stop_costs')
            trader.round_trip_cost_percent=9
            cases,pending,incomplete=trader.stop_audit.collect(3601)
            self.assertEqual((len(cases),pending,incomplete),(1,0,0))
            self.assertAlmostEqual(cases[0]['cost'],.2)
            trader.connection.execute('DELETE FROM samples')
            trader.connection.commit()
            texts='\n'.join(trader.stop_audit.report_texts(4000))
            self.assertIn('закрылись в плюс 1',texts)
            self.assertIn('Разница итогов +1.300 USDT',texts)
            self.assertIn('R,',texts)
            trader.close()
            trader=PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,9)
            self.assertEqual(len(trader.stop_audit.collect(5000)[0]),1)
            trader.close()

    def test_missing_paths_are_excluded_and_pending_paths_are_not_labeled(self):
        trader=self.make_case(':memory:')
        try:
            self.assertEqual(trader.stop_audit.collect(100),( [],1,0))
            trader.connection.execute('DELETE FROM samples WHERE timestamp BETWEEN 100 AND 500')
            self.assertEqual(trader.stop_audit.collect(3601),( [],0,1))
        finally:
            trader.close()

    def test_missing_end_unknown_costs_and_scalps_are_not_counted_as_complete(self):
        trader=self.make_case(':memory:')
        try:
            trader.connection.execute('DELETE FROM samples WHERE timestamp>3500')
            self.assertEqual(trader.stop_audit.collect(3601),( [],0,1))
            trader.connection.execute('DELETE FROM rocket_stop_replays')
            trader.connection.execute('DELETE FROM rocket_stop_costs')
            trader.connection.execute("DELETE FROM paper_fills WHERE side='SELL'")
            self.assertEqual(trader.stop_audit.collect(3601),( [],0,1))
            trader.connection.execute("UPDATE paper_positions SET signal_kind='ранний'")
            self.assertEqual(trader.stop_audit.collect(3601),( [],0,0))
        finally:
            trader.close()

    def test_winning_entries_are_included_and_open_cost_is_frozen(self):
        trader=self.make_case(':memory:')
        try:
            trader.open_on_signal('WIN',100,'аномальный лидер',80,0)
            trader.update_positions({'WIN':103},20)
            trader.update_positions({'WIN':102},30)
            trader.connection.executemany('INSERT INTO samples VALUES(?,?,?)',
                [(t,'WIN',100 if t<20 else 103 if t<30 else 102) for t in range(10,3601,10)])
            trader.open_on_signal('OPEN',100,'лидер',80,0)
            trader.connection.executemany('INSERT INTO samples VALUES(?,?,?)',
                [(t,'OPEN',100.7) for t in range(10,3601,10)])
            trader.round_trip_cost_percent=9
            cases,_,_=trader.stop_audit.collect(3601)
            self.assertEqual(len(cases),3)
            winner=next(c for c in cases if c['symbol']=='WIN')
            self.assertEqual(winner['legs']['0.5']['pnl'],winner['legs']['2.0']['pnl'])
            opened=next(c for c in cases if c['symbol']=='OPEN')
            self.assertAlmostEqual(opened['cost'],.2)
            self.assertEqual(opened['legs']['2.0']['reason'],'OPEN')
        finally:
            trader.close()

    def test_report_is_delivered_without_changing_trades_or_stop_settings(self):
        from unittest.mock import Mock
        from bot.audit import AuditLog
        from bot.main import send_overall_reports
        trader=self.make_case(':memory:')
        audit=AuditLog(':memory:')
        try:
            before=tuple(trader.connection.execute('SELECT * FROM paper_account').fetchone())
            fills=trader.connection.execute('SELECT COUNT(*) FROM paper_fills').fetchone()[0]
            telegram=Mock()
            send_overall_reports(3601,{},audit,trader,telegram,'owner')
            texts='\n'.join(call.args[1] for call in telegram.send.call_args_list)
            self.assertIn('Стопы ракет — сравнение на одинаковых входах',texts)
            self.assertIn('Стоп −2%',texts)
            self.assertEqual(trader.stop_loss_percent,.5)
            self.assertEqual(tuple(trader.connection.execute('SELECT * FROM paper_account').fetchone()),before)
            self.assertEqual(trader.connection.execute('SELECT COUNT(*) FROM paper_fills').fetchone()[0],fills)
        finally:
            trader.close()
            audit.close()


if __name__ == '__main__':
    unittest.main()
