import json
import unittest

from bot.rocket_cards import schema, build_card
from bot.trading import PaperTrader


class PostStopSummaryTests(unittest.TestCase):
    def setUp(self):
        self.trader = PaperTrader(':memory:',200,50,4,70,.5,.7,1,1.5,1,0,.2)
        self.addCleanup(self.trader.close)
        self.db = self.trader.connection
        schema(self.db)
        self.trader.open_on_signal('R',100,'лидер',80,0)
        self.trader.update_positions({'R':99.4},10)
        self.row = self.db.execute('SELECT * FROM paper_positions').fetchone()

    def path(self):
        # Recovery occurs early; the deepest low is later with NO recovery.
        self.db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',
            [('R',t,100.8 if 30<=t<60 else 94 if t>=600 else 99.4)
             for t in range(11,3611)])
        self.db.commit()

    def report(self, now=3610):
        return self.trader.post_stop_report_text(now,0)

    def test_early_recovery_survives_later_deeper_low_and_matches_card(self):
        self.path()
        before = tuple(self.db.execute('SELECT * FROM paper_positions').fetchone())
        card = build_card(self.db,self.row,3610)
        self.assertLess(card['windows']['60']['return_to_entry_minutes'],1)
        self.assertEqual(card['windows']['60']['rebound_from_low'],0)
        self.db.execute('INSERT INTO rocket_trade_cards VALUES(1,3610,?)',(json.dumps(card),))
        text = self.report()
        self.assertIn('Полных путей: 1; неполных: 0',text)
        self.assertIn('к цене входа: 1/1; достигли +0,7% от входа: 1/1',text)
        self.assertIn('отскок именно после дна 0.00%',text)
        self.assertEqual(before,tuple(self.db.execute('SELECT * FROM paper_positions').fetchone()))

    def test_known_gap_preserves_observed_return_but_excludes_extrema(self):
        self.path()
        self.db.execute('INSERT INTO rocket_path_gaps VALUES(100,101)')
        text = self.report()
        self.assertIn('Полных путей: 0; неполных: 1',text)
        self.assertIn('возврат к входу зафиксирован: 1/1',text)
        self.assertNotIn('Дно от входа в среднем',text)
        self.assertNotIn('Не вернулись к входу',text)

    def test_incomplete_without_observed_recovery_is_unknown_not_protected(self):
        self.db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',
                            [('R',11,99),('R',3610,98)])
        text = self.report()
        self.assertIn('неполных: 1',text)
        self.assertIn('Остальные исходы неизвестны',text)
        self.assertNotIn('Не вернулись к входу',text)

    def test_pending_window_is_not_a_failure(self):
        self.path()
        self.assertIn('ещё наблюдаются: 1',self.report(3609))
        self.assertNotIn('к цене входа:',self.report(3609))

    def test_full_window_without_recovery_counts_no_return(self):
        self.path()
        self.db.execute('UPDATE rocket_bid_path SET bid=98')
        text = self.report()
        self.assertIn('Не вернулись к входу за полное окно: 1/1',text)

    def test_stale_cache_cannot_certify_complete_window(self):
        self.path()
        card=build_card(self.db,self.row,3610)
        card['windows']['60']['end_quote_at']=3000
        self.db.execute('INSERT INTO rocket_trade_cards VALUES(1,3610,?)',(json.dumps(card),))
        self.assertIn('Полных путей: 0; неполных: 1',self.report())

    def test_pending_cached_window_is_rebuilt_after_maturity(self):
        self.path()
        card=build_card(self.db,self.row,1210)
        self.db.execute('INSERT INTO rocket_trade_cards VALUES(1,1210,?)',(json.dumps(card),))
        self.assertIn('Полных путей: 1',self.report())
