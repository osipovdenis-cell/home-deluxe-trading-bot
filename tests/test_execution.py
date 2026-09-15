import json
import tempfile
import time
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import Mock, patch

from bot.execution import PositionExitWorker, fetch_quote, fresh_entry
from bot.streams import PositionBookTickerStream
from bot.trading import PaperTrader


def make_trader(path):
    return PaperTrader(path,200,50,4,70,.5,.7,1,1.5,1,0,.2,ordinary_max_open_positions=0)


class ExecutionTests(unittest.TestCase):
    def test_ordered_prices_retain_first_crossing_instead_of_only_extrema(self):
        trader=make_trader(':memory:')
        try:
            trader.open_on_signal('R',100,'лидер',80,1)
            stream=PositionBookTickerStream()
            stream.set_symbols(('R',))
            with patch('bot.streams.time.time',side_effect=[2,3,4,5]):
                for price in (100,99.4,98,96.27):
                    stream.ingest({'s':'R','b':str(price)})
            events=stream.drain_events()
            self.assertEqual([p for _,_,p in events],[100,99.4,98,96.27])
            notices=[]
            for at,symbol,bid in events:
                notices.extend(trader.update_positions({symbol:bid},at))
            self.assertEqual(len(notices),1)
            self.assertAlmostEqual(notices[0].pnl_percent,-.8)
            self.assertEqual(notices[0].price,99.4)
        finally:
            trader.close()

    def test_exit_worker_closes_while_main_does_no_work_and_does_not_double_sell(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'bot.db')
            trader=make_trader(path)
            stream=PositionBookTickerStream()
            trader.open_on_signal('R',100,'лидер',80,time.time())
            stream.set_symbols(('R',))
            stream.ingest({'s':'R','b':'100'})
            client=Mock()
            worker=PositionExitWorker(lambda:make_trader(path),stream,'unused',lambda:client)
            worker.start()
            try:
                for price in (99.4,98,96.27):
                    stream.ingest({'s':'R','b':str(price)})
                # No main loop or Telegram is running; the exit must still occur.
                text=worker.messages.get(timeout=2)
                self.assertIn('-0.80%',text)
                row=trader.connection.execute('SELECT * FROM paper_positions').fetchone()
                self.assertEqual(row['status'],'CLOSED')
                self.assertAlmostEqual(row['realized_pnl_usdt'],-.4)
                diagnostic=json.loads(trader.connection.execute(
                    'SELECT payload FROM paper_exit_diagnostics').fetchone()[0])
                self.assertEqual(diagnostic['bid'],99.4)
                self.assertLess(diagnostic['handling_delay_ms'],2000)
                self.assertEqual(trader.connection.execute(
                    "SELECT COUNT(*) FROM paper_fills WHERE side='SELL'").fetchone()[0],1)
                self.assertTrue(worker.healthy())
            finally:
                worker.close()
                trader.close()
            self.assertFalse(worker.healthy())

    def test_stale_and_preentry_quotes_cannot_close_a_new_position(self):
        trader=make_trader(':memory:')
        stream=PositionBookTickerStream()
        worker=PositionExitWorker(None,stream,'unused')
        try:
            trader.open_on_signal('R',100,'лидер',80,100)
            with patch('bot.execution.time.time',return_value=101), patch('builtins.print'):
                worker.handle_quote(trader,98,'R',90)
                worker.handle_quote(trader,99.5,'R',90)
            self.assertEqual(trader.open_symbols(),('R',))
        finally:
            trader.close()

    def test_single_real_price_gap_is_not_filled_at_a_fictional_stop_price(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'bot.db')
            trader=make_trader(path)
            trader.open_on_signal('R',100,'лидер',80,time.time())
            stream=PositionBookTickerStream()
            stream.set_symbols(('R',))
            stream.ingest({'s':'R','b':'100'})
            worker=PositionExitWorker(lambda:make_trader(path),stream,'unused',lambda:Mock())
            worker.start()
            try:
                stream.ingest({'s':'R','b':'96.27'})
                text=worker.messages.get(timeout=2)
                self.assertIn('-3.93%',text)
                d=json.loads(trader.connection.execute('SELECT payload FROM paper_exit_diagnostics').fetchone()[0])
                self.assertAlmostEqual(d['overshoot_percent'],3.23)
                self.assertEqual(d['source'],'bookTicker')
            finally:
                worker.close()
                trader.close()

    def test_stream_health_is_per_symbol_and_overflow_is_visible(self):
        stream=PositionBookTickerStream()
        stream.set_symbols(('A','B'))
        stream._pending=deque(maxlen=2)
        with patch('bot.streams.time.time',return_value=100):
            stream.ingest({'s':'A','b':'100'})
            stream.ingest({'s':'A','b':'99'})
            stream.ingest({'s':'A','b':'98'})
        self.assertFalse(stream.healthy(101))
        self.assertEqual(stream.stale_symbols(101),('B',))
        events,overflow=stream.drain_batch()
        self.assertTrue(overflow)
        self.assertEqual([p for _,_,p in events],[99,98])
        self.assertEqual(stream.drain_batch(),([],False))
        stream.set_symbols(('A',))
        self.assertTrue(stream.healthy(101))

    def client(self,bid=99.95,ask=100.05):
        client=Mock()
        client.get.return_value.json.return_value=dict(symbol='R',bidPrice=str(bid),askPrice=str(ask))
        return client

    def test_entry_uses_fresh_ask_and_current_time(self):
        with patch('bot.execution.time.time',return_value=200):
            at,bid,ask=fresh_entry(self.client(), 'R',100,.5,.25)
        self.assertEqual((at,bid,ask),(200,99.95,100.05))
        trader=make_trader(':memory:')
        try:
            trader.open_on_signal('R',ask,'лидер',80,at,signal_timestamp=100)
            row=trader.connection.execute('SELECT * FROM paper_positions').fetchone()
            self.assertEqual(row['opened_at'],200)
            self.assertEqual(row['signal_timestamp'],100)
            self.assertEqual(row['entry_price'],100.05)
        finally:
            trader.close()

    def test_old_ai_price_large_spread_and_malformed_quotes_are_rejected(self):
        for bid,ask in ((96.2,96.27),(104.95,105),(99.7,100.1),(100,99),(0,100),(100,float('nan'))):
            with self.subTest(bid=bid,ask=ask), self.assertRaises(ValueError):
                fresh_entry(self.client(bid,ask),'R',100,.5,.25)
        client=self.client()
        client.get.return_value.json.return_value['symbol']='OTHER'
        with self.assertRaises(ValueError):
            fetch_quote(client,'R')

    def test_slow_quote_response_is_rejected(self):
        with patch('bot.execution.time.monotonic',side_effect=[10,13]), self.assertRaises(ValueError):
            fresh_entry(self.client(),'R',100,.5,.25)

    def test_stale_stream_uses_fresh_rest_bid_to_close(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'bot.db')
            trader=make_trader(path)
            trader.open_on_signal('R',100,'лидер',80,time.time())
            stream=PositionBookTickerStream()
            worker=PositionExitWorker(lambda:make_trader(path),stream,'unused',
                                      lambda:self.client(99.4,99.5))
            worker.start()
            try:
                text=worker.messages.get(timeout=2)
                self.assertIn('REST bookTicker',text)
                self.assertEqual(trader.open_symbols(),())
                self.assertAlmostEqual(trader.connection.execute(
                    'SELECT realized_pnl_usdt FROM paper_positions').fetchone()[0],-.4)
            finally:
                worker.close()
                trader.close()

    def test_main_entry_pipeline_uses_fresh_quote_and_links_original_signal(self):
        from types import SimpleNamespace
        from bot.audit import AuditLog
        from bot.main import process_signal
        from bot.market import PumpSignal, SignalMarketContext, EntryDynamics
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'bot.db')
            audit=AuditLog(path)
            trader=make_trader(path)
            trader.exit_monitor_healthy=lambda:True
            market=Mock()
            market.client=self.client()
            market.execution_safety.return_value=(True,None,.01)
            market.leader_entry_quality.return_value=(True,None)
            market.entry_dynamics.return_value=EntryDynamics(.1,.1,.2,.3,.5,-.1,0,0,50)
            market.change_12h_percent={'R':10}
            market.rocket_shadow_sink=Mock()
            settings=SimpleNamespace(early_threshold_percent=.5,paper_take_profit_1_percent=.7,
                paper_take_profit_2_percent=1,paper_stop_loss_percent=.5,paper_min_ai_score=70,
                estimated_round_trip_cost_percent=.2,telegram_signal_alerts_enabled=False)
            analysis=SimpleNamespace(decision='BUY',score=80,verdict='ok',reason='test',risk='test')
            try:
                with patch('bot.main.analyze_momentum_with_retries',return_value=(analysis,None,1)), \
                        patch('bot.main.time.time',return_value=200), \
                        patch('bot.main.entry_probe',return_value={'fresh':True,'before_context':{'flow_buy_5s_usdt':100},'after_flow':{'buy_5s_usdt':200},'changes':{'5':.1,'10':.2},'allowed':False,'reason':'fresh impulse faded',
                              'entry_variants':{'decisions':{'A':True,'B':False,'C':False,'D':False}}}):
                    opened=process_signal(PumpSignal('R',100,3,300,'лидер'),{},100,
                        market,audit,trader,Mock(),Mock(),'owner',settings,
                        SignalMarketContext(1000,2,100,60,spread_bps=10))
                self.assertTrue(opened)
                row=trader.connection.execute('SELECT * FROM paper_positions').fetchone()
                self.assertEqual(row['entry_price'],100.05)
                self.assertEqual((row['opened_at'],row['signal_timestamp']),(200,100))
                self.assertEqual(audit.connection.execute('SELECT timestamp FROM signal_events').fetchone()[0],100)
                # A negative shadow verdict must not veto the existing trading decision.
                market.rocket_shadow_sink.assert_called_once_with(row['id'],{'fresh':True,'before_context':{'flow_buy_5s_usdt':100},'after_flow':{'buy_5s_usdt':200},'changes':{'5':.1,'10':.2},'allowed':False,'reason':'fresh impulse faded',
                    'entry_variants':{'decisions':{'A':True,'B':False,'C':False,'D':False}},
                    'entry_bid':99.95,'entry_quote_at':200})
            finally:
                trader.close()
                audit.close()

    def test_fading_buy_guard_blocks_even_ai_buy_before_position_created(self):
        from types import SimpleNamespace
        from bot.audit import AuditLog
        from bot.main import process_signal
        from bot.market import PumpSignal, SignalMarketContext, EntryDynamics
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'bot.db')
            audit=AuditLog(path)
            trader=make_trader(path)
            trader.exit_monitor_healthy=lambda:True
            market=Mock()
            market.client=self.client()
            market.execution_safety.return_value=(True,None,.01)
            market.leader_entry_quality.return_value=(True,None)
            market.entry_dynamics.return_value=EntryDynamics(.1,.1,.2,.3,.5,-.1,0,0,50)
            market.change_12h_percent={'R':10}
            market.rocket_shadow_sink=Mock()
            settings=SimpleNamespace(early_threshold_percent=.5,paper_take_profit_1_percent=.7,
                paper_take_profit_2_percent=1,paper_stop_loss_percent=.5,paper_min_ai_score=70,
                estimated_round_trip_cost_percent=.2,telegram_signal_alerts_enabled=False)
            analysis=SimpleNamespace(decision='BUY',score=80,verdict='ok',reason='test',risk='test')
            try:
                with patch('bot.main.analyze_momentum_with_retries',return_value=(analysis,None,1)), \
                        patch('bot.main.time.time',return_value=200), \
                        patch('bot.main.entry_probe',return_value={'fresh':True,'before_context':{'flow_buy_5s_usdt':6500},'after_flow':{'buy_5s_usdt':431},'changes':{'5':0,'10':-.046},'allowed':False,'reason':'fresh impulse faded',
                              'entry_variants':{'decisions':{'A':True,'B':False,'C':False,'D':False}}}):
                    opened=process_signal(PumpSignal('R',100,3,300,'лидер'),{},100,
                        market,audit,trader,Mock(),Mock(),'owner',settings,
                        SignalMarketContext(1000,2,100,60,spread_bps=10))
                self.assertFalse(opened)
                self.assertEqual(trader.connection.execute('SELECT COUNT(*) FROM paper_positions').fetchone()[0],0)
                reason=audit.connection.execute('SELECT reason FROM paper_entry_rejections ORDER BY rowid DESC LIMIT 1').fetchone()[0]
                self.assertIn('ослабление покупок',reason)
                self.assertIn('431.00',reason)
                market.rocket_shadow_sink.assert_not_called()
            finally:
                trader.close()
                audit.close()


if __name__ == '__main__':
    unittest.main()
