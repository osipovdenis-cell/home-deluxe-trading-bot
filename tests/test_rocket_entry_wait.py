import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.rocket_entry_wait import RocketEntryWaitWorker
from bot.market import PumpSignal
from bot.trading import PaperTrader


class ShortWaitTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.trader=PaperTrader(str(Path(self.tmp.name)/'test.db'),200,50,4,70,.5,.7,1,1.5,1,0,.2,
                               ordinary_max_open_positions=0)
        self.market=SimpleNamespace(change_12h_percent={'R':1},rocket_shadow_sink=Mock())
        self.worker=RocketEntryWaitWorker(None,self.market,lambda:True,'unused')
        self.worker.prepare(self.trader)
        with patch('bot.rocket_entry_wait.time.time',return_value=100):
            self.worker.submit(PumpSignal('R',100,3,300,'лидер'),None,None,80,80)
        self.good=dict(fresh=True,allowed=True,changes={'5':.1,'10':.1,'15':.1,'60':.1},
                       before_context={'flow_buy_5s_usdt':10,'spread_bps':10},
                       after_flow={'buy_5s_usdt':20,'sell_5s_usdt':10,'spread_bps':10})

    def fading_probe(self):
        # Observed EPIC case: small 5s uptick despite fading buys and a 10s decline.
        return dict(self.good, before_context={'flow_buy_5s_usdt':6205.55853,'spread_bps':10},
                    after_flow={'buy_5s_usdt':91.41469,'sell_5s_usdt':85.31244,'spread_bps':10},
                    changes={'5':.0849076629,'10':-.0212044105,'15':.1,'60':.1})

    def test_fading_buys_defer_then_recover_without_resetting_wait(self):
        weak = self.fading_probe()
        self.assertEqual(self.tick([weak]), 0)
        self.assertEqual(self.trader.open_symbols(), ())
        self.assertEqual(self.worker.symbols(), ('R',))
        self.assertEqual(self.worker._pending['R'].queued_at, 100)
        self.assertIn('ослабление покупок', self.worker._last_block['R'])
        # Lower volume alone is not a veto once the 10s decline has stopped.
        recovered = dict(weak, changes={'5':.1,'10':0.0,'15':.1,'60':.1})
        self.assertEqual(self.tick([recovered,recovered], 102), 1)
        self.assertEqual(self.trader.open_symbols(), ('R',))
        self.assertEqual(self.worker.symbols(), ())
        row = self.trader.connection.execute('SELECT signal_timestamp,opened_at FROM paper_positions').fetchone()
        self.assertEqual(tuple(row), (80,102))

    def test_fading_buys_during_quote_request_do_not_buy(self):
        self.assertEqual(self.tick([self.good,self.fading_probe()]), 1)
        self.assertEqual(self.trader.open_symbols(), ())
        self.assertEqual(self.worker.symbols(), ('R',))
        self.assertIn('ослабление покупок', self.worker._last_block['R'])
        self.tick([], 190)
        row = self.trader.connection.execute('SELECT state,detail FROM rocket_entry_waits').fetchone()
        self.assertEqual(row['state'], 'EXPIRED')
        self.assertIn('ослабление покупок', row['detail'])

    def test_missing_baseline_or_ten_second_change_cannot_bypass_guard(self):
        for weak in (dict(self.good,before_context={}),
                     dict(self.good,changes={'5':.1})):
            with self.subTest(probe=weak):
                self.assertEqual(self.tick([weak]), 0)
                self.assertEqual(self.trader.open_symbols(), ())
                self.assertEqual(self.worker.symbols(), ('R',))

    def tearDown(self):
        self.trader.close()
        self.tmp.cleanup()

    def tick(self, probes, at=101):
        with patch('bot.rocket_entry_wait.time.time',return_value=at), \
             patch('bot.rocket_entry_wait.entry_probe',side_effect=probes), \
             patch('bot.rocket_entry_wait.fresh_entry',return_value=(at,100,100.01)) as quote:
            self.worker.step(self.trader,Mock(),at)
            return quote.call_count

    def test_expiry_reports_last_block_without_changing_buy_rules(self):
        weak = dict(self.good, after_flow={'buy_5s_usdt':5,'sell_5s_usdt':10})
        self.assertEqual(self.tick([weak]), 0)
        self.tick([],191)
        detail = self.trader.connection.execute('SELECT detail FROM rocket_entry_waits').fetchone()[0]
        self.assertIn('покупки за 5с не превышают продажи', detail)
        self.assertEqual(self.trader.open_symbols(), ())

    def test_recovers_and_buys_without_new_signal_or_ai(self):
        self.assertEqual(self.tick([dict(fresh=False)]),0)
        self.assertEqual(self.worker.symbols(),('R',))
        self.assertEqual(self.tick([self.good,self.good],102),1)
        row=self.trader.connection.execute('SELECT * FROM paper_positions').fetchone()
        self.assertEqual(row['signal_timestamp'],80)
        self.assertEqual(row['opened_at'],102)
        self.assertIn('короткого ожидания',row['signal_kind'])
        self.assertEqual(self.worker.symbols(),())
        self.assertEqual(self.tick([],103),0)
        self.assertEqual(self.trader.connection.execute('SELECT count(*) FROM paper_positions').fetchone()[0],1)

    def test_changes_during_quote_request_do_not_buy(self):
        self.tick([self.good,dict(fresh=False)])
        self.assertEqual(self.trader.open_symbols(),())
        self.assertEqual(self.worker.symbols(),('R',))

    def test_widening_spread_during_quote_waits_and_can_recover_without_ai(self):
        bad={**self.good,'after_flow':{**self.good['after_flow'],'spread_bps':11}}
        self.assertEqual(self.tick([self.good,bad]),1)
        self.assertEqual(self.trader.open_symbols(),())
        self.assertEqual(self.worker._pending['R'].queued_at,100)
        self.assertEqual(self.tick([self.good,self.good],102),1)
        self.assertEqual(self.trader.open_symbols(),('R',))
        self.assertEqual(self.worker.symbols(),())

    def test_expiry_does_not_buy(self):
        self.assertEqual(self.tick([],190),0)
        self.assertEqual(self.worker.symbols(),())
        self.assertEqual(self.trader.connection.execute('SELECT state FROM rocket_entry_waits').fetchone()[0],'EXPIRED')

    def test_price_drift_cancels_old_approval(self):
        with patch('bot.rocket_entry_wait.entry_probe',return_value=self.good), \
             patch('bot.rocket_entry_wait.fresh_entry',side_effect=ValueError('drift')):
            self.worker.step(self.trader,Mock(),101)
        self.assertEqual(self.worker.symbols(),())
        self.assertEqual(self.trader.open_symbols(),())

    def test_exit_health_and_direction_still_required(self):
        self.worker.healthy=lambda:False
        self.assertEqual(self.tick([]),0)
        self.worker.healthy=lambda:True
        self.market.change_12h_percent['R']=-1
        self.assertEqual(self.tick([]),0)
        self.assertEqual(self.worker.symbols(),())

    def test_duplicate_submission_does_not_reset_expiry(self):
        with patch('bot.rocket_entry_wait.time.time',return_value=180):
            self.worker.submit(PumpSignal('R',100,3,300,'лидер'),None,None,80,80)
        self.tick([],190)
        self.assertEqual(self.worker.symbols(),())

    def test_growth_without_buy_support_waits(self):
        probe={**self.good,'after_flow':{'buy_5s_usdt':10,'sell_5s_usdt':20}}
        self.assertEqual(self.tick([probe]),0)
        self.assertEqual(self.worker.symbols(),('R',))

    def test_concurrent_admissions_cannot_duplicate_position(self):
        barrier=threading.Barrier(2)
        errors=[]
        def run():
            t=None
            try:
                t=PaperTrader(str(Path(self.tmp.name)/'test.db'),200,50,4,70,.5,.7,1,1.5,1,0,.2,
                              ordinary_max_open_positions=0)
                barrier.wait(timeout=5)
                t.open_on_signal('R',100,'лидер',80,101,bypass_min_score=True)
            except Exception as e:
                errors.append(e)
            finally:
                if t: t.close()
        threads=[threading.Thread(target=run) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join(timeout=6)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(errors,[])
        self.assertEqual(self.trader.connection.execute('SELECT count(*) FROM paper_positions').fetchone()[0],1)

    def test_fresh_quality_rejection_blocks_before_quote_then_can_recover(self):
        bad={**self.good,'allowed':False,'reason':'частота исполненных сделок не ускоряется'}
        self.assertEqual(self.tick([bad]),0)
        self.assertEqual(self.trader.open_symbols(),())
        self.assertEqual(self.worker.symbols(),('R',))
        self.tick([self.good,self.good],102)
        self.assertEqual(self.trader.open_symbols(),('R',))

    def test_quality_deterioration_during_quote_blocks_purchase(self):
        bad={**self.good,'allowed':False,'reason':'спред лидера не сокращается'}
        self.assertEqual(self.tick([self.good,bad]),1)
        self.assertEqual(self.trader.open_symbols(),())
        self.assertEqual(self.worker.symbols(),('R',))

    def test_slow_trade_frequency_rejected_despite_price_and_buy_recovery(self):
        from bot.market import MarketMonitor, SignalMarketContext, EntryDynamics
        context=SignalMarketContext(100000,1.45,100,60,10,1000,900,5,
            flow_cvd_60s_percent=49,flow_trade_rate_acceleration=.735,
            flow_price_change_60s_percent=.74,flow_price_efficiency_per_10k=.54,
            flow_spread_change_bps=-.07)
        dynamics=EntryDynamics(.41,.4,.74,1,2,-.05,0,0,40)
        allowed,reason=MarketMonitor.leader_entry_quality(context,dynamics)
        probe={**self.good,'allowed':allowed,'reason':reason}
        self.assertFalse(allowed)
        self.assertEqual(self.tick([probe]),0)
        self.assertEqual(self.trader.open_symbols(),())
