import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.market import SignalMarketContext, PumpSignal
from bot.rocket_daily import DailyModel, DailyWorker, SUFFIX, report_data as daily_data
from bot.rocket_volume_shadow import capture, evaluate, report_data, report_text


def snapshot(at=100):
    context = SignalMarketContext(1000,.7,100,65,spread_bps=10)
    probe = dict(at=at, fresh=True, allowed=True, reason=None,
        trade_age=.1, quote_age=.2, changes={'5':.1,'10':.2,'20':.3,'60':.4},
        recovery_windows=dict(complete=True,windows=[dict(buy=50,sell=20)]),
        before_context=dict(flow_buy_5s_usdt=50),
        after_flow=dict(buy_5s_usdt=100,sell_5s_usdt=20,buy_60s_usdt=500,sell_60s_usdt=100,
            spread_bps=10,spread_change_bps=-1,cvd_60s_percent=20,trade_rate_acceleration=2,
            price_efficiency_per_10k=.3))
    return probe, context


def experiment(at=100):
    probe,context=snapshot(at)
    return evaluate(probe,context,100,.001,3,at)


class VolumeFeaturesTests(unittest.TestCase):
    def test_continuation_needs_both_price_and_executed_dominance(self):
        self.assertEqual(experiment()['state'],'ELIGIBLE')
        for window in ('5','20','60'):
            probe,context=snapshot();probe['changes'][window]=-.1
            self.assertEqual(evaluate(probe,context,100,.001,3,100)['state'],'NO_ENTRY')
        probe,context=snapshot();probe['after_flow']['sell_5s_usdt']=101
        self.assertFalse(evaluate(probe,context,100,.001,3,100)['hypothesis'])

    def test_other_market_guards_are_preserved(self):
        for change in ('quality','fading','12h','spread','tick'):
            with self.subTest(change=change):
                probe,context=snapshot();growth=3;tick=.001
                if change=='quality': probe.update(allowed=False,reason='спред не сокращается')
                if change=='fading':
                    probe['before_context']['flow_buy_5s_usdt']=200
                    probe['changes']['10']=-.1
                if change=='12h':growth=-1
                if change=='spread':probe['after_flow']['spread_bps']=30
                if change=='tick':tick=1
                result=evaluate(probe,context,100,tick,growth,100)
                self.assertTrue(result['hypothesis'])
                self.assertEqual(result['state'],'NO_ENTRY')

    def test_missing_stale_future_and_nonfinite_are_unknown(self):
        for change in ('stale','future','gap','missing','nan','negative_age','missing_tick'):
            with self.subTest(change=change):
                probe,context=snapshot();tick=.001
                if change=='stale':probe['at']=97
                if change=='future':probe['at']=101
                if change=='gap':probe['recovery_windows']['complete']=False
                if change=='missing':probe['changes'].pop('20')
                if change=='nan':probe['after_flow']['buy_5s_usdt']=float('nan')
                if change=='negative_age':probe['trade_age']=-1
                if change=='missing_tick':tick=None
                result=evaluate(probe,context,100,tick,3,100)
                self.assertEqual(result['state'],'UNKNOWN')
                json.dumps(result,allow_nan=False)

    def test_snapshot_capture_uses_time_after_probe_and_no_rest(self):
        probe,context=snapshot(100.01)
        market=SimpleNamespace(entry_dynamics=Mock(),tick_sizes={'X':.001},change_12h_percent={'X':3})
        with patch('bot.rocket_volume_shadow.time.time',side_effect=[100,100.02]), \
             patch('bot.rocket_volume_shadow.entry_probe',return_value=probe):
            result=capture(market,PumpSignal('X',100,3,300,'лидер'),context,90)
        self.assertEqual(result['state'],'ELIGIBLE')

    def test_diagnostic_failure_is_unknown(self):
        market=SimpleNamespace(entry_dynamics=Mock(side_effect=RuntimeError('private error')))
        result=capture(market,PumpSignal('X',100,3,300,'лидер'),snapshot()[1],90)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertNotIn('private',str(result))


class VolumePathTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.model=DailyModel(self.db)
    def tearDown(self): self.db.close()
    def seed(self,ident='x',at=100,exp=None):
        self.model.capture(dict(id=ident,symbol='X',signal_at=at-10,at=at,stop=1.,cost=.2,
            source='signal',reason='volume',opened=False,volume_experiment=exp or experiment(at)))
        self.db.commit()
    def state(self,ident='x'):
        return json.loads(self.db.execute('SELECT payload FROM episodes WHERE id=?',(ident,)).fetchone()[0])
    def test_same_fills_costs_and_trailing_and_no_double_spread(self):
        self.seed();self.model.tick(103,[(100,'X',99.9,100),(101,'X',102,102.1),(103,'X',101,101.1)])
        s=self.state();self.assertEqual(s['volume_execution']['state'],'ENTERED')
        d=report_data([s],200)['daily']
        self.assertEqual(d['selected']['wins'],1);self.assertAlmostEqual(d['selected']['pnl'],.4)
        self.assertEqual(d['baseline_pnl'],0)
    def test_stop_before_later_rally_and_frozen_cost(self):
        self.seed();self.model.tick(103,[(100,'X',100,100),(101,'X',98.9,99),(103,'X',110,111)])
        s=self.state();self.assertEqual(s['leg']['reason'],'STOP')
        self.assertAlmostEqual(report_data([s],200)['daily']['selected']['pnl'],-.65)
    def test_first_quote_rejection_does_not_retry_nicer_quote(self):
        for quote in ((100,99,100),(100,101.9,102)):
            with self.subTest(quote=quote):
                ident=str(quote);self.seed(ident)
                at,bid,ask=quote
                self.model.tick(102,[(at,'X',bid,ask),(101,'X',100,100)])
                self.assertEqual(self.state(ident)['volume_execution']['state'],'NO_ENTRY')
    def test_late_quote_and_gap_are_not_known_zero_profit(self):
        self.seed();self.model.tick(103,[(103,'X',100,100)])
        self.assertEqual(self.state()['volume_execution']['state'],'UNKNOWN')
        self.seed('y',200);self.model.tick(201,[(200,'X',100,100)],gaps=[(200.5,'X')])
        d=report_data([self.state('y')],250)['daily']
        self.assertEqual(d['selected']['incomplete'],1)
        self.assertEqual(d['selected']['closed'],0)
    def test_no_first_quote_restart_and_stale_snapshot(self):
        self.seed();self.model.tick(106,[])
        self.assertEqual(self.state()['volume_execution']['state'],'UNKNOWN')
        self.seed('y',200,experiment(197))
        self.assertEqual(self.state('y')['volume_experiment']['state'],'UNKNOWN')
    def test_mutation_after_capture_cannot_change_decision(self):
        e=experiment();self.seed(exp=e);e['state']='NO_ENTRY';e['features']['signal_price']=1000
        self.model.tick(100,[(100,'X',100,100)])
        self.assertEqual(self.state()['volume_execution']['state'],'ENTERED')
        worker=DailyWorker('unused');worker.capture('X',90,100,'volume',False,1,.2,volume_experiment=e)
        e['features']['signal_price']=2000
        self.assertEqual(worker.queue.get()['volume_experiment']['features']['signal_price'],1000)
    def test_horizon_open_is_not_a_closed_winner(self):
        self.seed();self.model.tick(3700,[(t,'X',100.3 if t>100 else 100,100.3 if t>100 else 100)
                                          for t in range(100,3701,5)])
        d=report_data([self.state()],3800)['daily']['selected']
        self.assertEqual(d['marked'],1);self.assertEqual(d['wins'],0)
        self.assertAlmostEqual(d['marked_pnl'],.05)
    def test_summary_denominators_windows_and_best_coin(self):
        self.seed();self.model.tick(103,[(100,'X',100,100),(101,'X',102,102),(103,'X',101,101)])
        s=self.state();old=deepcopy(s);old.update(at=-90000,symbol='Y')
        legacy=deepcopy(s);legacy.pop('volume_experiment')
        unknown=deepcopy(s);unknown['volume_experiment']['state']='UNKNOWN'
        unknown['volume_execution']['state']='UNKNOWN'
        data=report_data([s,old,legacy,unknown],200)
        self.assertEqual(data['daily']['candidates'],2)
        self.assertEqual(data['daily']['unknown_features'],1)
        self.assertEqual(data['daily']['selected']['count'],1)
        self.assertEqual(data['seven_days']['selected']['count'],2)
        self.assertAlmostEqual(data['seven_days']['pnl_without_best_symbol'],.4)
        self.assertEqual(data['daily']['feature_comparison']['profitable']['price_5s']['n'],1)
        self.assertLess(len(report_text(data)),4096)
    def test_report_exports_new_summary_and_restart_unknown(self):
        with tempfile.TemporaryDirectory() as root:
            path=str(Path(root)/'main.sqlite');main=sqlite3.connect(path)
            ledger=sqlite3.connect(path+SUFFIX);m=DailyModel(ledger)
            for at in (100,90100):
                m.capture(dict(id=str(at),symbol='X',signal_at=at-10,at=at,stop=1,cost=.2,
                    source='signal',opened=False,volume_experiment=experiment(at)))
            ledger.commit();DailyModel(ledger);ledger.close()
            d=daily_data(main,90300)
            self.assertEqual(len(d['episodes']),1)
            self.assertEqual(d['volume_test']['seven_days']['candidates'],2)
            self.assertEqual(d['volume_test']['daily']['execution_states']['UNKNOWN'],1)
            main.close()


class NoTradeTests(unittest.TestCase):
    def test_selected_shadow_candidate_still_rejected_before_ai_and_order(self):
        from bot.main import process_signal
        from bot.audit import AuditLog
        with tempfile.TemporaryDirectory() as root:
            audit=AuditLog(str(Path(root)/'audit.sqlite'))
            market,trader,ai=Mock(),Mock(),Mock()
            market.rocket_daily_worker=Mock()
            settings=SimpleNamespace(paper_stop_loss_percent=1,estimated_round_trip_cost_percent=.2)
            with patch('bot.main.capture_volume_shadow',return_value=experiment()), \
                 patch('bot.main.time.time',return_value=100):
                opened=process_signal(PumpSignal('X',100,3,300,'лидер'),{},90,market,audit,
                                      trader,ai,Mock(),'owner',settings,snapshot()[1])
            self.assertFalse(opened);self.assertEqual(trader.mock_calls,[]);self.assertEqual(ai.mock_calls,[])
            self.assertEqual(market.rocket_daily_worker.capture.call_args.kwargs['volume_experiment']['state'],'ELIGIBLE')
            audit.close()
