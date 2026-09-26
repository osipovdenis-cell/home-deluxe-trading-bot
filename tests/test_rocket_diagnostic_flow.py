import json
import unittest
from unittest.mock import Mock

from bot.rocket_diagnostic_flow import DiagnosticFlowStream
from bot.rocket_volume_shadow import evaluate
from bot.market import SignalMarketContext
from bot.market import EntryDynamics, PumpSignal
from bot.rocket_cards import entry_probe
from types import SimpleNamespace


class DiagnosticFlowTests(unittest.TestCase):
    def setUp(self):
        self.stream=DiagnosticFlowStream();self.stream.set_symbols(['X'])

    def trade(self,at,price=100,ident=None):
        self.stream.ingest(dict(e='aggTrade',s='X',p=str(price),q='2',m=False,
                               a=int(at) if ident is None else ident),at)

    def book(self,at,bid=100,update=None):
        self.stream.ingest(dict(s='X',b=str(bid),a=str(bid+.01),u=int(at*100) if update is None else update),at)

    def depth(self,at,bid=100,update=1):
        self.stream.ingest(dict(stream='x@depth5',data=dict(lastUpdateId=update,
            bids=[[str(bid),'1']],asks=[[str(bid+.01),'1']])),at)

    def warm(self):
        for t in range(100,162):
            self.trade(t,100+(t-100)*.01)
            self.depth(t,100+(t-100)*.01,update=t)

    def test_real_depth_snapshots_fill_quiet_quote_windows_without_fake_ticks(self):
        self.warm()
        p=self.stream.entry_probe('X',161.2)
        self.assertTrue(p['fresh']);self.assertTrue(p['recovery_windows']['complete'])
        self.assertIsNotNone(p['snapshot'])
        self.assertEqual(p['feed_version'],2)
        self.assertFalse(self.stream.entry_probe('X',164)['fresh'])

    def test_live_subscription_changes_preserve_existing_history(self):
        self.warm();ws=Mock();pending={}
        current,rid=self.stream.sync_subscriptions(ws,set(),0,pending)
        current=self.stream.subscription_reply({'id':rid,'result':None},current,pending)
        self.stream.set_symbols(['Y','X'])
        current,rid=self.stream.sync_subscriptions(ws,current,rid,pending)
        sent=json.loads(ws.send.call_args.args[0])
        self.assertEqual(sent['method'],'SUBSCRIBE')
        self.assertTrue(all(s.startswith('y@') for s in sent['params']))
        self.stream.set_symbols(['X','Y'])
        self.stream.sync_subscriptions(ws,current,rid,pending)
        self.assertEqual(ws.send.call_count,2)
        self.assertTrue(self.stream.entry_probe('X',161.2)['fresh'])

    def test_old_depth_quote_and_duplicate_trade_do_not_undo_or_double_count(self):
        self.trade(100,100,ident=1);self.trade(100.1,100,ident=1)
        self.book(100,100,update=10);self.book(100.1,99,update=12)
        self.depth(100.2,105,update=11)
        self.assertEqual(len(self.stream.flow._trades['X']),1)
        self.assertEqual(self.stream.flow._quotes['X'][-1][1],99)
        self.assertEqual([r[2] for r in self.stream.drain()[0]],[100,99])

    def test_disconnect_clears_anchors_and_marks_gap_before_warming_up(self):
        self.warm();self.stream.drain()
        self.stream.interrupted(['X'],162)
        self.trade(163,102);self.book(163,102)
        p=self.stream.entry_probe('X',163.1)
        self.assertFalse(p['fresh'])
        self.assertIsNone(p['changes']['60'])
        self.assertIn('окно 60с после разрыва ещё не накоплено',p['freshness_reasons'])
        self.assertEqual(self.stream.drain()[2],[(162,'X')])

    def test_future_trade_never_changes_past_flow_snapshot(self):
        self.warm();before=self.stream.entry_probe('X',161.2)
        self.trade(162,110);self.book(162,110)
        after=self.stream.entry_probe('X',161.2)
        self.assertEqual(before['observed_flow'],after['observed_flow'])
        self.assertEqual(before['changes'],after['changes'])

    def test_stale_observations_remain_unknown_with_specific_reason(self):
        self.warm();p=self.stream.entry_probe('X',165)
        self.assertIsNotNone(p['observed_flow'])
        p['before_context']={'flow_buy_5s_usdt':100}
        result=evaluate(p,SignalMarketContext(1000,.8,100,60),100,.001,5,165)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertIsNotNone(result['features']['buy_60s_usdt'])
        self.assertIn('нет свежей сделки за 2 секунды',result['reasons'])
        json.dumps(result,allow_nan=False)

    def test_no_subscription_is_explicit_and_no_network_thread_started(self):
        p=self.stream.entry_probe('Y',100)
        self.assertFalse(p['fresh'])
        self.assertIn('нет подписки на монету',p['freshness_reasons'])
        self.assertIsNone(self.stream.flow._thread)

    def test_diagnostic_provider_does_not_replace_trading_probe(self):
        trade=Mock(return_value=dict(snapshot=None,fresh=False))
        diagnostic=Mock(return_value=dict(snapshot=None,fresh=False))
        market=SimpleNamespace(rocket_probe=trade)
        context=SignalMarketContext(1000,.8,10,60)
        dynamics=EntryDynamics(0,0,0,0,0,0,0,0,0)
        signal=PumpSignal('X',100,3,300,'лидер')
        entry_probe(market,signal,context,dynamics,100,probe_provider=diagnostic)
        trade.assert_not_called();diagnostic.assert_called_once()
        entry_probe(market,signal,context,dynamics,100)
        trade.assert_called_once();diagnostic.assert_called_once()
