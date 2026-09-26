import json
import unittest
from unittest.mock import Mock, patch

from bot.streams import LeaderOrderFlowStream
from bot.order_flow_transport import OrderFlowTransport
from bot.rocket_diagnostic_flow import DiagnosticFlowStream
from bot.rocket_quote_stream import RocketQuoteStream
from bot.warm_symbols import WarmSymbols


def trade(stream, at, ident, symbol='X'):
    stream.ingest(dict(e='aggTrade', s=symbol, a=ident, p=str(100+at/1000), q='2', m=False), at)


class FlowContinuityTests(unittest.TestCase):
    def stream(self):
        s = LeaderOrderFlowStream()
        s.set_symbols(['X'])
        for t in range(100, 162):
            trade(s, t, t)
            s.ingest(dict(s='X', b='100', a='100.1', B='1', A='1'), t)
        return s

    def test_reordering_members_preserves_socket_and_history(self):
        s = self.stream()
        s.set_symbols(['X', 'Y'])
        transport = s._transport = Mock()
        s._changed.clear()
        s.set_symbols(['Y', 'X'])
        transport.set_symbols.assert_not_called()
        self.assertFalse(s._changed.is_set())
        self.assertEqual(len(s._trades['X']), 62)

    def test_membership_update_uses_existing_transport(self):
        s = self.stream()
        transport = s._transport = Mock()
        s.set_symbols(['X', 'Y'])
        transport.set_symbols.assert_called_once_with(('X', 'Y'))
        transport.close.assert_not_called()
        self.assertEqual(len(s._trades['X']), 62)

    def test_reconnect_and_missing_trade_sequence_cannot_bridge_old_history(self):
        for explicit in (True, False):
            s = self.stream()
            self.assertTrue(s.entry_probe('X', 161.2)['fresh'])
            if explicit:
                s.interrupted(['X'], 162)
            trade(s, 163, 999)
            s.ingest(dict(s='X', b='101', a='101.1'), 163)
            p = s.entry_probe('X', 163.1)
            self.assertFalse(p['fresh'])
            self.assertIsNone(p['changes']['60'])
            self.assertIn('окно 60с после разрыва ещё не накоплено', p['freshness_reasons'])
            for t in range(164, 225):
                trade(s, t, 999+t-163)
                s.ingest(dict(s='X', b='101', a='101.1'), t)
            self.assertTrue(s.entry_probe('X', 224.1)['fresh'])

    def test_duplicate_trade_and_future_trade_do_not_change_past_snapshot(self):
        s = self.stream()
        before = s.snapshot('X', 161.2)
        trade(s, 161.1, 161)
        trade(s, 162, 162)
        self.assertEqual(before, s.snapshot('X', 161.2))

    def test_fresh_data_on_unconfirmed_subscription_cannot_authorize_entry(self):
        s = self.stream()
        s._transport = Mock()
        s._transport.subscription_confirmed.return_value = False
        p = s.entry_probe('X', 161.2)
        self.assertFalse(p['fresh'])
        self.assertIsNone(p['snapshot'])

    def test_transport_uses_real_depth_heartbeats_and_explicit_gaps(self):
        s = LeaderOrderFlowStream(); s.set_symbols(['X'])
        transport = OrderFlowTransport(s); transport.set_symbols(['X'])
        for t in range(100, 162):
            trade(transport, t, t)
            transport.ingest(dict(stream='x@depth5', data=dict(lastUpdateId=t,
                bids=[['100','1']], asks=[['100.1','1']])), t)
        self.assertTrue(s.entry_probe('X', 161.2)['fresh'])
        self.assertEqual(len(transport._quotes), 0)
        transport.interrupted(['X'], 162)
        self.assertFalse(s.entry_probe('X', 162.1)['fresh'])

    def test_diagnostic_missing_trade_marks_path_and_resets_flow(self):
        s = DiagnosticFlowStream(); s.set_symbols(['X'])
        trade(s, 100, 1); trade(s, 101, 3)
        self.assertEqual(s.drain()[2], [(101, 'X')])
        self.assertEqual(len(s.flow._trades['X']), 1)


class ControlPlaneTests(unittest.TestCase):
    def test_control_timeout_keeps_confirmed_data_and_bounds_pending_requests(self):
        s = RocketQuoteStream(); s.set_symbols(['X', 'Y']); ws = Mock()
        pending = {1: dict(sent=0, method='SUBSCRIBE', symbols={'Y'}),
                   2: dict(sent=31, method='LIST_SUBSCRIPTIONS', symbols=set())}
        s._last_data_received = 62
        rid = s.reconcile_timeout(ws, {'X'}, 2, pending, 62)
        self.assertEqual(rid, 3)
        self.assertEqual(len(pending), 2)
        self.assertEqual(s.drain_quotes()[2][0][1], 'Y')
        s._last_data_received = 94
        rid = s.reconcile_timeout(ws, {'X'}, rid, pending, 94)
        self.assertEqual(rid, 4)
        self.assertEqual(len(pending), 2)
        self.assertEqual(s.health()['control_timeouts'], 2)
        confirmed = s.subscription_reply({'id': 4, 'result': s.streams(['X', 'Y'])}, {'X'}, pending)
        self.assertEqual(confirmed, {'X', 'Y'})
        self.assertFalse(pending)

    def test_missing_control_and_missing_data_still_fail_closed(self):
        s = RocketQuoteStream(); s._last_data_received = 59
        pending = {1: dict(sent=0, method='LIST_SUBSCRIPTIONS', symbols=set())}
        with self.assertRaises(TimeoutError):
            s.reconcile_timeout(Mock(), {'X'}, 1, pending, 62)

    def test_wrapped_and_string_id_replies_are_recognized(self):
        s = RocketQuoteStream()
        pending = {1: dict(sent=0, method='SUBSCRIBE', symbols={'X'})}
        message = s.control_message({'data': {'id': '1', 'result': None}})
        self.assertEqual(s.subscription_reply(message, set(), pending), {'X'})
        self.assertIsNone(s.control_message({'stream':'x@bookTicker','data':{'s':'X','u':1}}))


class WatchTests(unittest.TestCase):
    def test_retention_prewarms_but_never_displaces_active_positions(self):
        watch = WarmSymbols(3, seconds=600)
        self.assertEqual(watch.select(['A'], ['B','C'], now=0), ('A','B','C'))
        self.assertEqual(watch.select([], ['A'], now=300), ('A','B','C'))
        self.assertEqual(watch.select(['D'], ['A'], now=301), ('D','A','B'))
        self.assertEqual(watch.select([], ['A'], now=901), ('A',))



class ExchangeClockTests(unittest.TestCase):
    def test_backlogged_frames_do_not_become_fresh_on_arrival(self):
        s = RocketQuoteStream(); s.set_symbols(['X', 'Y'])
        self.assertTrue(s.timely_message({'data': {'E': 100000}}, 100.5))
        self.assertFalse(s.timely_message({'data': {'E': 101000}}, 110))
        self.assertFalse(s.timely_message({'data': {'s': 'X', 'b': '1'}}, 110.1))
        self.assertFalse(s.timely_message({'data': {'E': 102000}}, 111))
        self.assertEqual(set(s.drain_quotes()[2]), {(110, 'X'), (110, 'Y')})
        self.assertTrue(s.timely_message({'data': {'E': 111000}}, 111.1))
        self.assertEqual(s.health()['stale_data_messages'], 3)


class PartitionTests(unittest.TestCase):
    def test_symbol_changes_and_gaps_are_local_to_one_partition(self):
        from bot.sharded_market_stream import ShardedMarketStream
        owner = RocketQuoteStream(); owner.set_symbols(['BTCUSDT','ETHUSDT'])
        shards = owner._shards = ShardedMarketStream(owner)
        shards.set_symbols(owner._symbols)
        btc = shards.parts[shards.index('BTCUSDT')]
        eth = shards.parts[shards.index('ETHUSDT')]
        self.assertIsNot(btc,eth)
        eth._stats['connected'] = True; eth._confirmed = {'ETHUSDT'}
        owner.set_symbols(['BTCUSDT','ETHUSDT','ARBUSDT'])
        self.assertEqual(eth._symbols, {'ETHUSDT'})
        self.assertTrue(owner.subscription_confirmed('ETHUSDT'))
        btc.interrupted(['BTCUSDT'], 100)
        self.assertEqual(owner.drain_quotes()[2], [(100,'BTCUSDT')])
        self.assertTrue(owner.subscription_confirmed('ETHUSDT'))
        self.assertEqual(owner.health()['subscription_replies'],0)


    def test_price_trade_and_depth_channels_have_separate_sockets(self):
        from bot.sharded_market_stream import ShardedMarketStream
        owner = OrderFlowTransport(LeaderOrderFlowStream())
        owner.set_symbols(['BTCUSDT'])
        shards = owner._shards = ShardedMarketStream(owner)
        shards.set_symbols(owner._symbols)
        parts = shards.parts[shards.index('BTCUSDT'):shards.index('BTCUSDT')+3]
        channels = [p.streams(['BTCUSDT']) for p in parts]
        self.assertEqual(sorted(c for group in channels for c in group), sorted(owner.streams(['BTCUSDT'])))
        self.assertEqual(len(channels), 3)
        self.assertFalse(any(any('@aggTrade' in c for c in group) and any('@bookTicker' in c for c in group) for group in channels))
        for part in parts:
            part._stats['connected'] = True
            part._confirmed = {'BTCUSDT'}
        self.assertTrue(owner.subscription_confirmed('BTCUSDT'))
        parts[0]._confirmed.clear()
        self.assertFalse(owner.subscription_confirmed('BTCUSDT'))
        self.assertEqual(owner.health()['confirmed_symbols'], 0)
