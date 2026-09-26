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
        s = RocketQuoteStream(); s.set_symbols(['X', 'Y']); s.require_clock = True
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
        owner = RocketQuoteStream(); owner.set_symbols(['BNBUSDT','BTCUSDT','DOGEUSDT','ETHUSDT'])
        shards = owner._shards = ShardedMarketStream(owner)
        shards.set_symbols(owner._symbols)
        btc = shards.parts[shards.index('BTCUSDT')]
        eth = shards.parts[shards.index('ETHUSDT')]
        self.assertIsNot(btc,eth)
        eth._stats['connected'] = True; eth._confirmed = {'ETHUSDT'}; eth._event_seen_at = __import__('time').time()
        owner.set_symbols(['BNBUSDT','BTCUSDT','DOGEUSDT','ETHUSDT','ARBUSDT'])
        self.assertEqual(eth._symbols, {'ETHUSDT','ARBUSDT'})
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
        parts = shards.parts[shards.index('BTCUSDT'):shards.index('BTCUSDT')+4]
        channels = [p.streams(['BTCUSDT']) for p in parts]
        self.assertEqual(sorted({c for group in channels for c in group if '@kline_' not in c}), sorted(owner.streams(['BTCUSDT'])))
        self.assertEqual(len(channels), 4)
        self.assertFalse(any(any('@aggTrade' in c for c in group) and any('@bookTicker' in c for c in group) for group in channels))
        for part in parts:
            part._stats['connected'] = True
            part._event_seen_at = __import__('time').time()
            part._confirmed = {'BTCUSDT'}
        self.assertTrue(owner.subscription_confirmed('BTCUSDT'))
        parts[0]._confirmed.clear()
        self.assertFalse(owner.subscription_confirmed('BTCUSDT'))
        self.assertEqual(owner.health()['confirmed_symbols'], 0)


class QuoteClockTests(unittest.TestCase):
    def test_quote_channel_requires_a_recent_exchange_clock(self):
        s = RocketQuoteStream(); s.set_symbols(['X']); s.require_clock = True
        quote = {'data': {'s':'X','b':'100','a':'101','u':1}}
        self.assertFalse(s.timely_message(quote,100))
        clock = {'data': {'e':'kline','s':'X','E':100000,'k':{'i':'1s'}}}
        self.assertTrue(s.timely_message(clock,100.1))
        s.ingest(clock,100.1)
        self.assertEqual(s.health()['invalid_messages'],0)
        self.assertEqual(s.drain_quotes()[0],[])
        self.assertTrue(s.timely_message(quote,101))
        self.assertFalse(s.timely_message(quote,104))
        self.assertFalse(s.timely_message(clock,105))
        self.assertTrue(s.timely_message({'data':{'e':'kline','E':106000}},106.1))


class SharedQueueGapTests(unittest.TestCase):
    def test_older_frame_from_another_channel_cannot_cross_a_gap(self):
        from bot.rocket_quote_stream import QuoteIngestQueue
        s = RocketQuoteStream(); s.set_symbols(['X'])
        queue = QuoteIngestQueue(s)
        queue.put('gap', {'X'}, 100)
        queue.put('data', {'s':'X','b':'99','a':'100','u':1}, 99)
        queue.put('data', {'s':'X','b':'101','a':'102','u':2}, 101)
        queue.thread.start(); queue.close()
        self.assertEqual(s.drain_quotes()[0], [(101,'X',101.,102.)])


class EventTimeTests(unittest.TestCase):
    def test_delayed_trade_is_historical_not_a_fresh_buy_and_does_not_leak_backwards(self):
        s = LeaderOrderFlowStream(); s.set_symbols(['X'])
        for at in range(100,162):
            s.ingest(dict(e='aggTrade',s='X',a=at,E=at*1000,p='1',q='2',m=False),at)
            s.ingest(dict(s='X',b='1',a='1.01'),at)
        before = s.snapshot('X',163)
        s.ingest(dict(e='aggTrade',s='X',a=162,E=162000,p='1',q='999',m=False),168)
        self.assertEqual(s.snapshot('X',163), before)
        self.assertEqual(s.snapshot('X',168.1).buy_5s_usdt, 0)
        self.assertFalse(s.entry_probe('X',168.1)['fresh'])
        s.ingest(dict(e='aggTrade',s='X',a=163,E=169000,p='1',q='2',m=False),169)
        s.ingest(dict(s='X',b='1',a='1.01'),169)
        self.assertEqual(s.snapshot('X',169.1).buy_5s_usdt, 2)
        self.assertTrue(s.entry_probe('X',169.1)['fresh'])
        self.assertEqual(s.health()['gaps'], 0)

    def test_delayed_transport_keeps_sequence_but_blocks_current_entry(self):
        s = RocketQuoteStream(); s.set_symbols(['X'])
        s._confirmed = {'X'}; s._stats['connected'] = True
        self.assertTrue(s.timely_message({'e':'aggTrade','E':100000},104))
        self.assertFalse(s.subscription_confirmed('X'))
        self.assertEqual(s.drain_quotes()[2], [])
        self.assertTrue(s.timely_message({'e':'aggTrade','E':105000},105.1))
        self.assertTrue(s.subscription_confirmed('X'))

    def test_structure_uses_exchange_time_and_availability_for_late_trades(self):
        from bot.rocket_structure import StructureStream
        s = StructureStream(); s.set_symbols(['X'])
        s.ingest(dict(e='aggTrade',s='X',a=1,p='1',q='1',m=False),-1)
        for second in range(300):
            s.ingest(dict(s='X',u=second+1,b='100',a='101'),second+.1)
            if second < 295:
                s.ingest(dict(e='aggTrade',s='X',a=second+2,E=second*1000+200,p='1',q='2',m=False),second+.2)
        before = s.snapshot('X',300.3)
        self.assertEqual(before['state'],'KNOWN')
        s.ingest(dict(e='aggTrade',s='X',a=297,E=295200,p='1',q='2',m=False),302.3)
        self.assertEqual(s.snapshot('X',300.3),before)
        for second in range(300,303):
            s.ingest(dict(s='X',u=second+1,b='100',a='101'),second+.1)
        current = s.snapshot('X',303)
        self.assertEqual(current['state'],'KNOWN')
        self.assertEqual(current['chart'][-1][5],0)
        self.assertEqual(next(row for row in current['chart'] if row[0]==293)[5],6)

    def test_quote_delay_marks_path_without_erasing_sequenced_flow(self):
        owner = LeaderOrderFlowStream(); owner.set_symbols(['X'])
        transport = OrderFlowTransport(owner)
        trade(owner,100,1)
        transport.delayed(['X'],101)
        self.assertEqual(len(owner._trades['X']),1)
        self.assertEqual(transport.drain_quotes()[2],[(101,'X')])
        transport.interrupted(['X'],102)
        self.assertEqual(len(owner._trades['X']),0)


class CapacityTests(unittest.TestCase):
    def test_groups_stay_bounded_and_existing_members_do_not_move(self):
        from bot.sharded_market_stream import ShardedMarketStream
        owner = RocketQuoteStream()
        symbols = [f'S{i:03d}' for i in range(20)]
        owner.set_symbols(symbols)
        shards = owner._shards = ShardedMarketStream(owner)
        shards.set_symbols(symbols)
        before = dict(shards.assignments)
        owner.set_symbols(symbols+['NEW'])
        self.assertEqual({s:shards.assignments[s] for s in symbols},before)
        self.assertTrue(all(len(p._symbols)<=3 for p in shards.parts))
        self.assertEqual(set().union(*(p._symbols for p in shards.parts)),set(symbols+['NEW']))

    def test_reconnect_attempts_respect_a_rolling_budget_and_shutdown(self):
        from bot.connection_budget import ConnectionBudget
        clock = [0.]
        budget = ConnectionBudget(limit=2,seconds=10,clock=lambda:clock[0])
        stop = Mock(); stop.is_set.return_value=False
        def wait(delay):
            clock[0] += delay
            return False
        stop.wait.side_effect=wait
        for _ in range(5):
            self.assertTrue(budget.acquire(stop))
        self.assertEqual(clock[0],20)
        self.assertLessEqual(len(budget.attempts),2)
        stop.is_set.return_value=True
        self.assertFalse(budget.acquire(stop))

class RedundantQuoteTests(unittest.TestCase):
    def build(self):
        from bot.sharded_market_stream import ShardedMarketStream
        owner = RocketQuoteStream(); owner.set_symbols(['BTCUSDT'])
        shards = owner._shards = ShardedMarketStream(owner)
        shards.set_symbols(owner._symbols)
        a,b = shards.parts
        now = __import__('time').time()
        for part in (a,b):
            part._stats['connected'] = True
            part._confirmed = {'BTCUSDT'}
            part._event_seen_at = now
            part._good_quotes['BTCUSDT'] = now
        return owner, shards, a,b,now

    def test_one_delayed_or_disconnected_quote_feed_does_not_erase_healthy_peer(self):
        owner, shards, a,b,now = self.build()
        a._event_late = True
        a.queue_delay(['BTCUSDT'],now)
        a.queue_interruption(['BTCUSDT'],now)
        a._stats['connected'] = False
        self.assertEqual(owner.drain_quotes()[2],[])
        self.assertTrue(owner.subscription_confirmed('BTCUSDT'))
        self.assertTrue(owner.health()['connected'])
        self.assertNotEqual(a._endpoint_index,b._endpoint_index)

    def test_stale_peer_cannot_hide_a_real_interruption(self):
        owner, shards, a,b,now = self.build()
        b._good_quotes['BTCUSDT'] = now-3
        a.queue_interruption(['BTCUSDT'],now)
        self.assertEqual(owner.drain_quotes()[2],[(now,'BTCUSDT')])

    def test_both_delayed_feeds_block_entry_and_mark_uncertainty(self):
        owner, shards, a,b,now = self.build()
        a._event_late = b._event_late = True
        a.queue_delay(['BTCUSDT'],now)
        self.assertFalse(owner.subscription_confirmed('BTCUSDT'))
        self.assertEqual(owner.drain_quotes()[2],[(now,'BTCUSDT')])

    def test_older_snapshot_from_backup_cannot_replace_newer_bid(self):
        owner, shards, a,b,now = self.build()
        def quote(update,bid):
            return dict(stream='btcusdt@depth5@100ms',data=dict(lastUpdateId=update,bids=[[str(bid),'1']],asks=[[str(bid+1),'1']]))
        a.ingest(quote(20,100),now)
        b.ingest(quote(19,90),now+.1)
        received,overflow,gaps=owner.drain_quotes()
        self.assertEqual(len(received),1)
        self.assertEqual(received[0][2],100)
        self.assertFalse(overflow)
