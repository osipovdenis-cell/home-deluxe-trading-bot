import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

from bot.rocket_quote_stream import RocketQuoteStream
from bot.rocket_daily import DailyModel


class RocketQuoteTests(unittest.TestCase):
    def setUp(self):
        self.stream=RocketQuoteStream()
        self.stream.set_symbols(['X'])

    def book(self, at, bid, ask, update):
        self.stream.ingest({'s':'X','b':str(bid),'a':str(ask),'u':update},at)

    def depth(self, at, bid, ask, update):
        self.stream.ingest({'stream':'x@depth5','data':{'lastUpdateId':update,
            'bids':[[str(bid),'1']], 'asks':[[str(ask),'1']]}},at)

    def test_real_snapshots_keep_unchanged_market_observed_without_interpolation(self):
        self.book(100,100,100.01,10)
        for t in range(101,111):
            self.depth(t,100,100.01,10)
        quotes,overflow,gaps=self.stream.drain_quotes()
        self.assertEqual(len(quotes),11)
        self.assertFalse(overflow);self.assertEqual(gaps,[])
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        m=DailyModel(db)
        m.capture(dict(id='x',symbol='X',at=100,stop=1.,cost=.2))
        db.commit();m.tick(110,quotes)
        self.assertEqual(m.active['x']['leg']['status'],'OPEN')
        m.tick(116,[])
        self.assertEqual(json.loads(db.execute('SELECT payload FROM episodes').fetchone()[0])['leg']['status'],'INCOMPLETE')

    def test_outdated_snapshot_does_not_undo_book_and_intrasecond_stop_is_retained(self):
        self.book(100,100,100.1,10)
        self.book(100.1,98,98.1,12)
        self.depth(100.2,100,100.1,11)
        self.book(100.3,105,105.1,13)
        quotes,_,_=self.stream.drain_quotes()
        self.assertEqual([r[2] for r in quotes],[100,98,105])

    def test_membership_change_only_updates_subscriptions(self):
        ws=Mock();pending={}
        subscribed,ident=self.stream.sync_subscriptions(ws,set(),0,pending)
        self.assertEqual(json.loads(ws.send.call_args.args[0])['params'],['x@bookTicker','x@depth5'])
        self.book(100,100,100.1,10)
        self.stream.set_symbols(['Y','X'])
        subscribed,ident=self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(json.loads(ws.send.call_args.args[0])['params'],['y@bookTicker','y@depth5'])
        self.stream.set_symbols(['X','Y'])
        self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(ws.send.call_count,2)
        self.assertEqual(len(self.stream.drain_quotes()[0]),1)
        self.stream.set_symbols(['X'])
        self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(json.loads(ws.send.call_args.args[0])['method'],'UNSUBSCRIBE')

    def test_reconnect_gap_is_explicit_and_update_ids_reset(self):
        self.book(100,100,100.1,10)
        self.stream.interrupted(['X'],101)
        self.book(102,101,101.1,9)
        quotes,_,gaps=self.stream.drain_quotes()
        self.assertEqual(len(quotes),2);self.assertEqual(gaps,[(101,'X')])

    def test_invalid_quotes_and_overflow_are_not_silently_accepted(self):
        from collections import deque
        self.stream._quotes=deque(maxlen=1)
        self.book(100,100,100.1,10);self.book(101,101,101.1,11)
        self.assertTrue(self.stream.drain_quotes()[1])
        self.book(102,float('nan'),102,12)
        quotes,_,gaps=self.stream.drain_quotes()
        self.assertEqual(quotes,[]);self.assertEqual(gaps,[(102,'X')])

    def run_socket(self, recv, clock, attempts=1):
        ws=Mock()
        ws.recv.side_effect=recv
        cm=Mock()
        cm.__enter__=Mock(return_value=ws)
        cm.__exit__=Mock(return_value=False)
        self.stream._stop=Mock()
        self.stream._stop.is_set.return_value=False
        self.stream._stop.wait.side_effect=[False]*(attempts-1)+[True]
        with patch('bot.rocket_quote_stream.connect',return_value=cm) as connect, \
             patch('bot.rocket_quote_stream.time.monotonic',side_effect=lambda:clock[0]):
            self.stream.run()
        return ws,connect

    def test_idle_watchdog_still_reconnects_without_client_pings(self):
        clock=[0]
        answers=iter([json.dumps({'id':1,'result':None}),None])
        def recv(**kw):
            answer=next(answers)
            if answer is None:
                clock[0]=31
                raise TimeoutError()
            return answer
        _,connect=self.run_socket(recv,clock)
        self.assertIsNone(connect.call_args.kwargs['ping_interval'])
        self.assertEqual(connect.call_args.kwargs['max_queue'],256)
        self.assertEqual(self.stream.health()['last_error_phase'],'idle')
        self.assertEqual(len(self.stream.drain_quotes()[2]),1)

    def test_missing_ack_reconnects_even_with_busy_market_data(self):
        clock=[0]
        def recv(**kw):
            clock[0]=31
            return json.dumps({'s':'X','b':'100','a':'101','u':1})
        self.run_socket(recv,clock)
        self.assertEqual(self.stream.health()['last_error_phase'],'subscription_ack')
        self.assertEqual(self.stream.health()['book_quotes'],1)

    def test_queued_ack_is_read_before_deadline_check(self):
        clock=[0]
        answers=iter([json.dumps({'id':1,'result':None}),None])
        def recv(**kw):
            clock[0]=31
            answer=next(answers)
            if answer is None: raise OSError('private URL must not be exported')
            return answer
        self.run_socket(recv,clock)
        self.assertEqual(self.stream.health()['last_error_phase'],'receive')
        self.assertNotIn('private',json.dumps(self.stream.health()))

    def test_repeated_short_failures_back_off_and_keep_error_counts(self):
        clock=[0]
        def recv(**kw): raise ValueError('private response')
        self.run_socket(recv,clock,attempts=2)
        self.assertEqual([c.args[0] for c in self.stream._stop.wait.call_args_list],[1,2])
        self.assertEqual(self.stream.health()['disconnect_reasons'],{'receive:ValueError':2})
        self.assertFalse(self.stream.health()['connected'])
