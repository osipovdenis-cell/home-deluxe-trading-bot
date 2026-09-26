import json
import sqlite3
import unittest
import threading
import time
from unittest.mock import Mock, patch

from bot.rocket_quote_stream import RocketQuoteStream, QuoteIngestQueue
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

    def test_membership_changes_wait_for_ack_and_preserve_newest_wanted_set(self):
        ws=Mock(); pending={}
        subscribed, ident=self.stream.sync_subscriptions(ws,set(),0,pending)
        self.assertEqual(subscribed,set())  # Sending isn't acknowledgement.
        self.assertEqual(json.loads(ws.send.call_args.args[0])['params'],['x@bookTicker','x@depth5'])
        self.stream.set_symbols(['X','Y'])
        self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(ws.send.call_count,1)
        subscribed=self.stream.subscription_reply({'id':ident,'result':None},subscribed,pending)
        subscribed,ident=self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(json.loads(ws.send.call_args.args[0])['params'],['y@bookTicker','y@depth5'])
        subscribed=self.stream.subscription_reply({'id':ident,'result':None},subscribed,pending)
        self.stream.set_symbols(['X'])
        subscribed,ident=self.stream.sync_subscriptions(ws,subscribed,ident,pending)
        self.assertEqual(subscribed,{'X','Y'})
        subscribed=self.stream.subscription_reply({'id':ident,'result':None},subscribed,pending)
        self.assertEqual(subscribed,{'X'})

    def test_lost_ack_reconciles_without_disconnecting_continuous_quotes(self):
        ws=Mock(); pending={1:dict(sent=0,method='SUBSCRIBE',symbols={'X'})}
        ident=self.stream.reconcile_timeout(ws,set(),1,pending,31)
        self.assertEqual(json.loads(ws.send.call_args.args[0]),{'method':'LIST_SUBSCRIPTIONS','id':2})
        self.stream.reconcile_timeout(ws,set(),ident,pending,32)
        self.assertEqual(ws.send.call_count,1)
        confirmed=self.stream.subscription_reply({'id':2,'result':self.stream.streams(['X'])},set(),pending)
        self.assertEqual(confirmed,{'X'}); self.assertFalse(pending)
        self.assertEqual(self.stream.drain_quotes()[2],[])
        self.assertEqual(self.stream.health()['subscription_reconciliations'],1)
        self.assertEqual(self.stream.subscription_reply({'id':1,'result':None},confirmed,pending),confirmed)

    def test_reconciliation_retries_missing_channels_and_marks_only_affected_symbol(self):
        self.stream.set_symbols(['X','Y'])
        pending={1:dict(sent=0,method='SUBSCRIBE',symbols={'Y'}),
                 2:dict(sent=31,method='LIST_SUBSCRIPTIONS',symbols=set())}
        confirmed=self.stream.subscription_reply({'id':2,'result':self.stream.streams(['X'])+['y@bookTicker']}, {'X'},pending)
        self.assertEqual(confirmed,{'X'})
        self.assertEqual([s for at,s in self.stream.drain_quotes()[2]],['Y'])
        ws=Mock(); self.stream.sync_subscriptions(ws,confirmed,2,pending)
        self.assertEqual(json.loads(ws.send.call_args.args[0])['params'],self.stream.streams(['Y']))

    def test_reconciliation_timeout_and_server_errors_are_not_ignored(self):
        pending={1:dict(sent=0,method='SUBSCRIBE',symbols={'X'}),
                 2:dict(sent=31,method='LIST_SUBSCRIPTIONS',symbols=set())}
        with self.assertRaises(TimeoutError):
            self.stream.reconcile_timeout(Mock(),set(),2,pending,62)
        with self.assertRaises(ValueError):
            self.stream.subscription_reply({'id':1,'code':2},set(),pending)
        with self.assertRaises(ValueError):
            self.stream.subscription_reply({'id':2,'result':None},set(),pending)

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

    def test_initial_subscription_is_in_url_without_pending_ack(self):
        clock=[0]
        def recv(**kw):
            raise OSError('private URL must not be exported')
        ws,connect=self.run_socket(recv,clock)
        self.assertTrue(connect.call_args.args[0].endswith('/stream?streams=x@bookTicker/x@depth5'))
        ws.send.assert_not_called()
        self.assertEqual(self.stream.health()['last_error_phase'],'receive')
        self.assertNotIn('private',json.dumps(self.stream.health()))

    def test_running_socket_recovers_lost_dynamic_ack(self):
        clock=[0]; step=[0]
        def recv(**kw):
            step[0]+=1
            if step[0]==1:
                self.stream.set_symbols(['X','Y']);clock[0]=1
                return json.dumps({'s':'X','b':'100','a':'101','u':1})
            if step[0]==2:
                clock[0]=32
                return json.dumps({'s':'X','b':'100','a':'101','u':2})
            if step[0]==3:
                return json.dumps({'id':2,'result':self.stream.streams(['X','Y'])})
            self.stream._stop.is_set.return_value=True
            return json.dumps({'s':'Y','b':'100','a':'101','u':1})
        ws,_=self.run_socket(recv,clock)
        self.assertEqual([json.loads(c.args[0])['method'] for c in ws.send.call_args_list],['SUBSCRIBE','LIST_SUBSCRIPTIONS'])
        self.assertEqual(self.stream.health()['reconnects'],0)
        self.assertEqual(self.stream.health()['confirmed_symbols'],2)

    def test_repeated_short_failures_back_off_and_keep_error_counts(self):
        clock=[0]
        def recv(**kw): raise ValueError('private response')
        self.run_socket(recv,clock,attempts=2)
        self.assertEqual([c.args[0] for c in self.stream._stop.wait.call_args_list],[1,2])
        self.assertEqual(self.stream.health()['disconnect_reasons'],{'receive:ValueError':2})
        self.assertFalse(self.stream.health()['connected'])

    def test_slow_ingest_does_not_delay_dynamic_subscription_ack(self):
        entered, release = threading.Event(), threading.Event()
        original = self.stream.ingest
        def ingest(payload, at=None):
            entered.set();release.wait(2)
            original(payload,at)
        self.stream.ingest=ingest
        clock=[0];step=[0]
        def recv(**kw):
            step[0]+=1
            if step[0]==1:
                return json.dumps({'s':'X','b':'100','a':'101','u':1})
            if step[0]==2:
                self.assertTrue(entered.wait(1))
                self.stream.set_symbols(['X','Y']);clock[0]=1
                return json.dumps({'s':'X','b':'101','a':'102','u':2})
            if step[0]==3:
                return json.dumps({'id':1,'result':None})
            self.assertEqual(self.stream.health()['subscription_replies'],1)
            self.assertEqual(self.stream.health()['confirmed_symbols'],2)
            release.set();self.stream._stop.is_set.return_value=True
            return json.dumps({'s':'Y','b':'100','a':'101','u':1})
        try:
            self.run_socket(recv,clock)
        finally:
            release.set()
        quotes,overflow,gaps=self.stream.drain_quotes()
        self.assertEqual([q[2] for q in quotes],[100,101,100])
        self.assertFalse(overflow)
        self.assertEqual(self.stream.health()['reconnects'],0)

    def test_ingest_overflow_marks_all_dropped_symbols_and_keeps_receive_times(self):
        worker=QuoteIngestQueue(self.stream,capacity=2)
        worker.put('data',{'s':'OLD','b':'1','a':'2','u':1},100)
        worker.put('data',{'s':'X','b':'1','a':'2','u':1},101)
        worker.put('data',{'s':'X','b':'3','a':'4','u':2},102)
        worker.thread.start();worker.close()
        quotes,_,gaps=self.stream.drain_quotes()
        self.assertEqual(quotes,[(102,'X',3.,4.)])
        self.assertEqual(set(gaps),{(100,'X'),(100,'OLD')})
        self.assertEqual(worker.health()['ingest_overflows'],1)
