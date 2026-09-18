import json
import sqlite3
import unittest
from unittest.mock import Mock

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
