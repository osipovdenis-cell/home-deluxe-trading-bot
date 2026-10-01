"""Regression cases for quiet reports, channel continuity and frozen exits."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bot.exit_policy import new_policy, policy_for, ANOMALY_LEGACY_VERSION
from bot.rocket_daily import DailyModel, DailyWorker, POLICY_VERSION
from bot.rocket_diagnostic_flow import DiagnosticFlowStream
from bot.rocket_quote_stream import QuoteIngestQueue
from bot.rocket_structure import StructureStream
from bot.rocket_recovery_shadow import new_leg, advance, RecoveryShadow
from bot.rocket_timing_shadow import TimingModel, apply_commands
from bot.sharded_market_stream import ShardedMarketStream
from bot.rocket_cards import RocketPathWorker, format_card, build_card, schema
from bot.report_export import ReportCollector
from bot.report_digest import render_digest
from bot.trading import PaperTrader
from types import SimpleNamespace


def trader(path=':memory:'):
    return PaperTrader(path, 200, 50, 4, 70, 1, .7, 1, 1.5, 1, 0, .2)


def quote(at, bid=100, update=1):
    return dict(stream='x@depth5@100ms', data=dict(lastUpdateId=update,
                bids=[[str(bid),'1']], asks=[[str(bid+.01),'1']]))


class FrozenExitTests(unittest.TestCase):
    def test_shadow_matches_trader_at_activation_floor_trail_and_stop(self):
        cases = [('аномальный лидер',[107.99,102,94,93]),
                 ('аномальный лидер',[108,107.8,110.52,109.53,109.52]),
                 ('лидер',[101,102.2,102]),
                 ('лидер',[101.47,101.01,101])]
        for kind, prices in cases:
            with self.subTest(kind=kind, prices=prices):
                t=trader(); self.addCleanup(t.close)
                t.open_on_signal('X',100,kind,80,100)
                policy=policy_for(t.connection.execute('SELECT * FROM paper_positions').fetchone())
                leg=new_leg(100,100,100)
                for at,price in enumerate(prices,101):
                    notices=t.update_positions({'X':price},at)
                    advance(leg,[(at,price)],at,3700,1,.2,policy)
                    self.assertEqual(leg['status']=='CLOSED',bool(notices))
                    if notices:
                        self.assertAlmostEqual(leg['net'],notices[0].pnl_percent)
                        self.assertEqual(leg['exited'],at)

    def test_legacy_contract_keeps_eight_percent_floor(self):
        policy=new_policy('аномальный лидер',1,.2)
        policy['version']=ANOMALY_LEGACY_VERSION
        leg=new_leg(100,100,100)
        advance(leg,[(101,108.09),(102,108)],102,3700,1,.2,policy)
        self.assertEqual(leg['status'],'CLOSED')
        self.assertAlmostEqual(leg['net'],7.8)

    def test_daily_freezes_type_and_does_not_stop_anomaly_at_one_percent(self):
        db=sqlite3.connect(':memory:'); self.addCleanup(db.close)
        model=DailyModel(db)
        event=dict(id='x',symbol='X',signal_at=99,at=100,reason='gate',opened=False,
                   stop=1.,cost=.2,source='signal',signal_kind='аномальный лидер')
        model.capture(event); event['signal_kind']='лидер'
        model.tick(102,[(100,'X',100,100),(101,'X',98,98),(102,'X',108.13,108.13)])
        self.assertEqual(model.active['x']['leg']['status'],'OPEN')
        self.assertEqual(model.active['x']['policy_version'],POLICY_VERSION)
        model.tick(103,[(103,'X',107.13,107.13)])
        saved=json.loads(db.execute('SELECT payload FROM episodes').fetchone()[0])
        self.assertAlmostEqual(saved['leg']['net'],6.93)
        self.assertEqual(saved['exit_policy']['stop_percent'],7)

    def test_timing_command_carries_signal_kind(self):
        db=sqlite3.connect(':memory:'); self.addCleanup(db.close)
        model=TimingModel(db)
        signal=SimpleNamespace(symbol='X',price=100,kind='аномальный лидер')
        apply_commands(model,{},[('begin',(('X',99),signal,99,100,1,.2))])
        state=model.states()[0][1]
        self.assertEqual(state['exit_policy']['protect_percent'],8)
        self.assertEqual(state['stop'],1)  # Existing entry-distance gate is unchanged.

    def test_recovery_reads_position_contract_not_global_stop(self):
        t=trader(); self.addCleanup(t.close)
        t.open_on_signal('X',100,'аномальный лидер',80,100)
        schema(t.connection)
        model=RecoveryShadow(t.connection,stop=1)
        model.seed(1,dict(entry_bid=100,signal_price=100,fresh=True,allowed=True,
                          recovery_windows=dict(complete=True,passed=True)))
        for at,bid in [(101,98),(102,108.13),(103,107.13)]:
            t.connection.execute('INSERT INTO rocket_bid_path VALUES(?,?,?)',('X',at,bid))
            model.tick(at)
        state=json.loads(t.connection.execute('SELECT payload FROM rocket_recovery_pairs').fetchone()[0])
        self.assertAlmostEqual(state['A']['net'],6.93)
        self.assertEqual(state['A'],state['B'])

    def test_gap_stays_unknown_under_new_rules(self):
        leg=new_leg(100,100,100)
        advance(leg,[(101,108),(110,120)],110,3700,1,.2,new_policy('аномальный лидер',1,.2))
        self.assertEqual(leg['status'],'INCOMPLETE')
        self.assertIsNone(leg['net'])


class ChannelContinuityTests(unittest.TestCase):
    def test_trade_socket_gap_preserves_quotes_but_invalidates_structure(self):
        owner=StructureStream(); owner.set_symbols(['X'])
        shards=owner._shards=ShardedMarketStream(owner); shards.set_symbols(['X'])
        part=next(p for p in shards.parts if p._family=='trades')
        owner.ingest(quote(100),100)
        owner.ingest(dict(e='aggTrade',s='X',a=1,p='100',q='1',m=False),100)
        part.queue_interruption(['X'],100.5)
        owner.ingest(quote(101,102,2),101)
        quotes,overflow,gaps=owner.drain_quotes()
        self.assertEqual([q[2] for q in quotes],[100,102])
        self.assertEqual(gaps,[])
        self.assertFalse(overflow)
        self.assertNotIn('X',owner.first_trade)
        self.assertEqual(owner.snapshot('X',101)['state'],'UNKNOWN')
        self.assertEqual(owner.health()['flow_gap_markers'],1)

    def test_shared_queue_trade_fence_does_not_drop_quote_from_other_socket(self):
        owner=DiagnosticFlowStream(); owner.set_symbols(['X'])
        queue=QuoteIngestQueue(owner)
        queue.put('gap:trades',{'X'},100)
        queue.put('data',quote(99.9),99.9)
        queue.put('data',dict(e='aggTrade',s='X',a=1,p='100',q='1',m=False),99.9)
        queue.put('data',dict(e='aggTrade',s='X',a=2,p='100',q='1',m=False),100.1)
        queue.thread.start(); queue.close()
        quotes,overflow,gaps=owner.drain_quotes()
        self.assertEqual(len(quotes),1)
        self.assertEqual(gaps,[])
        self.assertEqual(len(owner.flow._trades['X']),1)
        self.assertFalse(owner.entry_probe('X',100.2)['fresh'])
        self.assertEqual(queue.errors,0)

    def test_quote_gap_and_clock_delay_still_invalidate_price_path(self):
        stream=StructureStream(); stream.set_symbols(['X'])
        stream.channel_interrupted('quotes',['X'],100)
        stream.delayed(['X'],101)
        self.assertEqual(stream.drain_quotes()[2],[(100,'X'),(101,'X')])
        self.assertEqual(stream.health()['quote_gap_markers'],1)
        self.assertEqual(stream.health()['quote_delay_markers'],1)


class QuietReportingTests(unittest.TestCase):
    def test_legacy_daily_outcomes_are_excluded_from_new_cohort(self):
        from bot.rocket_daily import report_text
        base=dict(symbol='X',at=100,source='signal',classification='REJECTED',
                  leg=dict(status='CLOSED',net=10))
        old=dict(base)
        current=dict(base,policy_version=POLICY_VERSION,leg=dict(status='CLOSED',net=-7.2))
        data=dict(since=0,episodes=[old,current],health={},
                  actual=dict(entries=0,closed=0,profitable=0,losing=0,pnl=0,open=0))
        with patch('bot.rocket_daily.report_data',return_value=data):
            text=report_text(None,200)
        self.assertIn('Если купить отклонённые: прибыльных 0, убыточных 1',text)
        self.assertIn('закрытые -3.600 USDT',text)
        self.assertIn('Архив прежних правил: отказов 1',text)

    def test_duplicate_sections_once_but_distinct_reports_retained(self):
        collector=ReportCollector()
        for message in ['⏳ loading','totals','stops','totals','different totals']:
            collector.send(None,message)
        _,body=render_digest(dict(generated_at_unix=100,reports=collector.messages))
        self.assertEqual(collector.messages,['totals','stops','different totals'])
        self.assertIn('different totals',body.decode())

    def test_completed_cards_are_saved_without_individual_notifications(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'db')
            t=trader(path)
            t.open_on_signal('X',100,'аномальный лидер',80,100)
            t.update_positions({'X':108.13},101)
            t.update_positions({'X':107.13},102)
            schema(t.connection)
            t.connection.execute("INSERT INTO rocket_path_meta VALUES('started',0)")
            t.connection.commit(); t.close()
            stream=Mock(); stream.drain_batch.return_value=([],False)
            worker=RocketPathWorker(path,stream=stream)
            worker._stop=Mock(); worker._stop.wait.side_effect=[False,True]
            with patch('bot.rocket_cards.time.time',return_value=4000):
                worker._run()
            self.assertTrue(worker.notifications.empty())
            with sqlite3.connect(path) as db:
                card=json.loads(db.execute('SELECT payload FROM rocket_trade_cards').fetchone()[0])
            self.assertIn('60',card['windows'])
            self.assertIn('Максимум по журналу позиции до выхода: +8.13%',format_card(card))

    def test_intelligence_marks_reached_prices_instead_of_partial_sell_flags(self):
        t=trader(); self.addCleanup(t.close)
        t.open_on_signal('X',100,'аномальный лидер',80,100)
        t.update_positions({'X':108.13},101)
        t.update_positions({'X':107.13},102)
        t.connection.execute('UPDATE paper_account SET report_started_at=0')
        summary=t.build_intelligence(103)
        self.assertTrue(summary.trade_breakdown[0].first_target_hit)
        self.assertTrue(summary.trade_breakdown[0].second_target_hit)
        self.assertNotIn('50/50',summary.telegram_text())
        self.assertNotIn('Лучший вариант',summary.telegram_text())
