import json
import sqlite3
import unittest
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock
from bot.rocket_timing_shadow import (TimingModel, TimingFlowStream, report_data, report_text,
    strengthened, open_recorder, apply_commands, SUFFIX, VERSION, TimingWorker)


def probe(now, bid=100, ask=100.01, growing=True, fresh=True):
    return dict(at=now,fresh=fresh,allowed=True,growth_12h=True,
        before_context={'flow_buy_5s_usdt':10,'spread_bps':10},after_flow={'buy_5s_usdt':20,'sell_5s_usdt':5,'spread_bps':10},
        changes={'5':.1,'10':.1,'15':.1,'60':.1},
        recovery_windows=dict(complete=True,quote_at=now,bid=bid,ask=ask,bid_5s=bid-.01,
            windows=[dict(buy=10 if growing else 30,sell=5),dict(buy=20,sell=5)]))


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.model=TimingModel(self.db)
        self.id=self.model.begin('X',100,99,100,.5,.2)
        self.model.approve(self.id,100)

    def state(self):
        return json.loads(self.db.execute('SELECT payload FROM rocket_timing_pairs WHERE id=?',(self.id,)).fetchone()[0])

    def tick(self, now, bid=100, growing=True, **kw):
        self.model.tick(now,{'X':[(now,bid)]},{'X':probe(now,bid,growing=growing,**kw)})

    def test_identical_when_strong_and_same_stop_costs(self):
        self.tick(100.2)
        self.tick(101,99.4)
        s=self.state()
        self.assertEqual(s['A']['status'],'CLOSED')
        self.assertEqual(s['A']['net'],s['B']['net'])
        self.assertAlmostEqual(s['A']['net'],(99.4/100.01-1)*100-.2)

    def test_delayed_entry_never_uses_earlier_low_as_entry(self):
        self.tick(100.2,growing=False)
        self.assertEqual(self.state()['B']['status'],'WAIT')
        self.model.tick(101,{'X':[(100.5,99.4),(100.8,100)]},{'X':probe(101,100,100.02)})
        s=self.state()
        self.assertEqual(s['A']['reason'],'STOP')
        self.assertEqual(s['B']['entry'],100.02)
        self.assertEqual(s['B']['entered'],101)
        self.assertEqual(s['B']['status'],'OPEN')

    def test_missing_probe_excludes_pair_not_no_entry(self):
        self.tick(100.2,fresh=False)
        self.assertTrue(all(self.state()[k]['status']=='INCOMPLETE' for k in ('A','B')))
        self.assertIn('неполных 1',report_text(self.db,now=3701))
        self.assertIn('полных допущенных пар 0',report_text(self.db,now=3701))

    def test_missing_wait_check_and_buffer_overflow(self):
        self.tick(100.2,growing=False)
        self.tick(103,growing=True)
        self.assertEqual(self.state()['B']['status'],'INCOMPLETE')
        self.model.tick(104,{}, {},overflow=True)
        self.assertEqual(self.state()['A']['status'],'INCOMPLETE')

    def test_rejected_common_gates_and_dedup(self):
        ident=self.model.begin('Y',100,99,100,.5,.2)
        self.model.decision(ident,110,'AI WAIT',False)
        d=report_data(self.db)
        row=next(s for s in d['pairs'] if s['id']==ident)
        self.assertEqual(row['A']['status'],'NO_ENTRY')
        self.assertEqual(row['B']['status'],'NO_ENTRY')
        self.assertIsNone(self.model.begin('Y',120,99,100,.5,.2))
        later=self.model.begin('Y',120,119,100,.5,.2)
        self.assertIsNotNone(later)
        self.model.approve(later,120)
        self.model.tick(120.2,{'Y':[(120.2,100)]},{later:probe(120.2)})
        self.assertEqual(next(s for i,s in self.model.states() if i==later)['A']['status'],'OPEN')
        self.assertIsNone(self.model.begin('BAD',100,99,100,-.5,.2))

    def test_trailing_whole_position_and_quote_gap(self):
        self.tick(100.2)
        self.tick(101,101.9)
        self.tick(102,101)
        self.assertEqual(self.state()['A']['reason'],'TRAIL')
        self.assertAlmostEqual(self.state()['A']['net'],(101/100.01-1)*100-.2)

    def test_wait_expiration_full_path_and_open_mark(self):
        self.tick(100.2,growing=False)
        for at in range(101,191):
            self.tick(at,growing=False)
        self.assertEqual(self.state()['B']['status'],'NO_ENTRY')
        for at in range(191,3701):
            self.model.tick(at,{'X':[(at,100)]},{})
        self.assertEqual(self.state()['A']['status'],'MARKED')
        self.assertIn('полных допущенных пар 1',report_text(self.db,now=3701))

    def test_raw_stream_keeps_intrastep_stop_and_rebound(self):
        stream=TimingFlowStream()
        stream.set_symbols(['X'])
        for i,(at,bid) in enumerate(((100.1,100),(100.2,99),(100.3,101))):
            stream.ingest({'s':'X','b':str(bid),'a':str(bid+.01),'u':i},at)
        events,overflow,gaps=stream.drain()
        self.assertFalse(overflow)
        self.assertEqual(gaps,[])
        self.assertEqual([r[2] for r in events],[100,99,101])
        self.assertEqual(stream.drain(),([],False,[]))

    def test_equal_buy_volume_does_not_count_as_strengthening(self):
        p=probe(100)
        p['recovery_windows']['windows'][0]['buy']=20
        self.assertFalse(strengthened(p))
        p['recovery_windows']['complete']=False
        self.assertIsNone(strengthened(p))

    def test_brief_disconnect_cannot_be_hidden_by_reconnect(self):
        self.tick(100.2)
        self.model.tick(101,{'X':[(100.4,100),(100.9,105)]},{},gaps=[(100.5,'X')])
        self.assertEqual(self.state()['A']['status'],'INCOMPLETE')

    def test_completed_exit_before_gap_stays_valid(self):
        self.tick(100.2)
        self.model.tick(101,{'X':[(100.4,99),(100.9,105)]},{},gaps=[(100.5,'X')])
        self.assertEqual(self.state()['A']['status'],'CLOSED')
        self.assertEqual(self.state()['A']['reason'],'STOP')

    def test_commands_are_replayable_after_rollback_without_losing_approval(self):
        self.db.commit()
        signal=SimpleNamespace(symbol='Y',price=100)
        token=('Y',100)
        commands=[('begin',(token,signal,100,100,1,.2)),('approve',(token,101,None,None))]
        jobs={}
        with patch.object(self.model,'approve',side_effect=sqlite3.OperationalError('busy')):
            with self.assertRaises(sqlite3.OperationalError):
                apply_commands(self.model,jobs,commands)
        self.db.rollback()
        self.assertEqual(jobs,{})
        self.assertIsNone(self.db.execute("SELECT id FROM rocket_timing_pairs WHERE symbol='Y'").fetchone())
        jobs=apply_commands(self.model,jobs,commands);self.db.commit()
        state=next(s for i,s in self.model.states() if i==jobs[token][0])
        self.assertEqual(state['approved'],101)
        self.assertEqual(state['A']['status'],'WAIT')

    def test_parallel_same_symbol_keeps_signal_specific_probes(self):
        other=self.model.begin('X',100.1,100,100,.5,.2)
        self.model.approve(other,100.1)
        bad=probe(100.2);bad['allowed']=False;bad['reason']='CVD'
        self.model.tick(100.2,{'X':[(100.2,100)]},{self.id:probe(100.2),other:bad})
        states=dict(self.model.states())
        self.assertEqual(states[self.id]['A']['status'],'OPEN')
        self.assertEqual(states[other]['A']['status'],'WAIT')

    def test_daily_report_excludes_old_pairs_and_counts_avoided_loss(self):
        self.tick(100.2,growing=False)
        self.tick(101,99.4,growing=False)
        for at in range(102,191): self.tick(at,99.4,growing=False)
        report=report_text(self.db,now=200)
        self.assertIn('B предотвратил убыточных A: 1',report)
        self.assertIn('Законченные сравнения: 1',report)
        self.assertIn('Законченные сравнения: 0',report_text(self.db,now=90000))
        self.assertIn('записано 1 сигналов',report_text(self.db,now=90000))

    def test_main_database_lock_does_not_block_diagnostics_or_readback(self):
        with tempfile.TemporaryDirectory() as root:
            path=str(Path(root)/'main.db');main=sqlite3.connect(path)
            main.execute('PRAGMA journal_mode=WAL')
            old=TimingModel(main);ident=old.begin('OLD',100,99,100,1,.2)
            main.execute("UPDATE rocket_timing_pairs SET version='rocket-timing-v1' WHERE id=?",(ident,))
            main.commit()
            main.execute("INSERT INTO rocket_timing_health VALUES('held_writer','1')")
            sidecar=open_recorder(path);model=TimingModel(sidecar)
            ident=model.begin('NEW',200,199,100,1,.2);model.approve(ident,200)
            model.tick(200.2,{'NEW':[(200.2,100)]},{'NEW':probe(200.2)})
            sidecar.execute("INSERT INTO rocket_timing_health VALUES('last_tick','200.2')")
            sidecar.commit()
            with patch('bot.rocket_timing_shadow.time.time',return_value=201):d=report_data(main)
            self.assertEqual(d['version'],VERSION)
            self.assertEqual(d['pairs'][0]['symbol'],'NEW')
            self.assertEqual(d['pairs'][0]['A']['status'],'OPEN')
            self.assertEqual(d['legacy']['candidates'],1)
            self.assertTrue(Path(path+SUFFIX).exists())
            self.assertEqual(main.execute('SELECT COUNT(*) FROM rocket_timing_pairs').fetchone()[0],1)
            with patch('bot.rocket_timing_shadow.time.time',return_value=212):d=report_data(main)
            self.assertEqual(d['pairs'][0]['A']['status'],'INCOMPLETE')
            self.assertEqual(model.states()[0][1]['A']['status'],'OPEN')  # Read-only report.
            main.rollback();main.close();sidecar.close()

    def test_background_worker_consumes_commands_with_main_writer_locked(self):
        with tempfile.TemporaryDirectory() as root:
            path=str(Path(root)/'main.db');main=sqlite3.connect(path)
            main.execute('CREATE TABLE trading_state(value)');main.commit()
            main.execute('INSERT INTO trading_state VALUES(1)')
            worker=TimingWorker(path,SimpleNamespace())
            worker.stream=Mock()
            worker.stream.drain.return_value=([],False,[])
            worker.stream.health.return_value={'connected':True}
            now=time.time();signal=SimpleNamespace(symbol='X',price=100);token=('X',now)
            worker.send('begin',token,signal,now,now,1,.2)
            worker.send('decision',token,now,'объём',False)
            worker.start()
            try:
                until=time.monotonic()+3
                complete=False
                while time.monotonic()<until:
                    try:
                        check=sqlite3.connect(path+SUFFIX,timeout=.05)
                        count=check.execute('SELECT COUNT(*) FROM rocket_timing_pairs WHERE finished IS NOT NULL').fetchone()[0]
                        health=dict(check.execute('SELECT key,value FROM rocket_timing_health'))
                        check.close()
                        if count==1 and health:
                            complete=True;break
                    except sqlite3.OperationalError:
                        if 'check' in locals():check.close()
                    time.sleep(.02)
                self.assertTrue(complete)
                self.assertEqual(int(health['session_errors']),0)
                self.assertEqual(int(health['queued_commands']),0)
                self.assertEqual(worker.commands.qsize(),0)
            finally:
                worker.close();main.rollback();main.close()

if __name__=='__main__':
    unittest.main()
