import json
import sqlite3
import unittest
from bot.rocket_timing_shadow import TimingModel, TimingFlowStream, report_data, report_text, strengthened


def probe(now, bid=100, ask=100.01, growing=True, fresh=True):
    return dict(at=now,fresh=fresh,allowed=True,growth_12h=True,
        before_context={'flow_buy_5s_usdt':10},after_flow={'buy_5s_usdt':20,'sell_5s_usdt':5},
        changes={'5':.1,'10':.1},
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
        self.assertIn('неполных 1',report_text(self.db))
        self.assertIn('полных допущенных пар 0',report_text(self.db))

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
        self.assertIsNone(self.model.begin('Y',120,119,100,.5,.2))
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
        self.assertIn('полных допущенных пар 1',report_text(self.db))

    def test_raw_stream_keeps_intrastep_stop_and_rebound(self):
        stream=TimingFlowStream()
        stream.set_symbols(['X'])
        for at,bid in ((100.1,100),(100.2,99),(100.3,101)):
            stream.ingest({'s':'X','b':str(bid),'a':str(bid+.01)},at)
        events,overflow=stream.drain()
        self.assertFalse(overflow)
        self.assertEqual([r[2] for r in events],[100,99,101])
        self.assertEqual(stream.drain(),([],False))

    def test_equal_buy_volume_does_not_count_as_strengthening(self):
        p=probe(100)
        p['recovery_windows']['windows'][0]['buy']=20
        self.assertFalse(strengthened(p))
        p['recovery_windows']['complete']=False
        self.assertIsNone(strengthened(p))

if __name__=='__main__':
    unittest.main()
