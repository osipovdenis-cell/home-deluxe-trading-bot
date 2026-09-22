import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from bot.rocket_structure import StructureStream, VERSION, describe, report_text
from bot.rocket_daily import DailyModel, SUFFIX, report_text as daily_report


def chart():
    prices = [100+i/6 for i in range(54)] + [110,105,108,106,109,110]
    return [[i*5,p,p,p,p,20.,50. if i==55 else 5.] for i,p in enumerate(prices)]


class StructureTests(unittest.TestCase):
    def test_ordered_higher_low_reclaim_and_selling_support(self):
        s=describe(chart(),300)
        self.assertTrue(s['pattern'])
        self.assertTrue(s['supported'])
        self.assertEqual(s['lows_at'],[275,285])
        self.assertAlmostEqual(s['features']['higher_low_percent'],(106/105-1)*100)
        self.assertLess(s['features']['pullback_percent'],0)
        c=chart();c[57][1:5]=[104]*4
        self.assertFalse(describe(c,300)['pattern'])
        c=chart();c[59][3]=103  # Unconfirmed final wick broke the higher low.
        self.assertFalse(describe(c,300)['pattern'])
        c=chart();c[57][6]=100
        self.assertTrue(describe(c,300)['pattern'])
        self.assertFalse(describe(c,300)['supported'])

    def test_no_rise_or_no_confirmed_lows_is_not_a_setup(self):
        c=chart()
        for row in c[:55]:row[1:5]=[110]*4
        self.assertFalse(describe(c,300)['pattern'])
        c=[[i*5,100+i,100+i,100+i,100+i,20.,1.] for i in range(60)]
        self.assertFalse(describe(c,300)['pattern'])

    def stream(self, omit_quote=None):
        s=StructureStream();s.set_symbols(['TEST'])
        s.ingest(dict(e='aggTrade',s='TEST',a=1,p='1',q='1',m=False),-1)
        for second in range(300):
            price=chart()[second//5][4]
            if second!=omit_quote:
                s.ingest(dict(s='TEST',u=second+1,b=str(price),a=str(price+.1)),second+.1)
            s.ingest(dict(e='aggTrade',s='TEST',a=second+2,p='1',q='2',m=False),second+.2)
        return s

    def test_snapshot_excludes_future_and_current_second_even_if_queued(self):
        s=self.stream()
        before=s.snapshot('TEST',300.3)
        self.assertEqual(before['state'],'KNOWN')
        s.ingest(dict(s='TEST',u=400,b='20',a='21'),300.2)
        s.ingest(dict(e='aggTrade',s='TEST',a=302,p='1',q='999',m=True),300.25)
        s.ingest(dict(s='TEST',u=401,b='10',a='11'),301.2)
        self.assertEqual(s.snapshot('TEST',300.3),before)
        self.assertEqual(len(before['chart']),60)
        self.assertTrue(all(row[0]+5<=300.3 for row in before['chart']))
        self.assertEqual(sum(row[5] for row in before['chart']),600)

    def test_warmup_disconnect_trade_loss_and_stale_quote_are_unknown(self):
        s=self.stream()
        self.assertEqual(s.snapshot('TEST',200)['state'],'UNKNOWN')
        self.assertEqual(s.snapshot('TEST',304)['state'],'UNKNOWN')
        s.interrupted(['TEST'],300)
        self.assertEqual(s.snapshot('TEST',300)['state'],'UNKNOWN')
        s=self.stream()
        s.ingest(dict(e='aggTrade',s='TEST',a=999,p='1',q='1',m=False),300)
        self.assertEqual(s.snapshot('TEST',300)['state'],'UNKNOWN')
        s=self.stream();s.set_symbols([]);s.set_symbols(['TEST'])
        self.assertEqual(s.snapshot('TEST',300)['state'],'UNKNOWN')

    def test_duplicate_trade_not_counted_and_buffer_is_bounded(self):
        s=self.stream()
        s.ingest(dict(e='aggTrade',s='TEST',a=301,p='1',q='999',m=False),299.4)
        self.assertEqual(sum(row[5] for row in s.snapshot('TEST',300)['chart']),600)
        for t in range(300,1500):s.on_quote(t,'TEST',100,101,t)
        self.assertLessEqual(len(s.bars['TEST']),610)

    def test_report_uses_same_closed_rejections_and_stays_one_message(self):
        with tempfile.TemporaryDirectory() as root:
            main=sqlite3.connect(str(Path(root)/'audit.db'))
            ledger=sqlite3.connect(str(Path(root)/'audit.db')+SUFFIX)
            model=DailyModel(ledger)
            snap=describe(chart(),300)
            for ident,net,opened,status in [('win',.8,False,'CLOSED'),('loss',-1.2,False,'CLOSED'),
                                           ('bought',99,True,'CLOSED'),('gap',None,False,'INCOMPLETE')]:
                model.capture(dict(id=ident,symbol='TEST',signal_at=300,at=300,stop=1.,cost=.2,
                                   source='signal',reason='volume',opened=opened,structure=snap))
                row=model.active[ident];row['leg']=dict(status=status,net=net)
                model.save(ident,row)
            ledger.commit()
            text=daily_report(main,1000)
            self.assertIn('исходов 2; неполных путей 1',text)
            self.assertIn('плюс/минус 1/1; -0.200 USDT',text)
            self.assertEqual(text.count('Структура до входа'),1)
            # Telegram limit is UTF-16 code units, not Python code points.
            self.assertLess(len(text.encode('utf-16-le'))//2,3900)
            self.assertNotIn('49.5',text)
            main.close();ledger.close()

    def test_legacy_and_unknown_not_treated_as_negative(self):
        rows=[dict(structure=dict(version=VERSION,state='UNKNOWN'),classification='REJECTED',leg=dict(status='CLOSED',net=1)),{}]
        text=report_text(rows)
        self.assertIn('без истории 1',text)
        self.assertIn('исходов 0',text)


if __name__=='__main__':unittest.main()
