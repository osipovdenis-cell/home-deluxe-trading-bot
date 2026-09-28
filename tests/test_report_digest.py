import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from bot.report_digest import DigestSender, INTERVAL, render_digest
from bot.telegram import TelegramClient

class DigestTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.db=str(Path(self.temp.name)/'audit.db');self.now=100000.
        self.send=Mock();self.clock=lambda:self.now
        self.bundle={'generated_at_unix':self.now,'reports':['section A','section B']}
    def sender(self,**kw):return DigestSender(self.db,self.send,clock=self.clock,**kw)
    def test_persistent_twelve_hours_and_single_send_after_downtime(self):
        self.sender()(self.bundle);self.send.assert_not_called()
        self.now+=INTERVAL-1;self.sender()(self.bundle);self.send.assert_not_called()
        self.now+=1;self.sender()(self.bundle);self.send.assert_called_once()
        self.sender()(self.bundle);self.send.assert_called_once()
        self.now+=INTERVAL*3;self.sender()(self.bundle);self.assertEqual(self.send.call_count,2)
    def test_failure_retries_without_advancing_success(self):
        worker=self.sender();worker(self.bundle);self.now+=INTERVAL
        self.send.side_effect=RuntimeError('failed')
        with self.assertRaises(RuntimeError):worker(self.bundle)
        with sqlite3.connect(self.db) as db:self.assertIsNone(db.execute('SELECT last_sent_at FROM telegram_digest_schedule').fetchone()[0])
        self.send.side_effect=None;worker(self.bundle);self.assertEqual(self.send.call_count,2)
        worker(self.bundle);self.assertEqual(self.send.call_count,2)
    def test_maintenance_failure_cannot_duplicate_delivery(self):
        maintenance=Mock(side_effect=ValueError('private'));worker=self.sender(on_sent=maintenance)
        worker(self.bundle);self.now+=INTERVAL
        worker(self.bundle);worker(self.bundle);self.send.assert_called_once()
    def test_all_sections_preserved_and_short_caption(self):
        self.bundle['reports']=['🚀 длинный раздел\n'*4000,'LAST_SECTION']
        self.bundle['digest_extra_reports']=['EXTRA']
        self.bundle['notification_summary']=dict(closed=5,wins=2,losses=3,pnl=-1.23,equity=198.77)
        caption,body=render_digest(self.bundle)
        self.assertLess(len(caption.encode('utf-16-le'))//2,1024)
        for section in self.bundle['reports']+['EXTRA']:self.assertIn(section,body.decode())
        self.assertIn('-1.230',caption)
    def test_one_multipart_request_and_api_rejection(self):
        client=TelegramClient.__new__(TelegramClient);client.client=Mock()
        response=client.client.post.return_value;response.json.return_value={'ok':True}
        client.send_document('123','report.txt',b'all sections','summary')
        client.client.post.assert_called_once()
        args,kw=client.client.post.call_args
        self.assertEqual(args,('/sendDocument',));self.assertEqual(kw['data']['chat_id'],'123')
        self.assertEqual(kw['files']['document'][1],b'all sections')
        response.json.return_value={'ok':False}
        with self.assertRaises(RuntimeError):client.send_document('123','report.txt',b'x','x')
