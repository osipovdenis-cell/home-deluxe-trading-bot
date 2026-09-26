import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bot.audit import AuditLog
from bot.probability import FEATURE_NAMES, ProbabilityModel
from bot.probability_worker import ProbabilityTrainingWorker


def model():
    n=len(FEATURE_NAMES)
    return ProbabilityModel(FEATURE_NAMES,(0.,)*n,(0.,)*n,(1.,)*n,(0.,)*n,0.,500,100,50.,50.,.25,())


class ProbabilityWorkerTests(unittest.TestCase):
    def test_confirmation_reads_published_model_without_training_or_future_leakage(self):
        with tempfile.TemporaryDirectory() as tmp:
            log=AuditLog(str(Path(tmp)/'test.db'))
            self.addCleanup(log.close)
            key=log.model_journal.register('mixed-legacy',100,model(),.5)
            def event(at):
                return SimpleNamespace(started_at=at,resolved_at=at+20,symbol='X',
                    trigger_price=100,resolution_price=101,accepted=True,reason='ok',signal_kind='лидер')
            with patch('bot.audit.train_probability_model',side_effect=AssertionError('entry must not train')):
                first=log.record_confirmation_event(event(101))
                earlier=log.record_confirmation_event(event(99))
            rows=log.connection.execute('SELECT id,shadow_probability_percent,shadow_model_meta FROM confirmation_events ORDER BY id').fetchall()
            self.assertEqual(rows[0][0],first);self.assertEqual(rows[0][1],50.)
            self.assertIn(key,rows[0][2])
            self.assertEqual(rows[1][0],earlier);self.assertIsNone(rows[1][1]);self.assertIsNone(rows[1][2])

    def test_background_refresh_publishes_for_another_reader(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=str(Path(tmp)/'test.db')
            writer=AuditLog(path);reader=AuditLog(path)
            self.addCleanup(writer.close);self.addCleanup(reader.close)
            self.assertIsNone(reader._current_probability_model(110))
            with patch.object(writer,'_probability_samples',return_value=[(1,{})]*500), \
                 patch('bot.audit.train_probability_model',return_value=model()) as train:
                writer.refresh_probability_model(100)
                writer.refresh_probability_model(101)
                train.assert_called_once()
            self.assertEqual(reader._current_probability_model(110).examples,500)

    def test_worker_owns_connection_and_stop_closes_it(self):
        entered,release,closed=threading.Event(),threading.Event(),threading.Event()
        owner=[]
        def factory():
            owner.append(threading.get_ident())
            def refresh(now):
                self.assertEqual(threading.get_ident(),owner[0])
                entered.set();release.wait(2)
            return SimpleNamespace(refresh_probability_model=refresh,close=closed.set,connection=Mock())
        worker=ProbabilityTrainingWorker(factory)
        self.addCleanup(worker.close);self.addCleanup(release.set)
        worker.start();self.assertTrue(entered.wait(1))
        self.assertNotEqual(owner[0],threading.get_ident())
        release.set();worker.close();self.assertTrue(closed.wait(1))
