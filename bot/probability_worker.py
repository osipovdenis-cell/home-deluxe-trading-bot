"""Train and publish shadow models independently of the trading scanner."""
import threading
import time
from queue import SimpleQueue


class ProbabilityTrainingWorker:
    def __init__(self, audit_factory, interval=300):
        self.audit_factory, self.interval = audit_factory, interval
        self._stop = threading.Event()
        self._thread = None
        self.errors = SimpleQueue()

    def start(self):
        self._thread = threading.Thread(target=self.run, name='shadow-model-training', daemon=True)
        self._thread.start()

    def run(self):
        audit = None
        try:
            audit = self.audit_factory()
            while not self._stop.is_set():
                try:
                    audit.refresh_probability_model(time.time())
                except Exception as error:
                    audit.connection.rollback()
                    self.errors.put('Фоновое обучение: ' + type(error).__name__)
                self._stop.wait(self.interval)
        except Exception as error:
            self.errors.put('Запуск фонового обучения: ' + type(error).__name__)
        finally:
            if audit is not None:
                audit.close()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)
