"""Encrypted, on-demand-readable reports; never export credentials or plaintext files."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time


REPORT_PATH = Path('/var/lib/home-deluxe-bot/reports/latest.p7m')
CERT_PATH = Path(__file__).resolve().parents[1] / 'ops/report-reader-cert.pem'


class ReportCollector:
    """The report handlers use this sink without contacting Telegram."""
    def __init__(self):
        self.messages = []

    def send(self, chat_id, text):
        if not text.startswith('⏳'):
            self.messages.append(text)


def query_rows(connection, sql):
    cursor = connection.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def collect_reports(audit, trader, prices, now, handler, exit_healthy=None):
    collector = ReportCollector()
    handler(['/status', '/learning'], now, prices, audit, trader, collector, None)
    observer = audit.observer_report_text(now, now - 7200)
    if observer:
        collector.send(None, observer)
    bundle = dict(schema_version=1,
                  generated_at=datetime.fromtimestamp(now, timezone.utc).isoformat(),
                  generated_at_unix=now, exit_monitor_healthy=exit_healthy,
                  reports=collector.messages, positions=[], fills=[], exit_diagnostics=[])
    if trader is not None:
        # Explicit allow-list: no environment, raw logs, account keys or full DB dump.
        bundle['positions'] = query_rows(trader.connection, '''
            SELECT id,opened_at,closed_at,symbol,entry_price,highest_price,
                   initial_quantity,remaining_quantity,position_usdt,
                   realized_pnl_usdt,ai_score,signal_kind,status,close_reason
            FROM paper_positions ORDER BY id DESC LIMIT 100''')
        bundle['fills'] = query_rows(trader.connection, '''
            SELECT id,position_id,timestamp,side,price,quantity,pnl_usdt,reason
            FROM paper_fills ORDER BY id DESC LIMIT 200''')
        exists = trader.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='paper_exit_diagnostics' AND type='table'"
        ).fetchone()
        if exists:
            rows = query_rows(trader.connection, '''
                SELECT position_id,timestamp,payload FROM paper_exit_diagnostics
                ORDER BY id DESC LIMIT 100''')
            for row in rows:
                row['payload'] = json.loads(row['payload'])
            bundle['exit_diagnostics'] = rows
    return bundle


def write_encrypted_report(bundle, path=REPORT_PATH, certificate=CERT_PATH):
    """OpenSSL CMS AES-256-GCM + RSA-OAEP; fail closed and replace atomically."""
    plain = json.dumps(bundle, ensure_ascii=False, allow_nan=False).encode('utf-8')
    result = subprocess.run(
        ['openssl', 'cms', '-encrypt', '-binary', '-aes-256-gcm',
         '-outform', 'DER', '-recip', str(certificate),
         '-keyopt', 'rsa_padding_mode:oaep'],
        input=plain, capture_output=True, timeout=30,
    )
    if result.returncode or not result.stdout:
        raise RuntimeError('Report encryption failed')  # Never log plaintext or stderr.
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.encrypted-', delete=False) as output:
            temporary = Path(output.name)
            output.write(result.stdout)
            output.flush()
            os.fsync(output.fileno())
            os.fchmod(output.fileno(), 0o644)  # Runner can read ciphertext only.
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class ReportExportWorker:
    """Independent DB connections in the factory keep reporting off the trading loop."""
    def __init__(self, collect, path=REPORT_PATH, interval=900, retry_interval=60):
        self.collect = collect
        self.path = path
        self.interval = interval
        self.retry_interval = retry_interval
        self._prices = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def set_prices(self, prices):
        with self._lock:
            self._prices = dict(prices)

    def start(self):
        self._thread = threading.Thread(target=self._run, name='encrypted-report-export', daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            delay = self.interval
            try:
                with self._lock:
                    prices = dict(self._prices)
                bundle = self.collect(prices, time.time())
                write_encrypted_report(bundle, self.path)
            except Exception as error:
                # Exceptions may contain SQL/data; only the type may reach public diagnostics.
                print('Encrypted report export failed: ' + type(error).__name__, flush=True)
                delay = self.retry_interval
            self._stop.wait(delay)

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
