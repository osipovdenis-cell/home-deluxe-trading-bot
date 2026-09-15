"""Encrypted, on-demand-readable reports; never export credentials or plaintext files."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from bot.rocket_cards import cards
from bot.rocket_entry_variants import report_data as entry_variants_report


REPORT_PATH = Path('/var/lib/home-deluxe-bot/reports/latest.p7m')
CERT_PATH = Path(__file__).resolve().parents[1] / 'ops/report-reader-cert.pem'


class ReportCollector:
    """The report handlers use this sink without contacting Telegram."""
    def __init__(self):
        self.messages = []

    def send(self, chat_id, text):
        if not text.startswith('⏳'):
            self.messages.append(text)


def query_rows(connection, sql, parameters=()):
    cursor = connection.execute(sql, parameters)
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
        if trader.connection.execute("SELECT 1 FROM sqlite_master WHERE name='rocket_entry_waits'").fetchone():
            bundle['rocket_entry_waits'] = query_rows(trader.connection,
                'SELECT * FROM rocket_entry_waits ORDER BY id DESC LIMIT 100')
        bundle['rocket_cards']=cards(trader.connection, now, 100)
        bundle['rocket_entry_variants'] = entry_variants_report(trader.connection)
        # Explicit allow-list: no environment, raw logs, account keys or full DB dump.
        bundle['positions'] = query_rows(trader.connection, '''
            SELECT id,opened_at,closed_at,signal_timestamp,symbol,entry_price,highest_price,
                   initial_quantity,remaining_quantity,position_usdt,
                   realized_pnl_usdt,ai_score,signal_kind,status,close_reason
            FROM paper_positions ORDER BY id DESC LIMIT 100''')
        # Exact signal linkage; nearby confirmations are explicitly candidates,
        # not invented foreign keys. Export only numeric market features/decisions.
        signal_fields = '''id,timestamp,symbol,entry_price,signal_kind,change_percent,
            change_24h_percent,quote_volume_usdt,ai_score,ai_decision,
            quote_volume_5m_usdt,volume_ratio_5m,trades_5m,taker_buy_ratio_percent,
            spread_bps,bid_depth_usdt,ask_depth_usdt,order_book_imbalance_percent,
            change_15s_percent,change_30s_percent,change_60s_percent,
            change_180s_percent,change_300s_percent,pullback_from_high_percent,
            btc_change_300s_percent,market_breadth_60s_percent'''
        confirmation_fields = '''id,started_at,resolved_at,symbol,accepted,signal_kind,
            confirmation_progress_percent,confirmation_change_5s_percent,
            confirmation_change_10s_percent,volume_ratio_5m,taker_buy_ratio_percent,
            spread_bps,change_60s_percent,pullback_from_high_percent,
            large_buy_volume_15s_usdt,large_sell_volume_15s_usdt,
            large_buy_volume_60s_usdt,large_sell_volume_60s_usdt,
            trend_change_15m_percent,trend_change_60m_percent,trend_change_240m_percent,
            flow_cvd_60s_percent,flow_trade_rate_acceleration,
            flow_price_change_60s_percent,flow_price_efficiency_per_10k,
            flow_spread_bps,flow_spread_change_bps'''
        for position in bundle['positions']:
            stamp = position['signal_timestamp'] or position['opened_at']
            position['entry_signals'] = query_rows(audit.connection,
                f'SELECT {signal_fields} FROM signal_events WHERE symbol=? AND timestamp=? ORDER BY id',
                (position['symbol'], stamp))
            position['confirmation_candidates'] = query_rows(audit.connection,
                f'''SELECT {confirmation_fields} FROM confirmation_events
                    WHERE symbol=? AND resolved_at<=? AND resolved_at>=?
                    ORDER BY resolved_at DESC LIMIT 5''',
                (position['symbol'], stamp, stamp - 120))
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
