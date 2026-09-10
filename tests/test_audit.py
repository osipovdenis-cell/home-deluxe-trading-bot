import tempfile
import unittest
from pathlib import Path

from bot.audit import AuditLog, detect_pumps


class AuditTests(unittest.TestCase):
    def test_detects_and_groups_pumps(self) -> None:
        candles = [
            (0, 100, 100),
            (60, 100, 101),
            (120, 100, 104),
            (180, 103, 105),
            (2040, 100, 100),
            (2100, 100, 104),
        ]
        self.assertEqual(detect_pumps(candles, 300, 3, 1800), [120, 2100])

    def test_builds_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                log._set_metadata("period_started_at", "0")
                log.connection.commit()
                log.record_prices({"DOGEUSDT": 1.0}, 60)
                log.record_alert("DOGEUSDT", True, 120)
                summary = log.build_summary(
                    300, ("DOGEUSDT",), {"DOGEUSDT": [120]}, 60, 3
                )
                self.assertEqual(summary.expected_pumps, 1)
                self.assertEqual(summary.delivered_alerts, 1)
                self.assertEqual(summary.missed_pumps, 0)
            finally:
                log.close()


if __name__ == "__main__":
    unittest.main()
