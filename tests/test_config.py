import os
import unittest
from unittest.mock import patch

from bot.config import load_settings


class SettingsTests(unittest.TestCase):
    def test_signal_notifications_are_silent_by_default(self) -> None:
        values = {
            "BINANCE_API_KEY": "key",
            "BINANCE_API_SECRET": "secret",
            "TELEGRAM_BOT_TOKEN": "token",
        }
        with patch.dict(os.environ, values, clear=True):
            self.assertFalse(load_settings().telegram_signal_alerts_enabled)

    def test_rejects_real_binance_url(self) -> None:
        values = {
            "BINANCE_API_KEY": "key",
            "BINANCE_API_SECRET": "secret",
            "BINANCE_BASE_URL": "https://api.binance.com",
            "TELEGRAM_BOT_TOKEN": "token",
        }
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaisesRegex(ValueError, "только к Binance Spot Testnet"):
                load_settings()


if __name__ == "__main__":
    unittest.main()
