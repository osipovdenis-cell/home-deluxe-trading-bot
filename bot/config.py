from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    binance_api_key: str
    binance_api_secret: str
    binance_base_url: str
    telegram_bot_token: str
    telegram_chat_id: str | None
    market_data_base_url: str
    watch_symbols: tuple[str, ...]
    pump_window_seconds: int
    pump_threshold_percent: float
    poll_interval_seconds: int
    alert_cooldown_seconds: int


def _load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load_settings() -> Settings:
    _load_env_file()
    settings = Settings(
        binance_api_key=os.getenv("BINANCE_API_KEY", "").strip(),
        binance_api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
        binance_base_url=os.getenv(
            "BINANCE_BASE_URL", "https://testnet.binance.vision"
        ).rstrip("/"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip() or None,
        market_data_base_url=os.getenv(
            "MARKET_DATA_BASE_URL", "https://api.binance.com"
        ).rstrip("/"),
        watch_symbols=tuple(
            symbol.strip().upper()
            for symbol in os.getenv(
                "WATCH_SYMBOLS", "DOGEUSDT,SHIBUSDT,PEPEUSDT,BONKUSDT,FLOKIUSDT,WIFUSDT"
            ).split(",")
            if symbol.strip()
        ),
        pump_window_seconds=int(os.getenv("PUMP_WINDOW_SECONDS", "300")),
        pump_threshold_percent=float(os.getenv("PUMP_THRESHOLD_PERCENT", "3")),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "15")),
        alert_cooldown_seconds=int(os.getenv("ALERT_COOLDOWN_SECONDS", "1800")),
    )
    missing = [
        name
        for name, value in (
            ("BINANCE_API_KEY", settings.binance_api_key),
            ("BINANCE_API_SECRET", settings.binance_api_secret),
            ("TELEGRAM_BOT_TOKEN", settings.telegram_bot_token),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Не заполнены переменные: {', '.join(missing)}")
    if settings.binance_base_url != "https://testnet.binance.vision":
        raise ValueError("Первая версия разрешает подключение только к Binance Spot Testnet")
    if settings.market_data_base_url != "https://api.binance.com":
        raise ValueError("Рыночные данные разрешены только с публичного Binance Spot API")
    if not settings.watch_symbols:
        raise ValueError("WATCH_SYMBOLS не может быть пустым")
    if min(
        settings.pump_window_seconds,
        settings.poll_interval_seconds,
        settings.alert_cooldown_seconds,
    ) <= 0 or settings.pump_threshold_percent <= 0:
        raise ValueError("Параметры мониторинга должны быть больше нуля")
    return settings
