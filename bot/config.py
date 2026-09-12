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
    telegram_signal_alerts_enabled: bool
    market_data_base_url: str
    watch_symbols: tuple[str, ...]
    scan_all_usdt: bool
    min_quote_volume_usdt: float
    pump_window_seconds: int
    early_threshold_percent: float
    pump_threshold_percent: float
    max_signals_per_cycle: int
    estimated_round_trip_cost_percent: float
    paper_trading_enabled: bool
    paper_starting_balance_usdt: float
    paper_position_usdt: float
    paper_max_open_positions: int
    paper_min_ai_score: int
    paper_stop_loss_percent: float
    paper_take_profit_1_percent: float
    paper_take_profit_2_percent: float
    paper_take_profit_3_percent: float
    paper_trailing_drawdown_percent: float
    paper_max_hold_seconds: int
    paper_stagnation_after_seconds: int
    paper_stagnation_window_seconds: int
    poll_interval_seconds: int
    alert_cooldown_seconds: int
    audit_db_path: str
    openai_api_key: str | None
    openai_model: str


def _load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "да"}


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
        telegram_signal_alerts_enabled=_env_bool(
            "TELEGRAM_SIGNAL_ALERTS_ENABLED", False
        ),
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
        scan_all_usdt=_env_bool("SCAN_ALL_USDT", True),
        min_quote_volume_usdt=float(os.getenv("MIN_QUOTE_VOLUME_USDT", "500000")),
        pump_window_seconds=int(os.getenv("PUMP_WINDOW_SECONDS", "300")),
        early_threshold_percent=float(os.getenv("EARLY_THRESHOLD_PERCENT", "1")),
        pump_threshold_percent=float(os.getenv("PUMP_THRESHOLD_PERCENT", "3")),
        max_signals_per_cycle=int(os.getenv("MAX_SIGNALS_PER_CYCLE", "5")),
        estimated_round_trip_cost_percent=float(
            os.getenv("ESTIMATED_ROUND_TRIP_COST_PERCENT", "0.2")
        ),
        paper_trading_enabled=_env_bool("PAPER_TRADING_ENABLED", True),
        paper_starting_balance_usdt=float(
            os.getenv("PAPER_STARTING_BALANCE_USDT", "200")
        ),
        paper_position_usdt=float(os.getenv("PAPER_POSITION_USDT", "50")),
        paper_max_open_positions=int(os.getenv("PAPER_MAX_OPEN_POSITIONS", "4")),
        paper_min_ai_score=int(os.getenv("PAPER_MIN_AI_SCORE", "55")),
        paper_stop_loss_percent=float(os.getenv("PAPER_STOP_LOSS_PERCENT", "1")),
        paper_take_profit_1_percent=float(
            os.getenv("PAPER_TAKE_PROFIT_1_PERCENT", "1.5")
        ),
        paper_take_profit_2_percent=float(
            os.getenv("PAPER_TAKE_PROFIT_2_PERCENT", "3")
        ),
        paper_take_profit_3_percent=float(
            os.getenv("PAPER_TAKE_PROFIT_3_PERCENT", "5")
        ),
        paper_trailing_drawdown_percent=float(
            os.getenv("PAPER_TRAILING_DRAWDOWN_PERCENT", "1")
        ),
        paper_max_hold_seconds=int(os.getenv("PAPER_MAX_HOLD_SECONDS", "0")),
        paper_stagnation_after_seconds=int(
            os.getenv("PAPER_STAGNATION_AFTER_SECONDS", "1800")
        ),
        paper_stagnation_window_seconds=int(
            os.getenv("PAPER_STAGNATION_WINDOW_SECONDS", "900")
        ),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "15")),
        alert_cooldown_seconds=int(os.getenv("ALERT_COOLDOWN_SECONDS", "1800")),
        audit_db_path=os.getenv("AUDIT_DB_PATH", "data/monitor.db").strip(),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip() or None,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.6-luna").strip(),
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
    if not settings.scan_all_usdt and not settings.watch_symbols:
        raise ValueError("WATCH_SYMBOLS не может быть пустым")
    if not settings.audit_db_path:
        raise ValueError("AUDIT_DB_PATH не может быть пустым")
    if not settings.openai_model:
        raise ValueError("OPENAI_MODEL не может быть пустым")
    if min(
        settings.pump_window_seconds,
        settings.poll_interval_seconds,
        settings.alert_cooldown_seconds,
        settings.max_signals_per_cycle,
        settings.paper_max_open_positions,
    ) <= 0 or min(
        settings.early_threshold_percent,
        settings.pump_threshold_percent,
        settings.min_quote_volume_usdt,
        settings.paper_position_usdt,
        settings.paper_starting_balance_usdt,
        settings.paper_stop_loss_percent,
        settings.paper_take_profit_1_percent,
        settings.paper_take_profit_2_percent,
        settings.paper_take_profit_3_percent,
        settings.paper_trailing_drawdown_percent,
    ) <= 0:
        raise ValueError("Параметры мониторинга должны быть больше нуля")
    if settings.early_threshold_percent > settings.pump_threshold_percent:
        raise ValueError("EARLY_THRESHOLD_PERCENT не может превышать PUMP_THRESHOLD_PERCENT")
    if settings.estimated_round_trip_cost_percent < 0:
        raise ValueError("ESTIMATED_ROUND_TRIP_COST_PERCENT не может быть отрицательным")
    if settings.paper_max_hold_seconds < 0:
        raise ValueError("PAPER_MAX_HOLD_SECONDS не может быть отрицательным")
    if min(
        settings.paper_stagnation_after_seconds,
        settings.paper_stagnation_window_seconds,
    ) <= 0:
        raise ValueError("Параметры выхода из застоя должны быть больше нуля")
    if (
        settings.paper_stagnation_window_seconds
        >= settings.paper_stagnation_after_seconds
    ):
        raise ValueError("Окно застоя должно быть короче времени до проверки")
    if not 0 <= settings.paper_min_ai_score <= 100:
        raise ValueError("PAPER_MIN_AI_SCORE должен быть в диапазоне 0–100")
    if not (
        settings.paper_take_profit_1_percent
        < settings.paper_take_profit_2_percent
        < settings.paper_take_profit_3_percent
    ):
        raise ValueError("Цели прибыли должны последовательно увеличиваться")
    if (
        settings.paper_position_usdt * settings.paper_max_open_positions
        > settings.paper_starting_balance_usdt
    ):
        raise ValueError("Общий размер тестовых позиций превышает виртуальный бюджет")
    return settings
