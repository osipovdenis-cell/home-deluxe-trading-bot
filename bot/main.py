from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.telegram import TelegramClient


def main() -> None:
    settings = load_settings()
    binance = BinanceTestnetClient(
        settings.binance_api_key,
        settings.binance_api_secret,
        settings.binance_base_url,
    )
    telegram = TelegramClient(settings.telegram_bot_token)
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        can_trade = "да" if account.get("canTrade") else "нет"
        telegram.send(
            chat_id,
            "✅ Home Deluxe Trading Bot подключён к Binance Spot Testnet.\n"
            f"Тестовая торговля разрешена: {can_trade}.\n"
            "Реальные деньги не используются.",
        )
        print(f"Подключение успешно. TELEGRAM_CHAT_ID={chat_id}")
    finally:
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
