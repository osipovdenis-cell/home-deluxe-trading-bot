import time

import httpx

from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.market import MarketMonitor
from bot.telegram import TelegramClient


def main() -> None:
    settings = load_settings()
    binance = BinanceTestnetClient(
        settings.binance_api_key,
        settings.binance_api_secret,
        settings.binance_base_url,
    )
    telegram = TelegramClient(settings.telegram_bot_token)
    market = MarketMonitor(
        settings.market_data_base_url,
        settings.watch_symbols,
        settings.pump_window_seconds,
        settings.pump_threshold_percent,
        settings.alert_cooldown_seconds,
    )
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        can_trade = "да" if account.get("canTrade") else "нет"
        telegram.send(
            chat_id,
            "✅ Home Deluxe Trading Bot запущен.\n"
            "Исполнение: Binance Spot Testnet.\n"
            f"Тестовая торговля разрешена: {can_trade}.\n"
            "Реальные деньги не используются.\n"
            f"Мониторинг: {', '.join(settings.watch_symbols)}.\n"
            f"Сигнал: рост от {settings.pump_threshold_percent:g}% "
            f"за {settings.pump_window_seconds // 60} мин.",
        )
        print(f"Мониторинг запущен. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        while True:
            try:
                prices = market.fetch_prices()
                for signal in market.update(prices):
                    telegram.send(
                        chat_id,
                        f"🚀 Резкий рост {signal.symbol}\n"
                        f"Изменение: +{signal.change_percent:.2f}% "
                        f"за {signal.window_seconds // 60} мин.\n"
                        f"Цена: {signal.price:.10g}\n"
                        "Это информационный сигнал, не команда на покупку.",
                    )
                time.sleep(settings.poll_interval_seconds)
            except httpx.HTTPError as error:
                print(f"Ошибка получения рынка: {error}", flush=True)
                time.sleep(max(settings.poll_interval_seconds, 30))
    except KeyboardInterrupt:
        print("Мониторинг остановлен.")
    finally:
        market.close()
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
