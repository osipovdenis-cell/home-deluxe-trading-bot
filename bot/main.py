import time

import httpx

from bot.audit import AuditLog, detect_pumps
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
    audit = AuditLog(settings.audit_db_path)
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
            f"за {settings.pump_window_seconds // 60} мин.\n"
            "Суточный аудит: включён.",
        )
        print(f"Мониторинг запущен. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        while True:
            try:
                now = time.time()
                prices = market.fetch_prices()
                audit.record_prices(prices, now)
                for signal in market.update(prices, now=now):
                    try:
                        telegram.send(
                            chat_id,
                            f"🚀 Резкий рост {signal.symbol}\n"
                            f"Изменение: +{signal.change_percent:.2f}% "
                            f"за {signal.window_seconds // 60} мин.\n"
                            f"Цена: {signal.price:.10g}\n"
                            "Это информационный сигнал, не команда на покупку.",
                        )
                        audit.record_alert(signal.symbol, True, now)
                    except httpx.HTTPError as error:
                        audit.record_alert(signal.symbol, False, now, str(error))
                        print(f"Ошибка отправки сигнала: {error}", flush=True)
                if audit.report_due(now):
                    started = audit.period_started_at()
                    events = {}
                    for symbol in settings.watch_symbols:
                        candles = market.fetch_minute_candles(
                            symbol,
                            started - settings.pump_window_seconds,
                            now,
                        )
                        detected = detect_pumps(
                            candles,
                            settings.pump_window_seconds,
                            settings.pump_threshold_percent,
                            settings.alert_cooldown_seconds,
                        )
                        events[symbol] = [event for event in detected if event >= started]
                    summary = audit.build_summary(
                        now,
                        settings.watch_symbols,
                        events,
                        settings.poll_interval_seconds,
                        settings.pump_threshold_percent,
                    )
                    telegram.send(chat_id, summary.telegram_text())
                    audit.finish_period(now)
                    print("Суточный аудит отправлен в Telegram.", flush=True)
                time.sleep(settings.poll_interval_seconds)
            except httpx.HTTPError as error:
                audit.record_error(str(error))
                print(f"Ошибка получения рынка: {error}", flush=True)
                time.sleep(max(settings.poll_interval_seconds, 30))
    except KeyboardInterrupt:
        print("Мониторинг остановлен.")
    finally:
        audit.close()
        market.close()
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
