import time

import httpx

from bot.ai import AIAnalyst, AIError
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
    ai = (
        AIAnalyst(settings.openai_api_key, settings.openai_model)
        if settings.openai_api_key
        else None
    )
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        can_trade = "да" if account.get("canTrade") else "нет"
        ai_status = "не настроен"
        if ai is not None:
            try:
                ai.check_connection()
                ai_status = f"подключён ({settings.openai_model})"
            except (httpx.HTTPError, AIError) as error:
                ai_status = "ошибка подключения"
                audit.record_error(f"OpenAI: {error}")
                print(f"Ошибка подключения OpenAI: {error}", flush=True)
        telegram.send(
            chat_id,
            "✅ Home Deluxe Trading Bot запущен.\n"
            "Исполнение: Binance Spot Testnet.\n"
            f"Тестовая торговля разрешена: {can_trade}.\n"
            "Реальные деньги не используются.\n"
            f"Мониторинг: {', '.join(settings.watch_symbols)}.\n"
            f"Сигнал: рост от {settings.pump_threshold_percent:g}% "
            f"за {settings.pump_window_seconds // 60} мин.\n"
            f"ИИ-аналитик: {ai_status}.\n"
            "Суточный аудит: включён.",
        )
        print(f"Мониторинг запущен. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        while True:
            try:
                now = time.time()
                prices = market.fetch_prices()
                audit.record_prices(prices, now)
                for signal in market.update(prices, now=now):
                    analysis = None
                    if ai is not None:
                        try:
                            analysis = ai.analyze_momentum(
                                signal.symbol,
                                signal.price,
                                signal.change_percent,
                                signal.window_seconds // 60,
                            )
                        except (httpx.HTTPError, AIError) as error:
                            audit.record_error(f"OpenAI: {error}", now)
                            print(f"Ошибка анализа OpenAI: {error}", flush=True)
                    ai_text = (
                        "\n"
                        f"ИИ-оценка: {analysis.score}/100 ({analysis.verdict}).\n"
                        f"Причина: {analysis.reason}\n"
                        f"Риск: {analysis.risk}\n"
                        if analysis is not None
                        else "\nИИ-анализ временно недоступен.\n"
                    )
                    try:
                        telegram.send(
                            chat_id,
                            f"🚀 Резкий рост {signal.symbol}\n"
                            f"Изменение: +{signal.change_percent:.2f}% "
                            f"за {signal.window_seconds // 60} мин.\n"
                            f"Цена: {signal.price:.10g}\n"
                            f"{ai_text}"
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
        if ai is not None:
            ai.close()
        audit.close()
        market.close()
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
