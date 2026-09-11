import time

import httpx

from bot.ai import AIAnalyst, AIError
from bot.audit import AuditLog, detect_pumps
from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.market import MarketMonitor
from bot.telegram import TelegramClient
from bot.trading import PaperTrader


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
        settings.scan_all_usdt,
        settings.min_quote_volume_usdt,
        settings.early_threshold_percent,
        settings.max_signals_per_cycle,
    )
    audit = AuditLog(settings.audit_db_path)
    trader = (
        PaperTrader(
            settings.audit_db_path,
            settings.paper_starting_balance_usdt,
            settings.paper_position_usdt,
            settings.paper_max_open_positions,
            settings.paper_min_ai_score,
            settings.paper_stop_loss_percent,
            settings.paper_take_profit_1_percent,
            settings.paper_take_profit_2_percent,
            settings.paper_take_profit_3_percent,
            settings.paper_trailing_drawdown_percent,
            settings.paper_max_hold_seconds,
            settings.estimated_round_trip_cost_percent,
        )
        if settings.paper_trading_enabled
        else None
    )
    ai = (
        AIAnalyst(settings.openai_api_key, settings.openai_model)
        if settings.openai_api_key
        else None
    )
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        market.fetch_prices()
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
        monitoring_text = (
            "Мониторинг: весь Binance Spot USDT "
            f"({len(market.symbols)} активных, {market.eligible_count} прошли фильтр).\n"
            if settings.scan_all_usdt
            else f"Мониторинг: {', '.join(settings.watch_symbols)}.\n"
        )
        startup_message = (
            "✅ Home Deluxe Trading Bot запущен.\n"
            "Исполнение: Binance Spot Testnet.\n"
            f"Тестовая торговля разрешена: {can_trade}.\n"
            "Реальные деньги не используются.\n"
            f"{monitoring_text}"
            f"Ранний сигнал: рост от {settings.early_threshold_percent:g}% "
            f"за {settings.pump_window_seconds // 60} мин.\n"
            f"Сильный сигнал: от {settings.pump_threshold_percent:g}%.\n"
            "Проверка сигналов: через 15, 30 и 60 мин.\n"
            + (
                f"Тестовые сделки: включены, банк "
                f"{settings.paper_starting_balance_usdt:g} USDT, "
                f"динамическое распределение между "
                f"{settings.paper_max_open_positions} позициями, "
                f"вход от {settings.paper_min_ai_score}/100.\n"
                "Выход: 40% на +1,5%, 40% на +3%, остаток 20% на +5%.\n"
                "Время удержания позиции не ограничено.\n"
                if trader is not None
                else "Тестовые сделки: выключены.\n"
            )
            + f"ИИ-аналитик: {ai_status}.\n"
            "Суточный аудит: включён."
        )
        telegram.send(chat_id, startup_message)
        print(f"Мониторинг запущен. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        while True:
            try:
                now = time.time()
                prices = market.fetch_prices()
                audit.record_prices(prices, now)
                audit.record_due_outcomes(
                    prices,
                    now,
                    settings.estimated_round_trip_cost_percent,
                )
                if trader is not None:
                    for notice in trader.update_positions(prices, now):
                        telegram.send(
                            chat_id,
                            trader.notice_telegram_text(notice, prices, now),
                        )
                for signal in market.update(prices, now=now):
                    signal_context = None
                    try:
                        signal_context = market.fetch_signal_context(signal.symbol)
                    except (httpx.HTTPError, ValueError) as error:
                        audit.record_error(f"Signal context {signal.symbol}: {error}", now)
                        print(
                            f"Ошибка данных объёма {signal.symbol}: {error}", flush=True
                        )
                    analysis = None
                    if ai is not None:
                        try:
                            analysis = ai.analyze_momentum(
                                signal.symbol,
                                signal.price,
                                signal.change_percent,
                                signal.window_seconds // 60,
                                signal.quote_volume_usdt,
                                signal.change_24h_percent,
                                signal.kind,
                                signal_context.quote_volume_5m_usdt
                                if signal_context is not None
                                else None,
                                signal_context.volume_ratio_5m
                                if signal_context is not None
                                else None,
                                signal_context.trades_5m
                                if signal_context is not None
                                else None,
                                signal_context.taker_buy_ratio_percent
                                if signal_context is not None
                                else None,
                                signal_context.spread_bps
                                if signal_context is not None
                                else None,
                                signal_context.bid_depth_usdt
                                if signal_context is not None
                                else None,
                                signal_context.ask_depth_usdt
                                if signal_context is not None
                                else None,
                                signal_context.order_book_imbalance_percent
                                if signal_context is not None
                                else None,
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
                    context_text = (
                        f"Объём за 5 мин: "
                        f"{signal_context.quote_volume_5m_usdt:,.0f} USDT "
                        f"(x{signal_context.volume_ratio_5m:.2f} к среднему).\n"
                        f"Сделок за 5 мин: {signal_context.trades_5m:,}.\n"
                        f"Доля покупок: "
                        f"{signal_context.taker_buy_ratio_percent:.1f}%.\n"
                        + (
                            f"Спред: {signal_context.spread_bps:.2f} б.п.\n"
                            f"Глубина стакана (20 уровней): покупки "
                            f"{signal_context.bid_depth_usdt:,.0f} / продажи "
                            f"{signal_context.ask_depth_usdt:,.0f} USDT.\n"
                            f"Перевес стакана: "
                            f"{signal_context.order_book_imbalance_percent:+.1f}%.\n"
                            if signal_context.spread_bps is not None
                            and signal_context.bid_depth_usdt is not None
                            and signal_context.ask_depth_usdt is not None
                            and signal_context.order_book_imbalance_percent is not None
                            else "Стакан временно недоступен.\n"
                        )
                        if signal_context is not None
                        else "Данные объёма за 5 мин временно недоступны.\n"
                    )
                    audit.record_signal(
                        now,
                        signal.symbol,
                        signal.price,
                        signal.kind,
                        signal.change_percent,
                        signal.change_24h_percent,
                        signal.quote_volume_usdt,
                        analysis.score if analysis is not None else None,
                        analysis.verdict if analysis is not None else None,
                        signal_context.quote_volume_5m_usdt
                        if signal_context is not None
                        else None,
                        signal_context.volume_ratio_5m
                        if signal_context is not None
                        else None,
                        signal_context.trades_5m
                        if signal_context is not None
                        else None,
                        signal_context.taker_buy_ratio_percent
                        if signal_context is not None
                        else None,
                        signal_context.spread_bps
                        if signal_context is not None
                        else None,
                        signal_context.bid_depth_usdt
                        if signal_context is not None
                        else None,
                        signal_context.ask_depth_usdt
                        if signal_context is not None
                        else None,
                        signal_context.order_book_imbalance_percent
                        if signal_context is not None
                        else None,
                    )
                    trade_notice = None
                    if trader is not None:
                        trade_notice = trader.open_on_signal(
                            signal.symbol,
                            signal.price,
                            signal.kind,
                            analysis.score if analysis is not None else None,
                            now,
                        )
                    try:
                        telegram.send(
                            chat_id,
                            f"{'🚀' if signal.kind == 'сильный' else '⚡️'} "
                            f"{signal.kind.capitalize()} сигнал {signal.symbol}\n"
                            f"Изменение: +{signal.change_percent:.2f}% "
                            f"за {signal.window_seconds // 60} мин.\n"
                            f"Изменение за 24 ч: {signal.change_24h_percent:+.2f}%.\n"
                            f"Оборот за 24 ч: {signal.quote_volume_usdt:,.0f} USDT.\n"
                            f"Цена: {signal.price:.10g}\n"
                            f"{context_text}"
                            f"{ai_text}"
                            "Это информационный сигнал, не команда на покупку.",
                        )
                        audit.record_alert(signal.symbol, True, now)
                        if trade_notice is not None:
                            telegram.send(
                                chat_id,
                                trader.notice_telegram_text(
                                    trade_notice, prices, now
                                ),
                            )
                    except httpx.HTTPError as error:
                        audit.record_alert(signal.symbol, False, now, str(error))
                        print(f"Ошибка отправки сигнала: {error}", flush=True)
                if trader is not None and trader.report_due(now):
                    bank_summary = trader.summary(prices, now)
                    intelligence = trader.build_intelligence(now)
                    trading_ai_text = ""
                    if ai is not None and intelligence.closed_positions:
                        try:
                            trading_analysis = ai.analyze_performance(
                                {
                                    "report_type": "paper_trading",
                                    "bank": {
                                        "starting_balance_usdt": (
                                            bank_summary.starting_balance_usdt
                                        ),
                                        "current_equity_usdt": bank_summary.equity_usdt,
                                    },
                                    "trade_intelligence": intelligence.as_dict(),
                                }
                            )
                            trading_ai_text = (
                                "\n\n🤖 ИИ-вывод по сделкам\n"
                                f"Оценка: {trading_analysis.score}/100 "
                                f"({trading_analysis.verdict}).\n"
                                f"Вывод: {trading_analysis.reason}\n"
                                f"Риск: {trading_analysis.risk}"
                            )
                        except (httpx.HTTPError, AIError) as error:
                            audit.record_error(f"OpenAI trading audit: {error}", now)
                            print(
                                f"Ошибка ИИ-разбора сделок: {error}", flush=True
                            )
                    telegram.send(
                        chat_id,
                        bank_summary.telegram_text()
                        + "\n\n"
                        + intelligence.telegram_text()
                        + trading_ai_text,
                    )
                    trader.finish_report(prices, now)
                    print("Суточный отчёт тестовой торговли отправлен.", flush=True)
                if audit.report_due(now):
                    started = audit.period_started_at()
                    events = {}
                    monitored_symbols = tuple(sorted(market.symbols))
                    for symbol in monitored_symbols:
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
                        monitored_symbols,
                        events,
                        settings.poll_interval_seconds,
                        settings.pump_threshold_percent,
                    )
                    performance = audit.build_signal_performance(now)
                    performance_ai_text = ""
                    if ai is not None and performance.signal_count:
                        try:
                            performance_analysis = ai.analyze_performance(
                                performance.as_dict()
                            )
                            performance_ai_text = (
                                "\n\n🤖 ИИ-вывод по статистике\n"
                                f"Оценка: {performance_analysis.score}/100 "
                                f"({performance_analysis.verdict}).\n"
                                f"Вывод: {performance_analysis.reason}\n"
                                f"Риск: {performance_analysis.risk}"
                            )
                        except (httpx.HTTPError, AIError) as error:
                            audit.record_error(f"OpenAI daily audit: {error}", now)
                            print(f"Ошибка суточного ИИ-аудита: {error}", flush=True)
                    daily_message = (
                        summary.telegram_text()
                        + "\n\n"
                        + performance.telegram_text()
                        + performance_ai_text
                    )
                    telegram.send(chat_id, daily_message)
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
        if trader is not None:
            trader.close()
        market.close()
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
