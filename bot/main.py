import time

import httpx

from bot.ai import AIAnalyst, AIError
from bot.audit import AuditLog, detect_pumps
from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.market import MarketMonitor
from bot.streams import AllMarketMiniTickerStream, PositionBookTickerStream
from bot.telegram import TelegramClient
from bot.trading import PaperTrader


def send_trade_notices(trader, telegram, chat_id, notices, prices, now):
    for notice in notices:
        telegram.send(chat_id, trader.notice_telegram_text(notice, prices, now))


def process_signal(
    signal, prices, now, market, audit, trader, ai, telegram, chat_id,
    send_signal_alerts,
):
    context = None
    try:
        context = market.fetch_signal_context(signal.symbol)
    except (httpx.HTTPError, ValueError) as error:
        audit.record_error(f"Signal context {signal.symbol}: {error}", now)
        print(f"Ошибка данных объёма {signal.symbol}: {error}", flush=True)
    analysis = None
    if ai is not None:
        try:
            analysis = ai.analyze_momentum(
                signal.symbol, signal.price, signal.change_percent,
                signal.window_seconds // 60, signal.quote_volume_usdt,
                signal.change_24h_percent, signal.kind,
                context.quote_volume_5m_usdt if context else None,
                context.volume_ratio_5m if context else None,
                context.trades_5m if context else None,
                context.taker_buy_ratio_percent if context else None,
                context.spread_bps if context else None,
                context.bid_depth_usdt if context else None,
                context.ask_depth_usdt if context else None,
                context.order_book_imbalance_percent if context else None,
            )
        except (httpx.HTTPError, AIError) as error:
            audit.record_error(f"OpenAI: {error}", now)
            print(f"Ошибка анализа OpenAI: {error}", flush=True)
    context_values = (
        (context.quote_volume_5m_usdt, context.volume_ratio_5m,
         context.trades_5m, context.taker_buy_ratio_percent,
         context.spread_bps, context.bid_depth_usdt, context.ask_depth_usdt,
         context.order_book_imbalance_percent)
        if context else (None,) * 8
    )
    audit.record_signal(
        now, signal.symbol, signal.price, signal.kind, signal.change_percent,
        signal.change_24h_percent, signal.quote_volume_usdt,
        analysis.score if analysis else None,
        analysis.verdict if analysis else None, *context_values,
    )
    context_text = (
        f"Объём за 5 мин: {context.quote_volume_5m_usdt:,.0f} USDT "
        f"(x{context.volume_ratio_5m:.2f} к среднему).\n"
        f"Сделок за 5 мин: {context.trades_5m:,}.\n"
        f"Доля покупок: {context.taker_buy_ratio_percent:.1f}%.\n"
        + (
            f"Спред: {context.spread_bps:.2f} б.п.\n"
            f"Глубина стакана (20 уровней): покупки "
            f"{context.bid_depth_usdt:,.0f} / продажи {context.ask_depth_usdt:,.0f} USDT.\n"
            f"Перевес стакана: {context.order_book_imbalance_percent:+.1f}%.\n"
            if context.spread_bps is not None and context.bid_depth_usdt is not None
            and context.ask_depth_usdt is not None
            and context.order_book_imbalance_percent is not None
            else "Стакан временно недоступен.\n"
        ) if context else "Данные объёма за 5 мин временно недоступны.\n"
    )
    ai_text = (
        f"\nИИ-оценка: {analysis.score}/100 ({analysis.verdict}).\n"
        f"Причина: {analysis.reason}\nРиск: {analysis.risk}\n"
        if analysis else "\nИИ-анализ временно недоступен.\n"
    )
    notice = (
        trader.open_on_signal(signal.symbol, signal.price, signal.kind,
                              analysis.score if analysis else None, now)
        if trader else None
    )
    signal_text = (
            f"{'🚀' if signal.kind == 'сильный' else '⚡️'} "
            f"{signal.kind.capitalize()} сигнал {signal.symbol}\n"
            f"Изменение: +{signal.change_percent:.2f}% за "
            f"{signal.window_seconds // 60} мин.\n"
            f"Изменение за 24 ч: {signal.change_24h_percent:+.2f}%.\n"
            f"Оборот за 24 ч: {signal.quote_volume_usdt:,.0f} USDT.\n"
            f"Цена: {signal.price:.10g}\n{context_text}{ai_text}"
            "Это информационный сигнал, не команда на покупку."
    )
    if send_signal_alerts:
        try:
            telegram.send(chat_id, signal_text)
            audit.record_alert(signal.symbol, True, now)
        except httpx.HTTPError as error:
            audit.record_alert(signal.symbol, False, now, str(error))
    else:
        # Сигнал обработан и сохранён, но пользователь выбрал тихий Telegram.
        audit.record_alert(signal.symbol, True, now)
    if notice is not None:
        try:
            telegram.send(chat_id, trader.notice_telegram_text(notice, prices, now))
        except httpx.HTTPError as error:
            audit.record_error(f"Telegram trade notice: {error}", now)
    return notice is not None


def send_due_reports(now, prices, settings, market, audit, trader, ai, telegram, chat_id):
    if trader is not None and trader.report_due(now):
        bank = trader.summary(prices, now)
        intelligence = trader.build_intelligence(now)
        ai_text = ""
        if ai is not None and intelligence.closed_positions:
            try:
                result = ai.analyze_performance({
                    "report_type": "paper_trading",
                    "bank": {"starting_balance_usdt": bank.starting_balance_usdt,
                             "current_equity_usdt": bank.equity_usdt},
                    "trade_intelligence": intelligence.as_dict(),
                })
                ai_text = (f"\n\n🤖 ИИ-вывод по сделкам\nОценка: {result.score}/100 "
                           f"({result.verdict}).\nВывод: {result.reason}\nРиск: {result.risk}")
            except (httpx.HTTPError, AIError) as error:
                audit.record_error(f"OpenAI trading audit: {error}", now)
        telegram.send(chat_id, bank.telegram_text() + "\n\n" + intelligence.telegram_text() + ai_text)
        for details_text in intelligence.trade_breakdown_texts():
            telegram.send(chat_id, details_text)
        trader.finish_report(prices, now)
    if not audit.report_due(now):
        return
    started = audit.period_started_at()
    symbols = tuple(sorted(market.symbols))
    events = {}
    for symbol in symbols:
        candles = market.fetch_minute_candles(
            symbol, started - settings.pump_window_seconds, now
        )
        detected = detect_pumps(
            candles, settings.pump_window_seconds,
            settings.pump_threshold_percent, settings.alert_cooldown_seconds,
        )
        events[symbol] = [event for event in detected if event >= started]
    summary = audit.build_summary(
        now, symbols, events, settings.poll_interval_seconds,
        settings.pump_threshold_percent,
    )
    performance = audit.build_signal_performance(now)
    ai_text = ""
    if ai is not None and performance.signal_count:
        try:
            result = ai.analyze_performance(performance.as_dict())
            ai_text = (f"\n\n🤖 ИИ-вывод по статистике\nОценка: {result.score}/100 "
                       f"({result.verdict}).\nВывод: {result.reason}\nРиск: {result.risk}")
        except (httpx.HTTPError, AIError) as error:
            audit.record_error(f"OpenAI daily audit: {error}", now)
    telegram.send(chat_id, summary.telegram_text() + "\n\n" + performance.telegram_text() + ai_text)
    audit.finish_period(now)


def main() -> None:
    settings = load_settings()
    binance = BinanceTestnetClient(
        settings.binance_api_key, settings.binance_api_secret,
        settings.binance_base_url,
    )
    telegram = TelegramClient(settings.telegram_bot_token)
    market = MarketMonitor(
        settings.market_data_base_url, settings.watch_symbols,
        settings.pump_window_seconds, settings.pump_threshold_percent,
        settings.alert_cooldown_seconds, settings.scan_all_usdt,
        settings.min_quote_volume_usdt, settings.early_threshold_percent,
        settings.max_signals_per_cycle,
    )
    audit = AuditLog(settings.audit_db_path)
    trader = PaperTrader(
        settings.audit_db_path, settings.paper_starting_balance_usdt,
        settings.paper_position_usdt, settings.paper_max_open_positions,
        settings.paper_min_ai_score, settings.paper_stop_loss_percent,
        settings.paper_take_profit_1_percent, settings.paper_take_profit_2_percent,
        settings.paper_take_profit_3_percent,
        settings.paper_trailing_drawdown_percent, settings.paper_max_hold_seconds,
        settings.estimated_round_trip_cost_percent,
        settings.paper_stagnation_after_seconds,
        settings.paper_stagnation_window_seconds,
    ) if settings.paper_trading_enabled else None
    ai = AIAnalyst(settings.openai_api_key, settings.openai_model) if settings.openai_api_key else None
    market_stream = None
    position_stream = None
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        prices = market.fetch_prices()
        market_stream = AllMarketMiniTickerStream(market.symbols, settings.min_quote_volume_usdt)
        market_stream.seed(prices, market.market_stats)
        market_stream.start()
        position_stream = PositionBookTickerStream(settings.paper_max_open_positions)
        position_stream.set_symbols(trader.open_symbols() if trader else ())
        position_stream.start()
        ai_status = "не настроен"
        if ai is not None:
            try:
                ai.check_connection()
                ai_status = f"подключён ({settings.openai_model})"
            except (httpx.HTTPError, AIError) as error:
                ai_status = "ошибка подключения"
                audit.record_error(f"OpenAI: {error}")
        monitoring = (
            f"весь Binance Spot USDT ({len(market.symbols)} активных, "
            f"{market.eligible_count} прошли фильтр)"
            if settings.scan_all_usdt else ", ".join(settings.watch_symbols)
        )
        telegram.send(
            chat_id,
            "✅ Home Deluxe Trading Bot запущен.\n"
            "Исполнение: Binance Spot Testnet.\n"
            f"Тестовая торговля разрешена: {'да' if account.get('canTrade') else 'нет'}.\n"
            "Реальные деньги не используются.\n"
            f"Мониторинг: {monitoring}.\n"
            "Поток рынка: примерно раз в 1 секунду.\n"
            "Открытые позиции: лучшая цена продажи в реальном времени.\n"
            f"Telegram-сигналы: "
            f"{'включены' if settings.telegram_signal_alerts_enabled else 'скрыты; только сделки и отчёты'}.\n"
            f"Ранний сигнал: рост от {settings.early_threshold_percent:g}% за "
            f"{settings.pump_window_seconds // 60} мин.\n"
            f"Сильный сигнал: от {settings.pump_threshold_percent:g}%.\n"
            + (f"Тестовые сделки: банк {settings.paper_starting_balance_usdt:g} USDT, "
               f"до {settings.paper_max_open_positions} позиций, вход от "
               f"{settings.paper_min_ai_score}/100.\n"
               "Выход: 50% на +0,7%, остаток 50% на +1%; стоп −0,5%.\n"
               if trader else "Тестовые сделки: выключены.\n")
            + f"ИИ-аналитик: {ai_status}.\nСуточный аудит: включён.",
        )
        print(f"Потоки рынка запущены. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        last_market = last_audit = last_fallback = last_report = 0.0
        while True:
            now = time.time()
            try:
                if trader:
                    for event_at, symbol, bid in position_stream.drain_events():
                        prices[symbol] = bid
                        notices = trader.update_positions({symbol: bid}, event_at)
                        valuation = dict(prices)
                        valuation.update(position_stream.latest())
                        send_trade_notices(trader, telegram, chat_id, notices, valuation, event_at)
                        if notices:
                            position_stream.set_symbols(trader.open_symbols())
                if now - last_market >= 1:
                    if market_stream.healthy(now):
                        prices, market.market_stats = market_stream.snapshot()
                        market.eligible_count = len(prices)
                    elif now - last_fallback >= 5:
                        prices = market.fetch_prices()
                        market_stream.set_symbols(market.symbols)
                        market_stream.seed(prices, market.market_stats)
                        last_fallback = now
                    if trader and not position_stream.healthy(now):
                        fallback = {s: prices[s] for s in trader.open_symbols() if s in prices}
                        notices = trader.update_positions(fallback, now)
                        send_trade_notices(trader, telegram, chat_id, notices, prices, now)
                        if notices:
                            position_stream.set_symbols(trader.open_symbols())
                    for signal in market.update(prices, now=now):
                        opened = process_signal(
                            signal, prices, now, market, audit, trader, ai,
                            telegram, chat_id, settings.telegram_signal_alerts_enabled,
                        )
                        if opened:
                            position_stream.set_symbols(trader.open_symbols())
                    last_market = now
                if now - last_audit >= settings.poll_interval_seconds:
                    audit.record_prices(prices, now)
                    audit.record_due_outcomes(prices, now, settings.estimated_round_trip_cost_percent)
                    if trader:
                        contexts = {}
                        for symbol in trader.stagnation_candidates(now):
                            try:
                                context = market.fetch_signal_context(symbol)
                                contexts[symbol] = (context.volume_ratio_5m,
                                                    context.taker_buy_ratio_percent,
                                                    context.order_book_imbalance_percent)
                            except (httpx.HTTPError, ValueError) as error:
                                audit.record_error(f"Position context {symbol}: {error}", now)
                        notices = trader.update_positions(prices, now, contexts)
                        send_trade_notices(trader, telegram, chat_id, notices, prices, now)
                        if notices:
                            position_stream.set_symbols(trader.open_symbols())
                    last_audit = now
                if settings.scan_all_usdt and now - market.last_symbol_refresh >= 3600:
                    refreshed = market.fetch_prices()
                    prices.update(refreshed)
                    market_stream.set_symbols(market.symbols)
                    market_stream.seed(refreshed, market.market_stats)
                if now - last_report >= 1:
                    send_due_reports(now, prices, settings, market, audit, trader, ai, telegram, chat_id)
                    last_report = now
                time.sleep(0.1)
            except httpx.HTTPError as error:
                audit.record_error(str(error))
                print(f"Временная ошибка внешнего API: {error}", flush=True)
                time.sleep(1)
    except KeyboardInterrupt:
        print("Мониторинг остановлен.")
    finally:
        if position_stream:
            position_stream.close()
        if market_stream:
            market_stream.close()
        if ai:
            ai.close()
        audit.close()
        if trader:
            trader.close()
        market.close()
        binance.close()
        telegram.close()


if __name__ == "__main__":
    main()
