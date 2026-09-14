import time

import httpx

from bot.ai import AIAnalyst, AIError
from bot.audit import AuditLog, detect_pumps
from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.market import MarketMonitor
from bot.streams import (
    AllMarketMiniTickerStream,
    LeaderOrderFlowStream,
    PositionBookTickerStream,
)
from bot.telegram import TelegramClient
from bot.trading import PaperTrader


def send_trade_notices(trader, telegram, chat_id, notices, prices, now):
    for notice in notices:
        telegram.send(chat_id, trader.notice_telegram_text(notice, prices, now))


def exceptional_new_entry(analysis, context, dynamics) -> bool:
    """Allow a rare, strongly confirmed setup while symbol history is cold."""
    return bool(
        analysis is not None
        and analysis.decision == "BUY"
        and analysis.score >= 85
        and context is not None
        and context.volume_ratio_5m >= 2
        and context.taker_buy_ratio_percent >= 60
        and context.order_book_imbalance_percent is not None
        and context.order_book_imbalance_percent >= 15
        and dynamics.change_15s_percent >= 0.05
        and dynamics.change_60s_percent >= 0.3
        and dynamics.pullback_from_5m_high_percent >= -0.05
        and dynamics.btc_change_300s_percent is not None
        and dynamics.btc_change_300s_percent >= -0.1
        and dynamics.market_breadth_60s_percent >= 45
    )


def history_entry_policy(behavior, exceptional: bool, base_score: int) -> tuple[bool, int]:
    """Return whether history permits a test entry and its minimum AI score."""
    history_unfavorable = len(behavior.impulses) >= 3 and not behavior.favorable
    return (not history_unfavorable or exceptional, max(base_score, 85 if history_unfavorable else 0))


def openai_error_kind(error: Exception) -> str:
    text = str(error).lower()
    if isinstance(error, httpx.TimeoutException) or "timeout" in text or "timed out" in text:
        return "timeout"
    if isinstance(error, httpx.HTTPStatusError):
        return f"http-{error.response.status_code}"
    if "некорректный формат" in text:
        return "format"
    if "не вернул текст" in text:
        return "empty"
    return "other"


def analyze_momentum_with_retries(ai, *args, **kwargs):
    """Return analysis, final error and attempts; retry transient/bad output twice."""
    final_error = None
    for attempt in range(1, 4):
        try:
            return ai.analyze_momentum(*args, **kwargs), None, attempt
        except (httpx.HTTPError, AIError) as error:
            final_error = error
            if (
                isinstance(error, httpx.HTTPStatusError)
                and error.response.status_code in {400, 401, 403}
            ):
                break
            if attempt < 3:
                time.sleep(0.25 * attempt)
    return None, final_error, attempt


def leader_ai_entry_policy(analysis, is_leader_reentry: bool) -> tuple[bool, str | None]:
    """Use AI caution as a delay for a leader, never as a permanent veto."""
    if analysis is None or analysis.decision == "BUY" or is_leader_reentry:
        return True, None
    return False, (
        f"AI {analysis.decision} {analysis.score}/100: первый вход отложен "
        "до отката и повторного ускорения"
    )


def process_signal(
    signal, prices, now, market, audit, trader, ai, telegram, chat_id,
    settings, preloaded_context=None,
):
    leader_paper_entry = "лидер" in signal.kind
    context = preloaded_context
    if context is None:
        try:
            context = market.fetch_signal_context(signal.symbol)
        except (httpx.HTTPError, ValueError) as error:
            audit.record_error(f"Signal context {signal.symbol}: {error}", now)
            print(f"Ошибка данных объёма {signal.symbol}: {error}", flush=True)
    context_values = (
        (context.quote_volume_5m_usdt, context.volume_ratio_5m,
         context.trades_5m, context.taker_buy_ratio_percent,
         context.spread_bps, context.bid_depth_usdt, context.ask_depth_usdt,
         context.order_book_imbalance_percent)
        if context else (None,) * 8
    )
    execution_safe, rejection_reason, tick_percent = market.execution_safety(
        signal.symbol, signal.price, context,
        max_spread_percent=0.25 if leader_paper_entry else 0.1,
    )
    dynamics = market.entry_dynamics(signal.symbol, now)
    shadow_prefilter_reason = None
    if not execution_safe:
        if signal.is_rescue:
            shadow_prefilter_reason = rejection_reason or "небезопасное исполнение"
        else:
            audit.record_signal(
                now, signal.symbol, signal.price, signal.kind, signal.change_percent,
                signal.change_24h_percent, signal.quote_volume_usdt,
                None, "вход отклонён фильтром исполнения", *context_values,
            )
            audit.record_entry_rejection(
                now,
                signal.symbol,
                rejection_reason or "небезопасное исполнение",
                context.spread_bps if context else None,
                tick_percent,
            )
            print(
                f"Вход {signal.symbol} отклонён: {rejection_reason}", flush=True
            )
            return False
    if execution_safe:
        quality_safe, quality_reason = (
            market.leader_entry_quality(context, dynamics)
            if leader_paper_entry
            else market.entry_quality(context, dynamics)
        )
    else:
        quality_safe, quality_reason = False, shadow_prefilter_reason
    if not quality_safe and shadow_prefilter_reason is None:
        if signal.is_rescue:
            shadow_prefilter_reason = quality_reason or "слабое качество импульса"
        else:
            audit.record_signal(
                now, signal.symbol, signal.price, signal.kind, signal.change_percent,
                signal.change_24h_percent, signal.quote_volume_usdt,
                None, "вход отклонён качеством импульса", *context_values,
                entry_dynamics=dynamics.as_dict(),
            )
            audit.record_entry_rejection(
                now, signal.symbol, quality_reason or "слабое качество импульса",
                context.spread_bps, tick_percent,
            )
            print(f"Вход {signal.symbol} отклонён: {quality_reason}", flush=True)
            return False
    behavior = audit.build_symbol_behavior(
        signal.symbol,
        now,
        settings.early_threshold_percent,
        settings.paper_take_profit_1_percent,
        settings.paper_take_profit_2_percent,
        settings.paper_stop_loss_percent,
    )
    learned_features = {
        "confirmation_progress_percent": signal.confirmation_progress_percent,
        "confirmation_change_5s_percent": signal.confirmation_change_5s_percent,
        "confirmation_change_10s_percent": signal.confirmation_change_10s_percent,
        "volume_ratio_5m": context.volume_ratio_5m if context else None,
        "taker_buy_ratio_percent": context.taker_buy_ratio_percent if context else None,
        "order_book_imbalance_percent": (
            context.order_book_imbalance_percent if context else None
        ),
        "change_60s_percent": dynamics.change_60s_percent,
        "pullback_from_high_percent": dynamics.pullback_from_5m_high_percent,
        "large_trade_imbalance_60s_percent": (
            context.large_trade_imbalance_60s_percent if context else None
        ),
        "large_trade_count_60s": (
            context.large_trade_count_60s if context else None
        ),
        "bid_wall_share_percent": (
            context.bid_wall_share_percent if context else None
        ),
        "ask_wall_share_percent": (
            context.ask_wall_share_percent if context else None
        ),
    }
    learned = audit.build_learning_profile(
        signal.symbol, now, learned_features
    )
    dynamics_payload = dynamics.as_dict()
    dynamics_payload["second_chance_90s"] = bool(signal.is_rescue)
    dynamics_payload["change_12h_percent"] = (
        market.change_12h_percent.get(signal.symbol)
    )
    dynamics_payload["leader_mode"] = (
        signal.kind if "лидер" in signal.kind else None
    )
    dynamics_payload["leader_reentry_after_pullback"] = bool(
        signal.is_leader_reentry
    )
    if context is not None:
        dynamics_payload.update({
            "trend_change_15m_percent": context.trend_change_15m_percent,
            "trend_change_60m_percent": context.trend_change_60m_percent,
            "trend_change_240m_percent": context.trend_change_240m_percent,
            "trend_efficiency_15m_percent": (
                context.trend_efficiency_15m_percent
            ),
            "trend_efficiency_60m_percent": (
                context.trend_efficiency_60m_percent
            ),
            "trend_efficiency_240m_percent": (
                context.trend_efficiency_240m_percent
            ),
        })
    analysis = None
    analysis_error = None
    analysis_attempts = 0
    if ai is not None:
        analysis, analysis_error, analysis_attempts = analyze_momentum_with_retries(
            ai,
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
                behavior.as_dict(),
                dynamics_payload,
                learned.as_dict(),
                {
                    "adaptive_large_trade_usdt": context.large_trade_threshold_usdt,
                    "large_buy_15s_usdt": context.large_buy_volume_15s_usdt,
                    "large_sell_15s_usdt": context.large_sell_volume_15s_usdt,
                    "large_buy_60s_usdt": context.large_buy_volume_60s_usdt,
                    "large_sell_60s_usdt": context.large_sell_volume_60s_usdt,
                    "large_flow_imbalance_60s_percent": (
                        context.large_trade_imbalance_60s_percent
                    ),
                    "large_trades_60s": context.large_trade_count_60s,
                    "largest_bid_wall_share_percent": context.bid_wall_share_percent,
                    "largest_ask_wall_share_percent": context.ask_wall_share_percent,
                    "continuous_buy_5s_usdt": context.flow_buy_5s_usdt,
                    "continuous_sell_5s_usdt": context.flow_sell_5s_usdt,
                    "continuous_buy_15s_usdt": context.flow_buy_15s_usdt,
                    "continuous_sell_15s_usdt": context.flow_sell_15s_usdt,
                    "continuous_buy_60s_usdt": context.flow_buy_60s_usdt,
                    "continuous_sell_60s_usdt": context.flow_sell_60s_usdt,
                    "continuous_cvd_60s_percent": context.flow_cvd_60s_percent,
                    "trade_rate_acceleration": context.flow_trade_rate_acceleration,
                    "price_response_60s_percent": context.flow_price_change_60s_percent,
                    "price_efficiency_per_10k": context.flow_price_efficiency_per_10k,
                    "ask_depletion_percent": context.flow_ask_depletion_percent,
                    "bid_support_percent": context.flow_bid_support_percent,
                    "continuous_spread_change_bps": context.flow_spread_change_bps,
                } if context else None,
            )
        if analysis_error is not None:
            kind = openai_error_kind(analysis_error)
            audit.record_error(
                f"OpenAI {signal.symbol} [{kind}] после "
                f"{analysis_attempts} попыток: {analysis_error}", now,
            )
            print(
                f"Ошибка анализа OpenAI {signal.symbol} [{kind}] после "
                f"{analysis_attempts} попыток: {analysis_error}", flush=True,
            )
    audit.record_signal(
        now, signal.symbol, signal.price, signal.kind, signal.change_percent,
        signal.change_24h_percent, signal.quote_volume_usdt,
        analysis.score if analysis else None,
        analysis.verdict if analysis else None, *context_values,
        ai_decision=analysis.decision if analysis else None,
        ai_reason=analysis.reason if analysis else None,
        ai_risk=analysis.risk if analysis else None,
        analysis_version=2 if analysis else 1,
        entry_dynamics=dynamics_payload,
    )
    if analysis is None:
        if not leader_paper_entry or shadow_prefilter_reason is not None:
            kind = openai_error_kind(analysis_error) if analysis_error else "disabled"
            audit.record_entry_rejection(
                now, signal.symbol, f"нет полного решения AI ({kind})",
                context.spread_bps if context else None,
                tick_percent,
            )
            return False
    if shadow_prefilter_reason is not None:
        reason = (
            "теневая AI-оценка второго шанса: "
            f"{analysis.decision} {analysis.score}/100; сделка не открыта — "
            f"{shadow_prefilter_reason}"
        )
        audit.record_entry_rejection(
            now, signal.symbol, reason,
            context.spread_bps if context else None, tick_percent,
        )
        print(f"Вход {signal.symbol} отклонён: {reason}", flush=True)
        return False
    if leader_paper_entry:
        # Independent paired experiment: use completion time, not signal time.
        audit.rocket_comparison.candidate(
            signal.symbol, analysis.decision if analysis else None,
            signal.is_leader_reentry, time.time(),
            context.spread_bps if context else None,
            settings.paper_stop_loss_percent,
            settings.estimated_round_trip_cost_percent,
        )
        ai_entry_allowed, ai_delay_reason = leader_ai_entry_policy(
            analysis, signal.is_leader_reentry
        )
        if not ai_entry_allowed:
            audit.record_entry_rejection(
                now, signal.symbol, ai_delay_reason,
                context.spread_bps if context else None, tick_percent,
            )
            print(f"Вход {signal.symbol} отложен: {ai_delay_reason}", flush=True)
            return False
    history_unfavorable = len(behavior.impulses) >= 3 and not behavior.favorable
    exceptional = exceptional_new_entry(analysis, context, dynamics)
    history_allowed, required_ai_score = history_entry_policy(
        behavior,
        exceptional,
        learned.required_ai_score(settings.paper_min_ai_score),
    )
    if not history_allowed and not leader_paper_entry:
        reason = (
            f"история неблагоприятна: цель +0,7% достигалась "
            f"{behavior.first_target_hits}/{len(behavior.impulses)} раз; "
            "исключительно сильный вход 85/100 не подтверждён"
        )
        audit.record_entry_rejection(
            now, signal.symbol, reason, context.spread_bps, tick_percent,
        )
        print(f"Вход {signal.symbol} отклонён: {reason}", flush=True)
        return False
    if learned.consecutive_trade_losses >= 2 and not leader_paper_entry:
        stronger_checks = (
            context.volume_ratio_5m >= 1.5,
            context.taker_buy_ratio_percent >= 55,
            context.order_book_imbalance_percent is not None
            and context.order_book_imbalance_percent >= 0,
            dynamics.change_60s_percent is not None
            and dynamics.change_60s_percent >= 0.1,
            dynamics.pullback_from_5m_high_percent is not None
            and dynamics.pullback_from_5m_high_percent >= -0.08,
        )
        if not all(stronger_checks):
            reason = (
                f"после {learned.consecutive_trade_losses} убытков подряд "
                "не пройдены усиленные проверки объёма, покупателей, "
                "стакана и продолжения импульса"
            )
            audit.record_entry_rejection(
                now, signal.symbol, reason, context.spread_bps, tick_percent,
            )
            print(f"Вход {signal.symbol} отклонён: {reason}", flush=True)
            return False
    if learned.blocked and not leader_paper_entry:
        reason = f"обучаемый фильтр BLOCK: {learned.explanation}"
        audit.record_entry_rejection(
            now, signal.symbol, reason, context.spread_bps, tick_percent,
        )
        print(f"Вход {signal.symbol} отклонён: {reason}", flush=True)
        return False
    if (
        not leader_paper_entry
        and (analysis.decision != "BUY" or analysis.score < required_ai_score)
    ):
        reason = (
            f"AI решил {analysis.decision}, оценка {analysis.score}/100; "
            f"нужно BUY и минимум {required_ai_score}/100 "
            f"(обучаемый профиль: {learned.status})"
        )
        audit.record_entry_rejection(
            now, signal.symbol, reason, context.spread_bps, tick_percent,
        )
        print(f"Вход {signal.symbol} отклонён: {reason}", flush=True)
        return False
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
        f"\nИИ-решение: {analysis.decision}. Оценка: "
        f"{analysis.score}/100 ({analysis.verdict}).\n"
            f"Причина: {analysis.reason}\nРиск: {analysis.risk}\n"
            f"Обучаемый профиль: {learned.status}. {learned.explanation}\n"
        if analysis else "\nИИ-анализ временно недоступен.\n"
    )
    trade_signal_kind = signal.kind + (
        " · повторный вход" if signal.is_leader_reentry else ""
    )
    notice = (
        trader.open_on_signal(signal.symbol, signal.price, trade_signal_kind,
                              analysis.score if analysis else 0, now,
                              bypass_min_score=leader_paper_entry)
        if trader else None
    )
    signal_text = (
            f"{'🚀' if signal.kind == 'сильный' or 'лидер' in signal.kind else '⚡️'} "
            f"{signal.kind.capitalize()} сигнал {signal.symbol}\n"
            f"Изменение: +{signal.change_percent:.2f}% за "
            f"{signal.window_seconds // 60} мин.\n"
            f"Изменение за 24 ч: {signal.change_24h_percent:+.2f}%.\n"
            f"Оборот за 24 ч: {signal.quote_volume_usdt:,.0f} USDT.\n"
            f"Цена: {signal.price:.10g}\n{context_text}{ai_text}"
            "Это информационный сигнал, не команда на покупку."
    )
    if settings.telegram_signal_alerts_enabled:
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
    learning = audit.build_learning_report(now)
    confirmation = audit.build_confirmation_audit(now)
    ai_text = ""
    if ai is not None and performance.signal_count:
        try:
            result = ai.analyze_performance(performance.as_dict())
            ai_text = (f"\n\n🤖 ИИ-вывод по статистике\nОценка: {result.score}/100 "
                       f"({result.verdict}).\nВывод: {result.reason}\nРиск: {result.risk}")
        except (httpx.HTTPError, AIError) as error:
            audit.record_error(f"OpenAI daily audit: {error}", now)
    telegram.send(
        chat_id,
        summary.telegram_text() + "\n\n" + performance.telegram_text()
        + "\n\n" + learning.telegram_text()
        + "\n\n" + confirmation.telegram_text() + ai_text,
    )
    audit.finish_period(now)


def handle_observer_commands(commands, now, prices, audit, trader, telegram, chat_id):
    for command in commands:
        if command == "/status":
            text = (
                trader.summary(prices, now).telegram_text()
                + "\n\n" + trader.rocket_report_text(prices, now)
                if trader is not None else "🧪 Тестовая торговля выключена."
            )
        elif command == "/ai":
            text = audit.recent_ai_decisions_text()
        elif command == "/learning":
            telegram.send(chat_id, "⏳ Команда /learning принята, формирую отчёт…")
            telegram.send(
                chat_id,
                audit.build_learning_report(now).telegram_text()
                + "\n\n" + audit.build_confirmation_audit(now).telegram_text()
            )
            telegram.send(chat_id, audit.candidate_pattern_report_text(now))
            telegram.send(
                chat_id,
                audit.probability_shadow_report_text(now)
                + "\n\n" + audit.leader_report_text(now)
                + "\n\n" + audit.leader_funnel_report_text(
                    now, now - 86400
                )
                + "\n\n" + audit.order_flow_report_text(now),
            )
            telegram.send(chat_id, audit.leader_path_report_text(now))
            telegram.send(chat_id, audit.rocket_comparison.report())
            if trader is not None:
                telegram.send(chat_id, trader.rocket_report_text(prices, now))
                telegram.send(chat_id, trader.post_stop_report_text(now))
            continue
        elif command in {"/help", "/start"}:
            text = (
                "👁 Команды наблюдателя\n"
                "/status — банк и позиции\n"
                "/ai — последние решения AI\n"
                "/learning — накопленное обучение"
            )
        else:
            continue
        telegram.send(chat_id, text)


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
        settings.max_signals_per_cycle, settings.entry_confirmation_seconds,
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
        0,
    ) if settings.paper_trading_enabled else None
    ai = AIAnalyst(settings.openai_api_key, settings.openai_model) if settings.openai_api_key else None
    market_stream = None
    position_stream = None
    order_flow_stream = None
    try:
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        telegram.discard_pending_updates()
        prices = market.fetch_prices()
        market.refresh_12h_changes(prices, time.time())
        market_stream = AllMarketMiniTickerStream(market.symbols, settings.min_quote_volume_usdt)
        market_stream.seed(prices, market.market_stats)
        market_stream.start()
        position_stream = PositionBookTickerStream(settings.paper_max_open_positions)
        position_stream.set_symbols(trader.open_symbols() if trader else ())
        position_stream.start()
        order_flow_stream = LeaderOrderFlowStream(max_symbols=20)
        order_flow_stream.start()
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
            "Фильтр направления: только монеты с ростом за последние 12 ч "
            "по скользящей статистике Binance "
            f"({sum(value > 0 for value in market.change_12h_percent.values())} "
            "сейчас в зелёной зоне).\n"
            f"Подтверждение входа: {settings.entry_confirmation_seconds} сек; "
            "объём, покупки, стакан, рынок и история монеты.\n"
            "Обучение: включено; результат каждого импульса через 15 минут "
            "влияет на следующие входы.\n"
            f"Холодный старт: без истории тестовый BUY от "
            f"{settings.paper_min_ai_score}/100 после всех фильтров; "
            "при плохой истории — только исключительный BUY от 85/100.\n"
            "Ожидание 20 секунд: ведётся теневой контроль пропущенной прибыли.\n"
            "Второй шанс: после отказа ещё 90 секунд наблюдения; повторный "
            "анализ только при новом ускорении.\n"
            "Крупный поток: исполненные крупные покупки/продажи за 15/60 сек "
            "и концентрация стенок стакана; пока теневой фактор.\n"
            "Order flow лидеров: непрерывные сделки и стакан за 5/15/60 сек; "
            "CVD, ускорение и эффективность покупок сохраняются в обучение.\n"
            "Вероятностная модель: теневой прогноз по прошлым исходам; "
            "тренд 15 мин/1 ч/4 ч; сделки сама не открывает.\n"
            "Лидеры: топ-5 роста за 24 ч и одиночный импульс от 3%; "
            "повторный вход ищется после отката и нового ускорения; AI оценивает, "
            "но после рыночных фильтров не блокирует тестовый вход.\n"
            f"Наблюдатель: каждые {settings.observer_report_interval_seconds // 3600} ч; "
            "команды /status, /ai, /learning.\n"
            + (f"Тестовые сделки: банк {settings.paper_starting_balance_usdt:g} USDT, "
               f"до {settings.paper_max_open_positions} позиций: обычный "
               "скальпинг только собирает аналитику, все слоты отданы лидерам; вход от "
               f"{settings.paper_min_ai_score}/100 и только решение BUY.\n"
               "Обычный выход: 50% на +0,7%, остаток 50% на +1%; стоп −0,5%. "
               "Лидер: держим 100%; после +1% защищаем минимум +1% и "
               "выходим при откате 1 п.п. от максимума.\n"
               if trader else "Тестовые сделки: выключены.\n")
            + f"ИИ-аналитик: {ai_status}.\nСуточный аудит: включён.",
        )
        print(f"Потоки рынка запущены. TELEGRAM_CHAT_ID={chat_id}", flush=True)
        last_market = last_audit = last_fallback = last_report = 0.0
        last_12h_refresh = time.time()
        last_command_poll = time.time()
        last_observer = time.time()
        while True:
            now = time.time()
            try:
                # Commands must not wait behind market scans and AI requests.
                if now - last_command_poll >= 2:
                    commands = telegram.poll_commands(chat_id)
                    handle_observer_commands(
                        commands, now, prices, audit, trader, telegram, chat_id
                    )
                    last_command_poll = now
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
                        audit.rocket_comparison.tick(prices, now)
                        market.eligible_count = len(prices)
                    elif now - last_fallback >= 5:
                        prices = market.fetch_prices()
                        audit.rocket_comparison.tick(prices, time.time())
                        market_stream.set_symbols(market.symbols)
                        market_stream.seed(prices, market.market_stats)
                        last_fallback = now
                    if trader and not position_stream.healthy(now):
                        fallback = {s: prices[s] for s in trader.open_symbols() if s in prices}
                        notices = trader.update_positions(fallback, now)
                        send_trade_notices(trader, telegram, chat_id, notices, prices, now)
                        if notices:
                            position_stream.set_symbols(trader.open_symbols())
                    signals = market.update(prices, now=now)
                    order_flow_stream.set_symbols(market.order_flow_symbols())
                    confirmation_contexts = {}
                    for rejected_at, rejected_symbol, reason in (
                        market.drain_confirmation_rejections()
                    ):
                        audit.record_entry_rejection(
                            rejected_at, rejected_symbol, reason, None, None
                        )
                        print(
                            f"Вход {rejected_symbol} отклонён: {reason}",
                            flush=True,
                        )
                    for confirmation_event in market.drain_confirmation_events():
                        confirmation_context = None
                        try:
                            confirmation_context = market.fetch_signal_context(
                                confirmation_event.symbol
                            )
                            confirmation_context = market.with_order_flow(
                                confirmation_context,
                                order_flow_stream.snapshot(
                                    confirmation_event.symbol, now
                                ),
                            )
                        except (httpx.HTTPError, ValueError) as error:
                            audit.record_error(
                                f"Confirmation context {confirmation_event.symbol}: {error}",
                                now,
                            )
                        audit.record_confirmation_event(
                            confirmation_event,
                            confirmation_context,
                            market.entry_dynamics(confirmation_event.symbol, now),
                        )
                        if confirmation_event.accepted:
                            confirmation_contexts[confirmation_event.symbol] = (
                                confirmation_context
                            )
                    confirmation_symbols = (
                        market.active_confirmation_symbols()
                        | audit.active_confirmation_symbols(now)
                    )
                    audit.record_confirmation_prices(
                        {
                            symbol: prices[symbol]
                            for symbol in confirmation_symbols
                            if symbol in prices
                        },
                        now,
                    )
                    for signal in signals:
                        opened = process_signal(
                            signal, prices, now, market, audit, trader, ai,
                            telegram, chat_id, settings,
                            confirmation_contexts.pop(signal.symbol, None),
                        )
                        if opened:
                            position_stream.set_symbols(trader.open_symbols())
                    last_market = now
                if now - last_audit >= settings.poll_interval_seconds:
                    audit.record_prices(prices, now)
                    audit.record_due_outcomes(prices, now, settings.estimated_round_trip_cost_percent)
                    audit.refresh_learning_examples(
                        now,
                        settings.paper_take_profit_1_percent,
                        settings.paper_take_profit_2_percent,
                        settings.paper_stop_loss_percent,
                    )
                    audit.refresh_confirmation_outcomes(
                        now,
                        settings.paper_take_profit_1_percent,
                        settings.paper_stop_loss_percent,
                    )
                    if trader:
                        notices = trader.update_positions(prices, now)
                        send_trade_notices(trader, telegram, chat_id, notices, prices, now)
                        if notices:
                            position_stream.set_symbols(trader.open_symbols())
                    last_audit = now
                if settings.scan_all_usdt and now - market.last_symbol_refresh >= 3600:
                    refreshed = market.fetch_prices()
                    prices.update(refreshed)
                    market_stream.set_symbols(market.symbols)
                    market_stream.seed(refreshed, market.market_stats)
                if now - last_12h_refresh >= 300:
                    market.refresh_12h_changes(prices, now)
                    last_12h_refresh = now
                if now - last_report >= 1:
                    send_due_reports(now, prices, settings, market, audit, trader, ai, telegram, chat_id)
                    last_report = now
                if now - last_observer >= settings.observer_report_interval_seconds:
                    observer_text = audit.observer_report_text(
                        now, last_observer
                    )
                    if observer_text:
                        rocket_text = (
                            "\n\n" + trader.rocket_report_text(
                                prices, now, last_observer
                            )
                            + "\n\n" + trader.post_stop_report_text(
                                now, last_observer
                            ) if trader is not None else ""
                        )
                        telegram.send(
                            chat_id,
                            observer_text + "\n\n"
                            + audit.build_learning_report(now).telegram_text()
                            + "\n\n"
                            + audit.build_confirmation_audit(now).telegram_text()
                            + "\n\n"
                            + audit.leader_funnel_report_text(
                                now, last_observer
                            )
                            + "\n\n" + audit.order_flow_report_text(now)
                            + "\n\n" + audit.leader_path_report_text(
                                now, now - last_observer
                            )
                            + rocket_text,
                        )
                    last_observer = now
                    telegram.send(chat_id, audit.rocket_comparison.report())
                time.sleep(0.1)
            except httpx.HTTPError as error:
                audit.record_error(str(error))
                print(f"Временная ошибка внешнего API: {error}", flush=True)
                time.sleep(1)
    except KeyboardInterrupt:
        print("Мониторинг остановлен.")
    finally:
        if order_flow_stream:
            order_flow_stream.close()
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
