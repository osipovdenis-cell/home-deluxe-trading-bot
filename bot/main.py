import time
import math
import sqlite3
from queue import Empty
from types import SimpleNamespace

import httpx

from bot.rocket_daily import DailyWorker, report_text as daily_report, volume_report_text
from bot.rocket_volume_shadow import capture as capture_volume_shadow
from bot.rocket_timing_shadow import TimingWorker, report_text as timing_report
from bot.rocket_entry_wait import RocketEntryWaitWorker
from bot.rocket_signal_worker import RocketSignalWorker
from bot.rocket_entry_guard import fading_buy_guard
from bot.ai import AIAnalyst, AIError, AIUnavailable
from bot.audit import AuditLog, detect_pumps
from bot.binance_testnet import BinanceTestnetClient
from bot.config import load_settings
from bot.market import MarketMonitor, SignalMarketContext, EntryDynamics
from bot.streams import (
    AllMarketMiniTickerStream,
    LeaderOrderFlowStream,
    PositionBookTickerStream,
)
from bot.telegram import TelegramClient
from bot.trading import PaperTrader
from bot.scalp_shadow import ScalpQuoteStream
from bot.audit import SymbolBehavior
from bot.reporting import rocket_totals, scalp_totals
from bot.execution import PositionExitWorker, fresh_entry
from bot.report_export import ReportExportWorker, collect_reports
from bot.rocket_cards import RocketPathWorker, entry_probe, cards, format_card, shadow_summary


def make_paper_trader(settings):
    return PaperTrader(
        settings.audit_db_path, settings.paper_starting_balance_usdt,
        settings.paper_position_usdt, settings.paper_max_open_positions,
        settings.paper_min_ai_score, settings.paper_stop_loss_percent,
        settings.paper_take_profit_1_percent, settings.paper_take_profit_2_percent,
        settings.paper_take_profit_3_percent,
        settings.paper_trailing_drawdown_percent, settings.paper_max_hold_seconds,
        settings.estimated_round_trip_cost_percent,
        settings.paper_stagnation_after_seconds,
        settings.paper_stagnation_window_seconds, 0,
    )


def send_overall_reports(now, prices, audit, trader, telegram, chat_id):
    if trader is not None:
        telegram.send(chat_id, rocket_totals(trader, prices, now))
        for stop_text in trader.stop_audit.report_texts(now):
            telegram.send(chat_id, stop_text)
    telegram.send(chat_id, scalp_totals(audit.scalp_shadow, now))
    telegram.send(chat_id, audit.model_status_text(now) + '\n\n'
                  + audit.scalp_shadow.learning_status(now))


def build_report_snapshot(settings, prices, now, exit_healthy):
    # Created and closed in the export thread, never shared with entry/exit workers.
    export_audit = AuditLog(settings.audit_db_path)
    export_trader = None
    try:
        if settings.paper_trading_enabled:
            export_trader = make_paper_trader(settings)
        bundle = collect_reports(export_audit, export_trader, prices, now,
                                 handle_observer_commands, exit_healthy)
        bundle['runtime'] = dict(paper_trading_enabled=settings.paper_trading_enabled,
                                 stop_loss_percent=settings.paper_stop_loss_percent,
                                 round_trip_cost_percent=settings.estimated_round_trip_cost_percent)
        return bundle
    finally:
        export_audit.close()
        if export_trader is not None:
            export_trader.close()


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
    if isinstance(error, AIUnavailable):
        return error.kind
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
            if isinstance(error, AIUnavailable):
                return None, error, attempt if error.attempted else 0
            if (
                isinstance(error, httpx.HTTPStatusError)
                and error.response.status_code in {400, 401, 403, 429}
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


def timed_entry_call(diagnostics, stage, callback, *args, **kwargs):
    started = time.perf_counter()
    try:
        return callback(*args, **kwargs)
    finally:
        if diagnostics is not None:
            stages = diagnostics.setdefault('stages', {})
            stages[stage] = stages.get(stage, 0.0) + max(0.0, time.perf_counter() - started)


def process_signal(
    signal, prices, now, market, audit, trader, ai, telegram, chat_id,
    settings, preloaded_context=None, processing_started=None, initial_stages=None,
):
    opened = False
    diagnostics = {'stages': dict(initial_stages or {})}
    processing_started = time.time() if processing_started is None else processing_started
    processing_clock = time.perf_counter()
    timing = market.__dict__.get('rocket_timing_worker') if "лидер" in signal.kind else None
    token = (signal.symbol, now)
    if timing is not None:
        timing.send('begin', token, signal, now, time.time(),
                    settings.paper_stop_loss_percent, settings.estimated_round_trip_cost_percent)
    try:
        opened = _process_signal(signal, prices, now, market, audit, trader, ai,
                                 telegram, chat_id, settings, preloaded_context, diagnostics)
        return opened
    finally:
        if "лидер" in signal.kind and isinstance(audit, AuditLog):
            try:
                row = audit.connection.execute(
                    "SELECT reason FROM paper_entry_rejections WHERE symbol=? AND timestamp>=? "
                    "ORDER BY rowid DESC LIMIT 1", (signal.symbol, now)).fetchone()
                reason = "покупка" if opened else row[0] if row else "ожидание/пропуск без записанной причины"
                daily = market.__dict__.get('rocket_daily_worker')
                if daily is not None:
                    daily.capture(signal.symbol, now, time.time(), reason, opened,
                                  settings.paper_stop_loss_percent, settings.estimated_round_trip_cost_percent,
                                  volume_experiment=diagnostics.get('volume_experiment'))
                audit.rocket_spread.record_gate(time.time(), signal.symbol, "решение входа", reason)
                if timing is not None:
                    timing.send('decision', token, time.time(), reason, opened)
            except Exception as error:
                print("Rocket gate diagnostics: " + type(error).__name__, flush=True)
        if "лидер" in signal.kind and isinstance(audit, AuditLog):
            try:
                audit.record_entry_latency(signal.symbol, now, processing_started, time.time(),
                                           time.perf_counter()-processing_clock+sum((initial_stages or {}).values()), diagnostics['stages'])
            except sqlite3.Error as error:
                print('Entry latency recording: ' + type(error).__name__, flush=True)
        if "лидер" not in signal.kind:
            rejection = audit.connection.execute(
                "SELECT reason FROM paper_entry_rejections WHERE symbol=? AND timestamp>=? "
                "ORDER BY rowid DESC LIMIT 1", (signal.symbol, now)
            ).fetchone()
            audit.scalp_shadow.decision(signal.symbol, time.time(),
                                        reason=rejection[0] if rejection else None)
            audit.connection.commit()


def handle_ready_rocket(job, audit, trader, telegram, ai, chat_id, settings, flow):
    started, clock = time.time(), time.perf_counter()
    context = None
    try:
        context = job.market.fetch_signal_context(job.signal.symbol)
        context = job.market.with_order_flow(context, flow.snapshot(job.signal.symbol, time.time()))
    except (httpx.HTTPError, ValueError) as error:
        audit.record_error('Ready rocket context: ' + type(error).__name__, job.at)
    if job.confirmation is not None:
        audit.record_confirmation_event(job.confirmation, context,
            job.market.entry_dynamics(job.signal.symbol, job.at),
            scalp_now=time.time(), scalp_cost=settings.estimated_round_trip_cost_percent)
    if job.market._entry_cancelled():
        return False
    return process_signal(job.signal, job.prices, job.at, job.market, audit, trader,
                          ai, telegram, chat_id, settings, context, processing_started=started,
                          initial_stages={'контекст и запись подтверждения':time.perf_counter()-clock})


def _process_signal(
    signal, prices, now, market, audit, trader, ai, telegram, chat_id,
    settings, preloaded_context=None, diagnostics=None,
):
    leader_paper_entry = "лидер" in signal.kind
    waiter = market.__dict__.get('rocket_entry_waiter')
    if leader_paper_entry and waiter is not None and signal.symbol in waiter.symbols():
        return False  # The approved entry is already observed without another AI call.
    context = preloaded_context
    if context is None:
        try:
            context = timed_entry_call(diagnostics, "контекст рынка", market.fetch_signal_context, signal.symbol)
        except (httpx.HTTPError, ValueError) as error:
            audit.record_error(f"Signal context {signal.symbol}: {error}", now)
            print(f"Ошибка данных объёма {signal.symbol}: {error}", flush=True)
    if leader_paper_entry:
        volume = context.volume_ratio_5m if context is not None else None
        if (not isinstance(volume, (int, float)) or isinstance(volume, bool)
                or not math.isfinite(volume) or volume < 1):
            reason = (f"объём лидера ниже ×1: ×{volume:.3f}"
                      if isinstance(volume, (int, float)) and math.isfinite(volume)
                      else "объём лидера неизвестен; вход отложен")
            if (diagnostics is not None and market.__dict__.get('rocket_daily_worker') is not None
                    and isinstance(volume,(int,float)) and not isinstance(volume,bool)
                    and math.isfinite(volume) and 0 <= volume < 1):
                diagnostics['volume_experiment'] = capture_volume_shadow(market,signal,context,now)
            audit.record_entry_rejection(now, signal.symbol, reason,
                                         context.spread_bps if context else None, None)
            return False
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
    if execution_safe and leader_paper_entry and isinstance(audit, AuditLog):
        try:
            audit.rocket_spread.observe(signal, context, dynamics, market, time.time(),
                settings.paper_stop_loss_percent, settings.estimated_round_trip_cost_percent)
        except Exception as error:
            print("Rocket spread shadow: " + type(error).__name__, flush=True)
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
    behavior = timed_entry_call(diagnostics, "история монеты", audit.build_symbol_behavior,
        signal.symbol,
        now,
        settings.early_threshold_percent,
        settings.paper_take_profit_1_percent,
        settings.paper_take_profit_2_percent,
        settings.paper_stop_loss_percent,
    ) if leader_paper_entry else SymbolBehavior(signal.symbol, ())
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
    learned = timed_entry_call(diagnostics, "обучающая история", audit.build_learning_profile,
        signal.symbol, now, learned_features,
        strategy=None if leader_paper_entry else "scalp",
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
        analysis, analysis_error, analysis_attempts = timed_entry_call(diagnostics, "AI с повторами", analyze_momentum_with_retries,
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
        audit.record_ai_health(ai.health())
        if analysis_error is not None and not (
            isinstance(analysis_error, AIUnavailable) and not analysis_error.attempted
        ):
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
    if not leader_paper_entry:
        audit.scalp_shadow.decision(signal.symbol, time.time(),
                                    decision=analysis.decision if analysis else "NO_RESPONSE")
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
    if not leader_paper_entry:
        audit.scalp_shadow.decision(signal.symbol, time.time(), allowed=True,
                                    reason="все фильтры пройдены; торговля скальпинга выключена")
    entry_at, entry_price = now, signal.price
    timing = market.__dict__.get('rocket_timing_worker')
    if timing is not None and leader_paper_entry:
        timing.send('approve', (signal.symbol, now), time.time(), context, dynamics)
    if trader is not None and leader_paper_entry:
        try:
            if trader.exit_monitor_healthy is not None and not trader.exit_monitor_healthy():
                raise ValueError('обработчик выходов недоступен; покупка отложена')
            entry_at, bid, entry_price = timed_entry_call(diagnostics, "свежая цена", fresh_entry,
                market.client, signal.symbol, signal.price,
                settings.paper_stop_loss_percent, .25,
            )
            if trader.exit_monitor_healthy is not None and not trader.exit_monitor_healthy():
                raise ValueError('обработчик выходов недоступен; покупка отложена')
            prices[signal.symbol] = bid
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
            audit.record_entry_rejection(time.time(), signal.symbol,
                f'Свежесть входа: {error}', None, None)
            return False
    diagnostic_probe = timed_entry_call(diagnostics, "финальная проверка", entry_probe, market, signal, context, dynamics, now) if leader_paper_entry else None
    if trader is not None and leader_paper_entry:
        entry_allowed, entry_reason = fading_buy_guard(diagnostic_probe)
        if not entry_allowed:
            if waiter is not None:
                queued = waiter.submit(signal,context,dynamics,analysis.score if analysis else 0,now)
                entry_reason += ('; короткое наблюдение без нового AI/20с' if queued
                                 else '; очередь короткого наблюдения заполнена')
            audit.record_entry_rejection(time.time(), signal.symbol, entry_reason,
                                         context.spread_bps if context else None, None)
            return False
    if leader_paper_entry:
        cancelled = market.__dict__.get('_entry_cancelled')
        if cancelled is not None and cancelled():
            return False
        if time.time() - entry_at > 2:
            audit.record_entry_rejection(time.time(), signal.symbol,
                'свежая цена устарела за время финальной проверки', None, None)
            return False
    notice = (
        trader.open_on_signal(signal.symbol, entry_price, trade_signal_kind,
                              analysis.score if analysis else 0, entry_at,
                              bypass_min_score=leader_paper_entry, signal_timestamp=now)
        if trader else None
    )
    if notice is not None and diagnostic_probe is not None:
        # Preserve the probe; the final fading-buy guard has already passed.
        try:
            sink=market.__dict__.get('rocket_shadow_sink')
            row=trader.connection.execute('SELECT id FROM paper_positions WHERE symbol=? AND opened_at=? ORDER BY id DESC LIMIT 1',
                                          (signal.symbol,entry_at)).fetchone()
            if sink is not None and row is not None:
                sink(row[0],{**diagnostic_probe,'entry_bid':bid,'entry_quote_at':entry_at})
        except Exception:
            print('Rocket shadow entry could not be recorded',flush=True)
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
            telegram.send(chat_id, trader.notice_telegram_text(notice, prices, entry_at))
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
                if not isinstance(error, AIUnavailable) or error.attempted:
                    audit.record_error(f"OpenAI trading audit: {error}", now)
            finally:
                audit.record_ai_health(ai.health())
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
            if not isinstance(error, AIUnavailable) or error.attempted:
                audit.record_error(f"OpenAI daily audit: {error}", now)
        finally:
            audit.record_ai_health(ai.health())
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
                + "\n\n" + rocket_totals(trader, prices, now)
                if trader is not None else "🧪 Тестовая торговля выключена."
            )
        elif command == "/ai":
            text = audit.recent_ai_decisions_text()
        elif command == "/learning":
            telegram.send(chat_id, "⏳ Команда /learning принята, формирую отчёт…")
            send_overall_reports(now, prices, audit, trader, telegram, chat_id)
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
            telegram.send(chat_id, audit.rocket_spread.report(now))
            telegram.send(chat_id, timing_report(audit.connection))
            telegram.send(chat_id, audit.entry_latency_report_text(now))
            telegram.send(chat_id, daily_report(audit.connection, now))
            telegram.send(chat_id, volume_report_text(audit.connection, now))
            telegram.send(chat_id, audit.scalp_shadow.report(now))
            if trader is not None:
                telegram.send(chat_id, trader.rocket_report_text(prices, now))
                telegram.send(chat_id, trader.post_stop_report_text(now))
                telegram.send(chat_id, shadow_summary(trader.connection))
                for card in cards(trader.connection, now, 5):
                    telegram.send(chat_id, format_card(card))
            continue
        elif command == "/rockets":
            if trader is not None:
                telegram.send(chat_id, rocket_totals(trader, prices, now))
                telegram.send(chat_id, shadow_summary(trader.connection))
                for card in cards(trader.connection, now, 10):
                    telegram.send(chat_id, format_card(card))
            continue
        elif command in {"/help", "/start"}:
            text = (
                "👁 Команды наблюдателя\n"
                "/status — банк и позиции\n"
                "/ai — последние решения AI\n"
                "/learning — накопленное обучение\n"
                "/rockets — карточки последних 10 сделок ракет"
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
    trader = make_paper_trader(settings) if settings.paper_trading_enabled else None
    ai = AIAnalyst(settings.openai_api_key, settings.openai_model) if settings.openai_api_key else None
    market_stream = None
    position_stream = None
    order_flow_stream = None
    scalp_stream = None
    position_worker = None
    report_worker = None
    rocket_path_worker = None
    entry_wait_worker = None
    timing_worker = None
    daily_worker = None
    signal_worker = None
    try:
        # Start protection before slow account checks, market history and AI checks.
        position_stream = PositionBookTickerStream(settings.paper_max_open_positions)
        position_stream.set_symbols(trader.open_symbols() if trader else ())
        position_stream.start()
        if trader is not None:
            position_worker = PositionExitWorker(
                lambda: make_paper_trader(settings), position_stream,
                settings.market_data_base_url,
            )
            position_worker.start()
            trader.exit_monitor_healthy = position_worker.healthy
        account = binance.account()
        chat_id = settings.telegram_chat_id or telegram.latest_chat_id()
        telegram.discard_pending_updates()
        prices = market.fetch_prices()
        market.refresh_12h_changes(prices, time.time())
        market_stream = AllMarketMiniTickerStream(market.symbols, settings.min_quote_volume_usdt)
        market_stream.seed(prices, market.market_stats)
        market_stream.start()
        order_flow_stream = LeaderOrderFlowStream(max_symbols=20)
        order_flow_stream.start()
        daily_worker = DailyWorker(settings.audit_db_path)
        daily_worker.start()
        market.rocket_daily_worker = daily_worker
        timing_worker = TimingWorker(settings.audit_db_path, market)
        timing_worker.start()
        market.rocket_timing_worker = timing_worker
        market.rocket_volume_probe = timing_worker.stream.entry_probe
        if trader is not None:
            market.rocket_probe=order_flow_stream.entry_probe
            def recovery_probe(symbol, original):
                probe=entry_probe(market,SimpleNamespace(symbol=symbol,price=original['signal_price']),
                    SignalMarketContext(**original['before_context']),
                    EntryDynamics(**original['before_dynamics']),original['signal_at'])
                probe['allowed']=fading_buy_guard(probe)[0] and market.change_12h_percent.get(symbol,0)>0
                return probe
            rocket_path_worker=RocketPathWorker(settings.audit_db_path,
                recovery_probe=recovery_probe,recovery_stop=settings.paper_stop_loss_percent)
            rocket_path_worker.start()
            market.rocket_shadow_sink=rocket_path_worker.record_shadow
            entry_wait_worker=RocketEntryWaitWorker(
                lambda: make_paper_trader(settings), market, position_worker.healthy,
                settings.market_data_base_url)
            market.rocket_entry_waiter=entry_wait_worker
            entry_wait_worker.start()
        def signal_resources():
            worker_audit = AuditLog(settings.audit_db_path)
            worker_trader = None
            try:
                worker_trader = make_paper_trader(settings) if trader is not None else None
                if worker_trader is not None:
                    worker_trader.exit_monitor_healthy = position_worker.healthy
                client = httpx.Client(base_url=settings.market_data_base_url, timeout=15)
                return worker_audit, worker_trader, client
            except Exception:
                worker_audit.close()
                if worker_trader is not None:
                    worker_trader.close()
                raise
        signal_worker = RocketSignalWorker(signal_resources,
            lambda job, worker_audit, worker_trader, notices: handle_ready_rocket(
                job, worker_audit, worker_trader, notices, ai, chat_id, settings, order_flow_stream))
        signal_worker.start()
        scalp_stream = ScalpQuoteStream()
        audit.scalp_shadow.expire(time.time())
        scalp_stream.set_symbols(audit.scalp_shadow.active_symbols())
        scalp_stream.start()
        report_worker = ReportExportWorker(
            lambda snapshot_prices, snapshot_now: build_report_snapshot(
                settings, snapshot_prices, snapshot_now,
                position_worker.healthy() if position_worker else None,
            )
        )
        report_worker.set_prices(prices)
        report_worker.start()
        ai_status = "не настроен"
        if ai is not None:
            try:
                ai.check_connection()
                ai_status = f"подключён ({settings.openai_model})"
            except (httpx.HTTPError, AIError) as error:
                ai_status = "ошибка подключения"
                audit.record_error(f"OpenAI: {error}")
            finally:
                audit.record_ai_health(ai.health())
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
                report_worker.set_prices(prices)
                while not signal_worker.errors.empty():
                    audit.record_error(signal_worker.errors.get(), now)
                while not signal_worker.messages.empty():
                    notice_chat, notice_text = signal_worker.messages.get()
                    telegram.send(notice_chat, notice_text)
                if entry_wait_worker is not None:
                    while not entry_wait_worker.errors.empty():
                        audit.record_error(entry_wait_worker.errors.get(),now)
                    while not entry_wait_worker.messages.empty():
                        telegram.send(chat_id,entry_wait_worker.messages.get())
                if rocket_path_worker is not None:
                    while True:
                        try: key,card_text=rocket_path_worker.notifications.get_nowait()
                        except Empty: break
                        try:
                            telegram.send(chat_id,card_text)
                            rocket_path_worker.acknowledge(*key)
                        except httpx.HTTPError:
                            rocket_path_worker.notifications.put((key,card_text))
                            break
                if position_worker is not None:
                    exit_texts, exit_errors = position_worker.drain()
                    for error in exit_errors:
                        audit.record_error('Exit worker: '+error, now)
                    for text in exit_texts:
                        telegram.send(chat_id, text)
                scalp_quotes, overflow = scalp_stream.drain_quotes()
                if overflow:
                    audit.scalp_shadow.expire(now, overflow=True)
                else:
                    for at, symbol, bid, ask in scalp_quotes:
                        audit.scalp_shadow.quote(at, symbol, bid, ask)
                audit.scalp_shadow.expire(now)
                audit.connection.commit()
                scalp_stream.set_symbols(audit.scalp_shadow.active_symbols())
                # Commands must not wait behind market scans and AI requests.
                if now - last_command_poll >= 2:
                    commands = telegram.poll_commands(chat_id)
                    handle_observer_commands(
                        commands, now, prices, audit, trader, telegram, chat_id
                    )
                    last_command_poll = now
                if now - last_market >= 1:
                    if market_stream.healthy(now):
                        prices, market.market_stats = market_stream.snapshot()
                        audit.rocket_comparison.tick(prices, now)
                        audit.rocket_spread.tick(prices, now)
                        market.eligible_count = len(prices)
                    elif now - last_fallback >= 5:
                        prices = market.fetch_prices()
                        audit.rocket_comparison.tick(prices, time.time())
                        audit.rocket_spread.tick(prices, time.time())
                        market_stream.set_symbols(market.symbols)
                        market_stream.seed(prices, market.market_stats)
                        last_fallback = now
                    signals = market.update(prices, now=now)
                    order_flow_stream.set_symbols((*entry_wait_worker.symbols(), *market.order_flow_symbols()) if entry_wait_worker else market.order_flow_symbols())
                    timing_worker.watch_symbols(market.order_flow_symbols())
                    daily_worker.watch_symbols(market.order_flow_symbols())
                    confirmation_events = market.drain_confirmation_events()
                    accepted_leaders = {e.symbol:e for e in confirmation_events
                                        if e.accepted and 'лидер' in (e.signal_kind or '')}
                    if rocket_path_worker is not None:
                        rocket_path_worker.watch_symbols(market.order_flow_symbols())
                    queued_leaders = set()
                    # Submit ready leaders before any slow rejected-candidate REST
                    # context, scalp analysis, Telegram report or learning rebuild.
                    for signal in signals:
                        if 'лидер' in signal.kind:
                            queued = signal_worker.submit(signal, prices, now, market,
                                                          accepted_leaders.get(signal.symbol))
                            if queued:
                                queued_leaders.add(signal.symbol)
                            else:
                                reason = 'очередь ракет занята или монета уже обрабатывается'
                                audit.record_entry_rejection(now, signal.symbol, reason, None, None)
                                daily_worker.capture(signal.symbol, now, time.time(), reason, False,
                                    settings.paper_stop_loss_percent, settings.estimated_round_trip_cost_percent)
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
                    for confirmation_event in confirmation_events:
                        if confirmation_event.accepted and confirmation_event.symbol in queued_leaders:
                            continue
                        if "лидер" in (confirmation_event.signal_kind or "") and not confirmation_event.accepted:
                            daily_worker.capture(confirmation_event.symbol, confirmation_event.started_at,
                                time.time(), confirmation_event.reason, False,
                                settings.paper_stop_loss_percent, settings.estimated_round_trip_cost_percent,
                                source='confirmation')
                            audit.rocket_spread.record_gate(now, confirmation_event.symbol,
                                "подтверждение", confirmation_event.reason)
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
                            scalp_now=time.time(),
                            scalp_cost=settings.estimated_round_trip_cost_percent,
                        )
                        scalp_stream.set_symbols(audit.scalp_shadow.active_symbols())
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
                        if 'лидер' in signal.kind:
                            continue
                        opened = process_signal(
                            signal, prices, now, market, audit, trader, ai,
                            telegram, chat_id, settings,
                            confirmation_contexts.pop(signal.symbol, None),
                        )
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
                    send_overall_reports(now, prices, audit, trader, telegram, chat_id)
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
                    telegram.send(chat_id, audit.rocket_spread.report(now))
                    telegram.send(chat_id, timing_report(audit.connection))
                    telegram.send(chat_id, daily_report(audit.connection, now))
                    telegram.send(chat_id, volume_report_text(audit.connection, now))
                    if trader is not None:
                        telegram.send(chat_id, shadow_summary(trader.connection))
                    telegram.send(chat_id, audit.scalp_shadow.report(now))
                time.sleep(0.1)
            except httpx.HTTPError as error:
                audit.record_error(str(error))
                print(f"Временная ошибка внешнего API: {error}", flush=True)
                time.sleep(1)
    except KeyboardInterrupt:
        print("Мониторинг остановлен.")
    finally:
        if signal_worker:
            signal_worker.close()
        if daily_worker:
            daily_worker.close()
        if timing_worker:
            timing_worker.close()
        if entry_wait_worker:
            entry_wait_worker.close()
        if rocket_path_worker:
            rocket_path_worker.close()
        if report_worker:
            report_worker.close()
        if position_worker:
            position_worker.close()
        if scalp_stream:
            scalp_stream.close()
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
