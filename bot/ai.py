from dataclasses import dataclass
import json
import math
import random
import threading
import time
from email.utils import parsedate_to_datetime

import httpx


class AIError(RuntimeError):
    pass


class AIUnavailable(AIError):
    """A transport failure or a local cooldown; never an AI trading decision."""
    def __init__(self, kind, code, retry_at, attempted=True):
        self.kind, self.code, self.retry_at = kind, code, retry_at
        self.attempted = attempted
        super().__init__(f"[{kind}] code={code}; повтор не ранее {retry_at:.0f} UTC unix")


@dataclass(frozen=True)
class AIAnalysis:
    score: int
    verdict: str
    reason: str
    risk: str
    decision: str = "SKIP"


class AIAnalyst:
    def __init__(self, api_key: str, model: str) -> None:
        self.model = model
        self._request_lock = threading.Lock()
        self._blocked_until = 0.0
        self._consecutive_limits = 0
        self._health = dict(requests=0, successes=0, failures=0, cooldown_skips=0,
                            last_code='', last_kind='', last_success_at=0.0,
                            next_retry_at=0.0, updated_at=0.0)
        self.client = httpx.Client(
            base_url="https://api.openai.com",
            headers={"Authorization": f"Bearer {api_key}"},
            # A momentum decision that arrives too late is no longer useful.
            timeout=httpx.Timeout(12.0, connect=5.0),
        )

    def health(self):
        return dict(self._health)

    @staticmethod
    def _retry_after(value, now):
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            try:
                seconds = parsedate_to_datetime(value).timestamp() - now
            except (TypeError, ValueError, OverflowError):
                return 0.0
        return max(0.0, seconds) if math.isfinite(seconds) else 0.0

    def _post(self, *args, **kwargs):
        # One shared gate for signal and performance requests. Never sleep while
        # a momentum decision waits; the next candidate may probe after cooldown.
        with self._request_lock:
            now = time.time()
            self._health['updated_at'] = now
            if now < self._blocked_until:
                self._health['cooldown_skips'] += 1
                raise AIUnavailable(self._health['last_kind'], self._health['last_code'],
                                    self._blocked_until, attempted=False)
            self._health['requests'] += 1
            try:
                response = self.client.post(*args, **kwargs)
                if response.status_code != 429:
                    response.raise_for_status()
            except httpx.HTTPError as error:
                status = error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None
                kind = f'http-{status}' if status else 'timeout' if isinstance(error, httpx.TimeoutException) else 'transport'
                self._health.update(failures=self._health['failures'] + 1,
                    last_code=kind, last_kind=kind, next_retry_at=0.0, updated_at=time.time())
                raise
            if response.status_code == 429:
                try:
                    error = response.json().get('error', {})
                    code = error.get('code') if isinstance(error, dict) else None
                except (ValueError, AttributeError):
                    code = None
                quota_codes = {'insufficient_quota', 'credit_balance_exhausted',
                    'organization_spend_limit_exceeded', 'project_spend_limit_exceeded',
                    'organization_usage_limit_exceeded', 'billing_hard_limit_reached'}
                quota = code in quota_codes if isinstance(code, str) else False
                code = code if isinstance(code, str) and code in quota_codes | {'rate_limit_exceeded', 'slow_down'} else 'unknown_429'
                kind = 'quota' if quota else 'http-429'
                self._consecutive_limits += 1
                delay = 3600 if quota else min(900, 30 * 2 ** min(5, self._consecutive_limits - 1))
                now = time.time()
                delay = max(delay + random.uniform(0, delay * .1),
                            self._retry_after(response.headers.get('Retry-After'), now))
                self._blocked_until = now + delay
                self._health.update(failures=self._health['failures'] + 1,
                    last_code=code, last_kind=kind, next_retry_at=self._blocked_until, updated_at=now)
                # Deliberately omit arbitrary response messages and request headers.
                raise AIUnavailable(kind, code, self._blocked_until)
            response.raise_for_status()
            self._consecutive_limits = 0
            self._blocked_until = 0.0
            self._health.update(successes=self._health['successes'] + 1,
                last_code='', last_kind='', next_retry_at=0.0,
                last_success_at=time.time(), updated_at=time.time())
            return response

    @staticmethod
    def _extract_output_text(payload: dict) -> str:
        for item in payload.get("output", []):
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") == "output_text" and content.get("text"):
                    return str(content["text"])
        raise AIError("OpenAI не вернул текст анализа")

    @staticmethod
    def _parse_analysis(text: str) -> AIAnalysis:
        try:
            data = json.loads(text)
            score = int(data["score"])
            verdict = str(data["verdict"])
            reason = str(data["reason"])
            risk = str(data["risk"])
            decision = str(data.get("decision", "SKIP")).upper()
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise AIError("Некорректный формат ответа OpenAI") from error
        if not 0 <= score <= 100:
            raise AIError("Оценка OpenAI вне диапазона 0–100")
        if decision not in {"BUY", "WAIT", "SKIP"}:
            raise AIError("Неизвестное решение OpenAI")
        return AIAnalysis(score, verdict, reason, risk, decision)

    def analyze_momentum(
        self,
        symbol: str,
        price: float,
        change_percent: float,
        window_minutes: int,
        quote_volume_usdt: float = 0.0,
        change_24h_percent: float = 0.0,
        signal_kind: str = "сильный",
        quote_volume_5m_usdt: float | None = None,
        volume_ratio_5m: float | None = None,
        trades_5m: int | None = None,
        taker_buy_ratio_percent: float | None = None,
        spread_bps: float | None = None,
        bid_depth_usdt: float | None = None,
        ask_depth_usdt: float | None = None,
        order_book_imbalance_percent: float | None = None,
        historical_behavior: dict | None = None,
        entry_dynamics: dict | None = None,
        learned_policy: dict | None = None,
        large_trade_flow: dict | None = None,
    ) -> AIAnalysis:
        response = self._post(
            "/v1/responses",
            json={
                "model": self.model,
                "store": False,
                "reasoning": {"effort": "medium"},
                "max_output_tokens": 900,
                "instructions": (
                    "Ты последний защитный фильтр входа криптовалютного тестового "
                    "бота. Цель сделки: сначала +0,7%, затем +1%, стоп −0,5%, "
                    "поэтому ложноположительный BUY особенно опасен. Сравни "
                    "текущую форму движения, ускорение, откат от максимума, объём, "
                    "агрессивные покупки, стакан, BTC, ширину рынка и 3–4 "
                    "полноценных прошлых импульса именно этой монеты. BUY разрешай "
                    "только при согласованном подтверждении факторов и достаточной "
                    "вероятности достижения +0,7% раньше −0,5%. При противоречии, "
                    "нехватке данных, затухании или входе возле вершины выбирай "
                    "WAIT либо SKIP. Учитывай learned_policy как накопленную "
                    "размеченную статистику. Особое внимание уделяй числу "
                    "похожих рыночных примеров и доле случаев, когда +0,7% "
                    "достигались раньше −0,5%; маленькую выборку считай только "
                    "слабой подсказкой, а не доказательством. Сравни динамику "
                    "последних 5, 10, 20 и 60 секунд, устойчивость покупок, "
                    "стакан, спред и откат с успешной и неуспешной историей. "
                    "Если second_chance_90s=true, оцени повторное ускорение "
                    "после первого отказа особенно строго; это может быть "
                    "теневая оценка и само решение BUY не означает сделку. "
                    "Крупный поток оценивай только по исполненным сделкам: "
                    "важны устойчивый перевес крупных taker-покупок за 15 и "
                    "60 секунд и подтверждение ценой. Одиночную крупную сделку "
                    "или стенку не считай сигналом; крупная ask-стенка, резкое "
                    "исчезновение перевеса и экстремальный поток без роста "
                    "могут означать истощение импульса. "
                    "Если signal_kind — лидер или аномальный лидер, монета "
                    "находится под усиленным наблюдением. Одна мощная свеча "
                    "может быть началом движения и сама по себе не является "
                    "причиной отказа. Для BUY всё равно требуй удержание цены "
                    "и новое ускорение; для повторного входа после отката — "
                    "восстановление цены и реальных покупок. Учитывай тренд и "
                    "его эффективность за 15 минут, 1 час и 4 часа из "
                    "entry_dynamics. Не считай высокий рост за 24 часа "
                    "самостоятельным доказательством продолжения. "
                    "Кандидаты с неположительным изменением за последние "
                    "12 часов отсеиваются до AI и не рассматриваются. "
                    "При отсутствии истории допускай осторожный тестовый BUY от "
                    "70 только при согласованном текущем импульсе после всех "
                    "рыночных проверок. Если накопленная история монеты плохая, "
                    "BUY допустим только для исключительно сильного сценария: "
                    "оценка не ниже 85, объём не менее x2, покупки от 60%, стакан "
                    "от +15%, продолжающееся ускорение, поддержка BTC и ширины "
                    "рынка, без заметного отката. "
                    "Не обещай прибыль, не выдумывай "
                    "новости или социальные сигналы. Пиши по-русски и кратко."
                ),
                "input": json.dumps(
                    {
                        "symbol": symbol,
                        "price": price,
                        "change_percent": round(change_percent, 4),
                        "window_minutes": window_minutes,
                        "change_24h_percent": round(change_24h_percent, 4),
                        "quote_volume_24h_usdt": round(quote_volume_usdt, 2),
                        "signal_kind": signal_kind,
                        "quote_volume_5m_usdt": quote_volume_5m_usdt,
                        "volume_ratio_5m_vs_previous_20m": volume_ratio_5m,
                        "trades_5m": trades_5m,
                        "taker_buy_ratio_5m_percent": taker_buy_ratio_percent,
                        "spread_bps": spread_bps,
                        "top_20_bid_depth_usdt": bid_depth_usdt,
                        "top_20_ask_depth_usdt": ask_depth_usdt,
                        "order_book_imbalance_percent": order_book_imbalance_percent,
                        "historical_behavior": historical_behavior,
                        "entry_dynamics": entry_dynamics,
                        "learned_policy": learned_policy,
                        "large_trade_flow": large_trade_flow,
                    },
                    ensure_ascii=False,
                ),
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "momentum_analysis",
                        "strict": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "score": {
                                    "type": "integer",
                                    "minimum": 0,
                                    "maximum": 100,
                                },
                                "decision": {
                                    "type": "string",
                                    "enum": ["BUY", "WAIT", "SKIP"],
                                },
                                "verdict": {"type": "string"},
                                "reason": {"type": "string"},
                                "risk": {"type": "string"},
                            },
                            "required": [
                                "score", "decision", "verdict", "reason", "risk"
                            ],
                            "additionalProperties": False,
                        },
                    }
                },
            },
        )
        response.raise_for_status()
        return self._parse_analysis(self._extract_output_text(response.json()))

    def analyze_performance(self, performance: dict) -> AIAnalysis:
        response = self._post(
            "/v1/responses",
            json={
                "model": self.model,
                "store": False,
                "reasoning": {"effort": "low"},
                "max_output_tokens": 450,
                "instructions": (
                    "Ты аналитический модуль криптовалютного тестового бота. "
                    "Проанализируй переданную суточную статистику импульсных "
                    "сигналов или виртуальных сделок после торговых издержек. "
                    "Сравни контрольные стратегии, но не делай уверенных выводов "
                    "по малой выборке. Назови одно конкретное наблюдение и главный "
                    "риск. Не обещай прибыль. Пиши по-русски."
                ),
                "input": json.dumps(performance, ensure_ascii=False),
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "signal_performance_analysis",
                        "strict": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "score": {
                                    "type": "integer",
                                    "minimum": 0,
                                    "maximum": 100,
                                },
                                "verdict": {"type": "string"},
                                "reason": {"type": "string"},
                                "risk": {"type": "string"},
                            },
                            "required": ["score", "verdict", "reason", "risk"],
                            "additionalProperties": False,
                        },
                    }
                },
            },
        )
        response.raise_for_status()
        return self._parse_analysis(self._extract_output_text(response.json()))

    def check_connection(self) -> None:
        self.analyze_momentum("SYSTEM_CHECK", 1.0, 0.0, 5)

    def close(self) -> None:
        self.client.close()
