from dataclasses import dataclass
import json

import httpx


class AIError(RuntimeError):
    pass


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
        self.client = httpx.Client(
            base_url="https://api.openai.com",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=45.0,
        )

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
    ) -> AIAnalysis:
        response = self.client.post(
            "/v1/responses",
            json={
                "model": self.model,
                "store": False,
                "reasoning": {"effort": "high"},
                "max_output_tokens": 450,
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
                    "размеченную статистику, но не отменяй жёсткий запрет BLOCK. "
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
        response = self.client.post(
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
