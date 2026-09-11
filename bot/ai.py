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
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise AIError("Некорректный формат ответа OpenAI") from error
        if not 0 <= score <= 100:
            raise AIError("Оценка OpenAI вне диапазона 0–100")
        return AIAnalysis(score, verdict, reason, risk)

    def analyze_momentum(
        self,
        symbol: str,
        price: float,
        change_percent: float,
        window_minutes: int,
    ) -> AIAnalysis:
        response = self.client.post(
            "/v1/responses",
            json={
                "model": self.model,
                "store": False,
                "reasoning": {"effort": "low"},
                "max_output_tokens": 350,
                "instructions": (
                    "Ты аналитический модуль криптовалютного тестового бота. "
                    "Оцени вероятность продолжения импульса в ближайшие 15 минут "
                    "только по переданным данным. Не обещай прибыль, не выдумывай "
                    "новости или социальные сигналы. Пиши по-русски и кратко."
                ),
                "input": json.dumps(
                    {
                        "symbol": symbol,
                        "price": price,
                        "change_percent": round(change_percent, 4),
                        "window_minutes": window_minutes,
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
