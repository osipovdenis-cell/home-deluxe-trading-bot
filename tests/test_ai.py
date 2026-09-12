import sys
from types import SimpleNamespace
import unittest

try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    sys.modules["httpx"] = SimpleNamespace(Client=DummyClient)

from bot.ai import AIAnalyst, AIError


class AIAnalystTests(unittest.TestCase):
    def test_parses_analysis(self) -> None:
        result = AIAnalyst._parse_analysis(
            '{"score":72,"decision":"BUY","verdict":"средний","reason":"Импульс есть",'
            '"risk":"Возможен откат"}'
        )
        self.assertEqual(result.score, 72)
        self.assertEqual(result.decision, "BUY")
        self.assertEqual(result.verdict, "средний")

    def test_rejects_unknown_decision(self) -> None:
        with self.assertRaises(AIError):
            AIAnalyst._parse_analysis(
                '{"score":72,"decision":"ENTER","verdict":"x",'
                '"reason":"x","risk":"x"}'
            )

    def test_rejects_out_of_range_score(self) -> None:
        with self.assertRaises(AIError):
            AIAnalyst._parse_analysis(
                '{"score":101,"verdict":"высокий","reason":"x","risk":"y"}'
            )


if __name__ == "__main__":
    unittest.main()
