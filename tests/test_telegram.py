import unittest

from bot.telegram import TelegramClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, updates):
        self.updates = updates
        self.posts = []

    def get(self, _path, params=None):
        offset = int((params or {}).get("offset", 0))
        return FakeResponse({
            "result": [
                update for update in self.updates
                if int(update["update_id"]) >= offset
            ]
        })

    def post(self, path, json=None):
        self.posts.append((path, json))
        return FakeResponse({"ok": True})


class TelegramClientTests(unittest.TestCase):
    def test_splits_long_messages_without_losing_text(self):
        telegram = TelegramClient.__new__(TelegramClient)
        telegram.client = FakeClient([])
        text = ("строка\n" * 1000).rstrip()

        telegram.send("123", text)

        parts = [payload["text"] for _, payload in telegram.client.posts]
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(part) <= 4000 for part in parts))
        self.assertEqual("\n".join(parts), text)

    def test_polls_only_commands_from_allowed_chat_without_replay(self):
        telegram = TelegramClient.__new__(TelegramClient)
        telegram.update_offset = 0
        telegram.client = FakeClient([
            {"update_id": 10, "message": {
                "chat": {"id": 123}, "text": "/status"
            }},
            {"update_id": 11, "message": {
                "chat": {"id": 999}, "text": "/ai"
            }},
            {"update_id": 12, "message": {
                "chat": {"id": 123}, "text": "обычный текст"
            }},
        ])
        self.assertEqual(telegram.poll_commands("123"), ["/status"])
        self.assertEqual(telegram.poll_commands("123"), [])


if __name__ == "__main__":
    unittest.main()
