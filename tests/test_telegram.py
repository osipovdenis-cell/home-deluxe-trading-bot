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

    def get(self, _path, params=None):
        offset = int((params or {}).get("offset", 0))
        return FakeResponse({
            "result": [
                update for update in self.updates
                if int(update["update_id"]) >= offset
            ]
        })


class TelegramClientTests(unittest.TestCase):
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
