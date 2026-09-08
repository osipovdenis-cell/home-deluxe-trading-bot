import httpx


class TelegramClient:
    def __init__(self, token: str) -> None:
        self.client = httpx.Client(
            base_url=f"https://api.telegram.org/bot{token}", timeout=10.0
        )

    def latest_chat_id(self) -> str:
        response = self.client.get("/getUpdates")
        response.raise_for_status()
        updates = response.json().get("result", [])
        for update in reversed(updates):
            message = update.get("message") or update.get("channel_post")
            if message and message.get("chat", {}).get("id") is not None:
                return str(message["chat"]["id"])
        raise RuntimeError("Сообщение боту не найдено. Отправьте ему текст и повторите запуск.")

    def send(self, chat_id: str, text: str) -> None:
        response = self.client.post(
            "/sendMessage", json={"chat_id": chat_id, "text": text}
        )
        response.raise_for_status()

    def close(self) -> None:
        self.client.close()
