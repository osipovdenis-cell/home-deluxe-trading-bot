import httpx


class TelegramClient:
    def __init__(self, token: str) -> None:
        self.client = httpx.Client(
            base_url=f"https://api.telegram.org/bot{token}", timeout=10.0
        )
        self.update_offset = 0

    def _updates(self) -> list[dict]:
        response = self.client.get(
            "/getUpdates", params={"offset": self.update_offset, "timeout": 0}
        )
        response.raise_for_status()
        updates = response.json().get("result", [])
        if updates:
            self.update_offset = max(
                int(update.get("update_id", 0)) for update in updates
            ) + 1
        return updates

    def latest_chat_id(self) -> str:
        updates = self._updates()
        for update in reversed(updates):
            message = update.get("message") or update.get("channel_post")
            if message and message.get("chat", {}).get("id") is not None:
                return str(message["chat"]["id"])
        raise RuntimeError("Сообщение боту не найдено. Отправьте ему текст и повторите запуск.")

    def discard_pending_updates(self) -> None:
        self._updates()

    def poll_commands(self, allowed_chat_id: str) -> list[str]:
        commands: list[str] = []
        for update in self._updates():
            message = update.get("message") or update.get("channel_post")
            if not message or str(message.get("chat", {}).get("id")) != allowed_chat_id:
                continue
            text = str(message.get("text", "")).strip()
            if text.startswith("/"):
                commands.append(text.split()[0].split("@")[0].lower())
        return commands

    def send(self, chat_id: str, text: str) -> None:
        response = self.client.post(
            "/sendMessage", json={"chat_id": chat_id, "text": text}
        )
        response.raise_for_status()

    def close(self) -> None:
        self.client.close()
