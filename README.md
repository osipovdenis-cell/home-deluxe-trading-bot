# Home Deluxe Trading Bot

Первая безопасная версия проекта. Она подключается только к Binance Spot Testnet,
проверяет тестовый API-ключ, определяет Telegram chat ID по последнему сообщению и
отправляет уведомление. Реальные сделки эта версия не совершает.

## Безопасность

- Никогда не отправляйте API secret или Telegram token в чат.
- Храните секреты только в локальном `.env` на сервере.
- Для реального Binance API никогда не включайте вывод средств.
- Пока `BINANCE_BASE_URL` указывает на `testnet.binance.vision`, реальные деньги не используются.

## Запуск

1. Скопируйте `.env.example` в `.env`.
2. Вставьте тестовые Binance API key/secret и Telegram bot token.
3. Установите зависимости: `python -m pip install -r requirements.txt`.
4. Запустите: `python -m bot.main`.

Если `TELEGRAM_CHAT_ID` пуст, программа возьмёт chat ID из последнего сообщения,
отправленного боту, и покажет его в терминале.
