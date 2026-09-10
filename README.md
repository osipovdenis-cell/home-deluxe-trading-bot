# Home Deluxe Trading Bot

Безопасная версия проекта подключается к Binance Spot Testnet, проверяет тестовый
API-ключ и круглосуточно следит за выбранными монетами через публичные рыночные
данные Binance. При росте от 3% за 5 минут бот отправляет Telegram-сигнал.
Реальные сделки эта версия не совершает.

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

По умолчанию отслеживаются DOGE, SHIB, PEPE, BONK, FLOKI и WIF к USDT. Список,
окно, порог, частота проверки и пауза между повторными уведомлениями настраиваются
переменными `WATCH_SYMBOLS`, `PUMP_WINDOW_SECONDS`, `PUMP_THRESHOLD_PERCENT`,
`POLL_INTERVAL_SECONDS` и `ALERT_COOLDOWN_SECONDS`.

Если `TELEGRAM_CHAT_ID` пуст, программа возьмёт chat ID из последнего сообщения,
отправленного боту, и покажет его в терминале.
