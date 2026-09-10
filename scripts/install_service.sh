#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="home-deluxe-trading-bot.service"
SOURCE_FILE="/root/bot/systemd/$SERVICE_NAME"
TARGET_FILE="/etc/systemd/system/$SERVICE_NAME"

if [[ ! -f "/root/bot/.env" ]]; then
  echo "Ошибка: файл /root/bot/.env не найден."
  exit 1
fi

install -m 0644 "$SOURCE_FILE" "$TARGET_FILE"
systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
systemctl --no-pager --full status "$SERVICE_NAME"
