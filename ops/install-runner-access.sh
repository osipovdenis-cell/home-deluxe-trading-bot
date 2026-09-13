#!/usr/bin/env bash
set -Eeuo pipefail

install -o root -g root -m 0755 ops/deploy-home-deluxe-bot /usr/local/sbin/deploy-home-deluxe-bot
install -o root -g root -m 0755 ops/diagnose-home-deluxe-bot /usr/local/sbin/diagnose-home-deluxe-bot
cat >/etc/sudoers.d/github-runner-home-deluxe <<'EOF'
github-runner ALL=(root) NOPASSWD: /usr/local/sbin/deploy-home-deluxe-bot, /usr/local/sbin/diagnose-home-deluxe-bot
EOF
chmod 0440 /etc/sudoers.d/github-runner-home-deluxe
visudo -cf /etc/sudoers.d/github-runner-home-deluxe
echo "Runner permissions installed."
