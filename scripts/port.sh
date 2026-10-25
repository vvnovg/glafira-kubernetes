#!/usr/bin/env bash
# Переносит файл из Glafira for Kafka с зафиксированного коммита (строку о нём в PORTED.md добавляет задача плана).
# Использование: scripts/port.sh <путь в kafka-explorer> <путь здесь>
# Репозиторий Kafka-версии — $KAFKA_REPO (по умолчанию ~/kafka-explorer).
set -euo pipefail
repo="${KAFKA_REPO:-$HOME/kafka-explorer}"
commit=8bbd0bd
src="$1"
dst="$2"
mkdir -p "$(dirname "$dst")"
git -C "$repo" show "$commit:$src" > "$dst"
