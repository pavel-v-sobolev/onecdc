#!/usr/bin/env bash
# Запуск docker-образа против отладочного контура (тестовая база торговли + dev-Postgres).
#
# Параметры не дублируются, а берутся из debug.py — там же, где их берут живые тесты, чтобы контуры
# не разъезжались. Меняете адрес 1С или базу — правите debug.py, и здесь ничего трогать не нужно.
#
# --network host обязателен: Postgres слушает только 127.0.0.1 хоста, а у контейнера свой сетевой
# стек, где localhost означает сам контейнер. Заодно так видна host-only сеть VirtualBox с 1С.
#
# ВНИМАНИЕ: и loop, и once подтверждают принятый пакет — изменения СПИСЫВАЮТСЯ из очереди обмена
# 1С, повторно она их не пришлёт. Прогон, который очередь не трогает, — это full_load
# (см. tests/test_cdc_run_once.py, там run_once вызывается с notify_changes=False).
#
# Использование:
#   tests/debug_docker.sh                     # один цикл (ONECDC_MODE=once) и выход
#   tests/debug_docker.sh loop                # вечный цикл, Ctrl-C для остановки
#   tests/debug_docker.sh once -e ONECDC_LOG_LEVEL=DEBUG   # всё после режима уходит в docker run
set -euo pipefail

cd "$(dirname "$0")"

IMAGE="${ONECDC_IMAGE:-sobolevp/onecdc:latest}"
MODE="${1:-once}"
[ $# -gt 0 ] && shift

# Значения читает сам Python: debug.py — обычный модуль, разбирать его текстом было бы хрупко.
PYTHON="${PYTHON:-../.venv/bin/python}"
eval "$("$PYTHON" - <<'PY'
import shlex
import debug

print(f"ODATA_URL={shlex.quote(debug.ODATA_URL)}")
print(f"ODATA_USER={shlex.quote(debug.ODATA_AUTH[0] if debug.ODATA_AUTH else '')}")
print(f"ODATA_PASSWORD={shlex.quote(debug.ODATA_AUTH[1] if debug.ODATA_AUTH else '')}")
print(f"EXCHANGE_NAME={shlex.quote(debug.EXCHANGE_NAME)}")
print(f"QUEUE_GUID={shlex.quote(debug.QUEUE_GUID)}")
print(f"DB_URL={shlex.quote(debug.DB_URL)}")
print(f"DB_SCHEMA={shlex.quote(debug.DB_SCHEMA)}")
PY
)"

echo "образ:  $IMAGE"
echo "режим:  ONECDC_MODE=$MODE"
echo "1С:     $ODATA_URL"
echo "схема:  $DB_SCHEMA"

exec docker run --rm --network host \
  -e ONECDC_ODATA_URL="$ODATA_URL" \
  -e ONECDC_ODATA_USER="$ODATA_USER" \
  -e ONECDC_ODATA_PASSWORD="$ODATA_PASSWORD" \
  -e ONECDC_EXCHANGE_NAME="$EXCHANGE_NAME" \
  -e ONECDC_QUEUE_GUID="$QUEUE_GUID" \
  -e ONECDC_DB_URL="$DB_URL" \
  -e ONECDC_DB_SCHEMA="$DB_SCHEMA" \
  -e ONECDC_DB_TEMP_SCHEMA="${DB_SCHEMA}_tmp" \
  -e ONECDC_MODE="$MODE" \
  -e TZ="${TZ:-Europe/Moscow}" \
  "$@" \
  "$IMAGE"
