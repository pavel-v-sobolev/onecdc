# Образ ставит onecdc с PyPI по версии: версия пакета = версия образа, собирать нечего.
#   docker build --build-arg ONECDC_VERSION=1.2.0 -t sobolevp/onecdc:1.2.0 -t sobolevp/onecdc:latest .
# Версия должна быть уже опубликована на PyPI, иначе pip внутри сборки её не найдёт.
# Базовый образ указан тегом, чтобы получать обновления, включая патчи безопасности.
# Для получения свежего образа при сборке используйте --pull.
# Зависимости Python ограничены диапазонами из пакета; точные версии эта сборка не фиксирует.
FROM python:3.13-slim

ARG ONECDC_VERSION
LABEL org.opencontainers.image.title="onecdc" \
      org.opencontainers.image.description="Change data capture (CDC) from 1C:Enterprise to your data warehouse" \
      org.opencontainers.image.source="https://github.com/pavel-v-sobolev/onecdc" \
      org.opencontainers.image.version="${ONECDC_VERSION}" \
      org.opencontainers.image.licenses="MIT"

# tzdata: расписания FullLoadCron считаются в локальном времени (TZ), а в slim-образе базы зон нет.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

# PYTHONUNBUFFERED: логи не залипают в буфере при `docker logs`.
# PYTHONDONTWRITEBYTECODE: не пытаемся писать __pycache__ в смонтированный (обычно ro) /config.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Экстра postgres — psycopg2-binary, колесо, поэтому компилятор и dev-заголовки в образе не нужны.
RUN test -n "${ONECDC_VERSION}" || (echo "build-arg ONECDC_VERSION is required" >&2; exit 1) \
    && pip install --no-cache-dir "onecdc[postgres]==${ONECDC_VERSION}"

# Шаблон копируется из рабочего каталога. Собирайте образ из версии репозитория,
# соответствующей ONECDC_VERSION; готовый шаблон можно извлечь командой ниже.
#   docker run --rm sobolevp/onecdc:latest tar c -C /opt/onecdc config | tar x
COPY config /opt/onecdc/config
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

RUN useradd --create-home --uid 1000 cdc
USER cdc

# Сюда монтируется пользовательский конфиг; не смонтирован — работает env-режим (см. entrypoint).
WORKDIR /config

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
