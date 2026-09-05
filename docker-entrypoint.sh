#!/bin/sh
# Две раскладки одного образа:
#   смонтирован /config со своим runner.py — запускается он (обработчики, расписания, свой pool_size);
#   не смонтирован — запускается `onecdc`, то есть репликатор, настроенный переменными ONECDC_*.
set -e

# Аргументы docker run имеют приоритет: `docker run image python -c ...` должен работать.
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

RUNNER="${ONECDC_RUNNER:-/config/runner.py}"

# exec обязателен: python должен остаться PID 1, иначе SIGTERM от `docker stop` не дойдёт
# до перехвата в stop_signal.py — циклы не дорабатывают итерацию, а незавершённые merge
# остаются висеть в реестре.
if [ -f "$RUNNER" ]; then
    # Каталог скрипта python сам кладёт в sys.path, поэтому `from handlers import ...`
    # внутри runner.py находит /config/handlers без правки путей.
    exec python "$RUNNER"
fi

exec onecdc
