import logging
from contextlib import contextmanager
from contextvars import ContextVar

# Пакетный логгер. Логгеры модулей (getLogger(__name__) → cdc_1c.replicator и т.п.) — его потомки,
# поэтому хендлер/уровень, выставленные здесь, действуют на весь пакет.
logger = logging.getLogger("cdc_1c")

# Пометка режима загрузки в сообщениях лога: полная выгрузка идёт фоновыми потоками параллельно
# с чтением изменений, и без пометки в общем логе не разобрать, к чему относится строка.
LOAD_MODE_CHANGES = 'CHANGES'
LOAD_MODE_FULL = 'FULL RELOAD'
# Чтение $metadata не относится ни к пакету изменений, ни к полной выгрузке: оно общее и
# вызывается из обоих (а также лениво при появлении нового объекта/поля).
LOAD_MODE_METADATA = 'METADATA'
# Пользовательские обработчики (handlers) бегут своим потоком параллельно и циклу изменений, и
# полной выгрузке — без пометки их сообщения в общем логе не отличить от сообщений загрузки.
LOAD_MODE_HANDLER = 'HANDLER'

# Чужие логгеры, которые при автонастройке логирования приглушаем до WARNING. alembic приходит
# транзитом через dbmerge (MigrationContext/Operations при добавлении колонок) и на каждом merge
# пишет INFO о диалекте и транзакционном DDL — к работе репликатора это отношения не имеет.
NOISY_LOGGERS = ('alembic',)

# ContextVar, а не глобальная переменная: у каждого потока свой контекст, поэтому режим фоновой
# полной выгрузки не протекает в основной цикл и в соседние выгрузки.
_load_mode: ContextVar[str] = ContextVar('cdc_1c_load_mode', default='')


@contextmanager
def load_mode(mode: str):
    """Помечает режимом загрузки все сообщения лога cdc_1c, выданные внутри блока."""
    token = _load_mode.set(mode)
    try:
        yield
    finally:
        _load_mode.reset(token)


def log_prefix() -> str:
    """Префикс сообщения: '[FULL RELOAD] ' / '[CHANGES] ', либо пусто вне режима загрузки."""
    mode = _load_mode.get()
    return f'[{mode}] ' if mode else ''


class _LoadModeAdapter(logging.LoggerAdapter):
    """Дописывает пометку режима в текст сообщения. Именно в текст, а не в поле записи лога:
    формат вывода задаёт приложение-хозяин, и на своё поле оно ссылаться не станет."""

    def process(self, msg, kwargs):
        return f'{log_prefix()}{msg}', kwargs


def get_logger(name: str) -> logging.LoggerAdapter:
    """Логгер модуля cdc_1c: как logging.getLogger, но с пометкой режима загрузки."""
    return _LoadModeAdapter(logging.getLogger(name), {})


def _ensure_handler(level: int = logging.INFO) -> None:
    """
    Настроить вывод логов cdc_1c в stderr — только если логирование ещё не настроено
    (ни на логгере cdc_1c, ни выше по цепочке до root). Если приложение уже настроило
    логирование, ничего не делаем: не перебиваем его выбор уровня и не плодим дубли.

    Вызывается из конструктора Replicator1C (точка начала работы), а не на импорте: к этому
    моменту приложение, если хотело, уже сконфигурировало логирование, и hasHandlers() даёт
    корректный снимок. Идемпотентна — после первого вызова свой хендлер уже висит на cdc_1c,
    и hasHandlers() вернёт True.

    Заодно приглушает шумные чужие логгеры (NOISY_LOGGERS) — но только когда настраиваем
    логирование мы: если приложение настроило его само, его выбор уровней не трогаем.
    """
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(level)
        for name in NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)
