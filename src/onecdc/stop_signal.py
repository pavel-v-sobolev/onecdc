"""
Graceful-остановка долгоживущих циклов: и репликатора, и обработчиков.

Перехват процессный, а не персональный. SIGTERM означает «останавливаемся целиком», поэтому флаг
взводится сразу всем живым циклам — в каком бы потоке они ни крутились. Обработчик сигналов ставится
один раз и только из главного потока (python другого не позволяет), а циклы в рабочих потоках просто
регистрируются в общем списке.

Отсюда важное следствие, из-за которого перехват ставится в КОНСТРУКТОРАХ Replicator1C и
HandlerLoop, а не только при запуске цикла. Типовая точка входа отправляет все run_forever в
ThreadPoolExecutor и сама больше ничего не делает — тогда главный поток не создаёт ни одного
StopSignal, из рабочих потоков signal.signal кидает ValueError, и перехват не ставит никто.
Процесс в этом случае просто убивается по SIGTERM (циклы не дорабатывают, незавершённые merge
остаются висеть в реестре), а по SIGINT главный поток получает KeyboardInterrupt и намертво
зависает в ожидании рабочих. Конструкторы же вызываются из главного потока — там и ставим.

Отдельным модулем, потому что пользуются им и replicator, и handlers, а replicator импортирует
handlers — общий код пришлось бы тащить через циклический импорт.
"""

import signal
import weakref

from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)

# WeakSet: закончившийся цикл свой сигнал больше не держит, и тот уходит вместе с ним.
_stop_signals: "weakref.WeakSet[StopSignal]" = weakref.WeakSet()
_handlers_installed = False


def handle_stop_signal(signum, frame) -> None:
    logger.info("Received signal %s, stopping after current cycle", signum)
    for stop in list(_stop_signals):
        stop.requested = True


def install_signal_handlers(*, quiet: bool = True) -> bool:
    """
    Ставит перехват SIGTERM/SIGINT — один раз за процесс. Возвращает, стоит ли он теперь.

    Python разрешает ставить перехват только из главного потока, поэтому попытка из рабочего
    ничего не делает: флаг «установлено» при неудаче не выставляется, и перехват поставит первый
    же вызов из главного потока.

    quiet=False — для вызова из конструктора, который в норме и исполняется главным потоком. Если
    там не вышло, значит весь объект собран в рабочем потоке и graceful-остановки у процесса не
    будет вовсе; молчать об этом нельзя — предупреждаем и подсказываем, что делать.
    """
    global _handlers_installed
    if _handlers_installed:
        return True
    try:
        signal.signal(signal.SIGTERM, handle_stop_signal)
        signal.signal(signal.SIGINT, handle_stop_signal)
    except ValueError:
        if quiet:
            logger.debug("Not the main thread: signal handlers will be installed by the main one")
        else:
            logger.warning(
                "Signal handlers are not installed: this object was created outside the main "
                "thread. SIGTERM will kill the process instead of stopping loops gracefully. "
                "Call cdc_1c.stop_signal.install_signal_handlers() from the main thread, or stop "
                "the loops with request_stop().")
        return False
    _handlers_installed = True
    return True


class StopSignal:
    """Флаг graceful-остановки одного цикла. Взводится общим перехватчиком (весь процесс
    останавливается разом) либо точечно через request_stop() того, кто цикл крутит."""

    def __init__(self):
        self.requested = False
        _stop_signals.add(self)
        install_signal_handlers()

    def wait(self, seconds: float) -> None:
        """Спит до seconds, прерываясь раньше при поступлении сигнала."""
        import time

        deadline = time.monotonic() + seconds
        while not self.requested and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
