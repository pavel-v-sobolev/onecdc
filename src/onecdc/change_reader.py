"""
Чтение изменений узла обмена 1С: SelectChanges и NotifyChangesReceived.

Наследник DataReader: страницы разбираются тем же кодом, добавлены очередь (узел плана обмена) и
подтверждение пакета. Подтверждение необратимо — оно удаляет регистрации изменений в самой 1С,
поэтому зовётся только после успешной записи в БД.
"""

import requests

import xmltodict

from onecdc.data_reader import MAX_RESPONSE_BYTES, DataReader
from onecdc.metadata_reader import MetadataReader, resolve_timeout
from onecdc.common_functions import (canonical_guid, check_queue_guid, format_bytes, parse_odata,
                                     raise_for_status, read_within_limit)
from onecdc.logging_config import get_logger

logger = get_logger(__name__)

# Таймаут подтверждения пакета: (connect, read). Намеренно свой, а не общий DEFAULT_REQUEST_TIMEOUT
# — см. notify_changes_received. Должен быть заведомо меньше LEASE_ROLE_TTL: запрос не вправе
# пережить аренду узла обмена.
NOTIFY_TIMEOUT: tuple[float, float] = (30, 30)


class ChangeReader(DataReader):
    """
    Очередь изменений одного узла плана обмена.

    read_changes() забирает пакет, notify_changes_received() его подтверждает — и это
    единственный необратимый шаг во всей библиотеке: регистрации изменений после него в 1С
    больше нет. Поэтому подтверждение отделено от чтения и зовётся только после записи в БД.
    """

    def __init__(self, odata_url: str, exchange_name: str, queue_guid: str,
                 metadata: MetadataReader, odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None, read_subconto: bool = False,
                 max_response_bytes: int | None = MAX_RESPONSE_BYTES):
        super().__init__(odata_url, metadata, odata_auth, request_timeout,
                         read_subconto=read_subconto, max_response_bytes=max_response_bytes)
        self.exchange_name = exchange_name
        # Канонизируем здесь, а не только в конструкторе репликатора: ридер создают и напрямую,
        # и тогда guid из формы 1С (верхний регистр) не совпал бы с Ref_Key при сравнении.
        self.queue_guid = check_queue_guid(queue_guid)
        self.message_no = 0

    def read_changes(self):
        # Узел не задан — дальше идти бессмысленно: подсказываем, из чего выбирать.
        if not self.queue_guid:
            self._raise_no_queue_guid()
        # Сбрасываем накопленные данные предыдущего цикла (важно для run_forever).
        self.clear()
        self.message_no = self.get_last_received_no()+1
        self.exchange_message_no = self.message_no
        self.entries_read = 0

        logger.info(f"Reading changes from 1C (message {self.message_no})")

        url = f"{self.odata_url}/SelectChanges?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"

        context = f'SelectChanges (message {self.message_no})'
        # Потоково и с потолком: пакет формирует 1С, и ограничить его со стороны клиента нечем —
        # $top/$filter она здесь игнорирует. Единственное, что в нашей власти, — не принимать
        # ответ, который нас убьёт (см. read_within_limit и MAX_RESPONSE_BYTES).
        with requests.post(url, auth=self.odata_auth, stream=True,
                           timeout=resolve_timeout(self.request_timeout)) as response:
            if not response.ok:
                raise_for_status(response, context)
            body = read_within_limit(response, self.max_response_bytes, context)
            encoding = response.encoding or 'utf-8'
        self.last_response_bytes = len(body)

        feed = parse_odata(body.decode(encoding, errors='replace'), 'feed', context,
                           force_list=('d:element', 'entry'))
        change_entries = (feed or {}).get('entry') or []

        # Сколько entry было В ОТВЕТЕ — отдельно от того, сколько объектов удалось разобрать.
        # Различать обязательно: пакет из одних неподдерживаемых классов даёт ноль объектов, и по
        # их числу он неотличим от пустого пакета. А трактовки у этих двух случаев противоположные
        # (см. Replicator.run_once): пустой не подтверждаем, непустой — обязаны.
        self.entries_read = len(change_entries)
        parsed = self.read_data_entries(change_entries)
        # Пакет приносит набор записей регистра бухгалтерии БЕЗ субконто (их нет в описании
        # движения вовсе) — дочитываем их отдельным запросом по периодам пакета, если чтение
        # субконто включено (см. DataReader._fill_subconto).
        self._fill_subconto()
        # Одна строка на пакет: что пришло, сколько строк и сколько весил ответ. Раньше лог писался
        # на каждую entry, и один пакет давал сотни одинаковых строк.
        logger.info("Read changes (message %s): %s entries, %s rows, %s%s",
                    self.message_no, len(change_entries), self.rows_read(),
                    format_bytes(self.last_response_bytes),
                    ''.join(f'\n    {name}: {n} entries' for name, n in parsed.items()))

    def notify_changes_received(self):
        """
        Подтвердить получение изменений, отправив запрос на сервер.

        Таймаут здесь СВОЙ и короткий, а не общий (NOTIFY_TIMEOUT). Общий рассчитан на чтение
        пакета — 15 минут на ответ, потому что пакет 1С формирует долго. Для подтверждения это
        опасно: аренда узла обмена живёт LEASE_ROLE_TTL, и запрос, провисевший дольше, применится
        уже тогда, когда узлом владеет другой процесс. Причём **без всякого замирания** — просто
        от медленного ответа. Подтверждению читать нечего, оно быстрое, поэтому короткий таймаут
        ничего не ломает и убирает целый класс расхождений.
        """
        url = f"{self.odata_url}/NotifyChangesReceived?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"
        response = requests.post(url, auth=self.odata_auth, timeout=NOTIFY_TIMEOUT)
        # Не-2xx -> HTTPError. Подтверждение не прошло — изменения не списаны и придут снова
        # (в run_forever цикл повторится, save идемпотентен).
        raise_for_status(response, f'NotifyChangesReceived (message {self.message_no})')
        logger.info(f"Changes confirmed for queue {self.queue_guid} (message {self.message_no})")


    def read_nodes(self) -> list[dict]:
        """
        Все узлы плана обмена (как их отдаёт OData), включая ЭтотУзел.
        """
        url = f"{self.odata_url}/ExchangePlan_{self.exchange_name}?$format=json"
        response = requests.get(url,auth=self.odata_auth,timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'ExchangePlan_{self.exchange_name}')
        return response.json().get('value') or []

    def available_nodes(self) -> list[dict]:
        """
        Узлы, которые можно указать в queue_guid: всё, кроме ЭтотУзел (ThisNode) — он описывает
        саму базу-источник, подписаться на его изменения нельзя.
        """
        return [node for node in self.read_nodes() if not node.get('ThisNode')]

    @staticmethod
    def _nodes_listing(nodes) -> str:
        """Узлы столбиком — из лога guid можно скопировать прямо в конфигурацию."""
        return ''.join(f"\n    {node.get('Ref_Key')}  {node.get('Code') or ''}"
                       f"  {node.get('Description') or ''}".rstrip()
                       for node in nodes if not node.get('ThisNode'))

    def _raise_no_queue_guid(self):
        """
        queue_guid не задан: выводим в лог список узлов плана обмена, чтобы guid можно было взять
        прямо отсюда, не открывая конфигуратор и не имея обработки в 1С.
        """
        try:
            nodes = self.available_nodes()
        except Exception as error:
            # Список не получили (нет связи / неверный exchange_name) — сообщение об этом не должно
            # подменять собой главную ошибку: она всё равно ниже.
            logger.warning("Could not list nodes of exchange plan %s: %s", self.exchange_name, error)
        else:
            if nodes:
                logger.error("queue_guid is not set. Available nodes of exchange plan %s:%s",
                             self.exchange_name, self._nodes_listing(nodes))
            else:
                logger.error("queue_guid is not set, and exchange plan %s has no nodes besides "
                             "ThisNode: create a node for this replication in 1C",
                             self.exchange_name)
        raise ValueError(f"queue_guid is not set (exchange plan {self.exchange_name}): "
                         "specify the Ref_Key of the exchange node")

    def _raise_bad_queue_guid(self, reason: str, nodes):
        """Узел указан, но не тот. Список печатаем так же, как для незаданного guid."""
        logger.error("queue_guid %s %s. Available nodes of exchange plan %s:%s",
                     self.queue_guid, reason, self.exchange_name, self._nodes_listing(nodes))
        raise ValueError(f"queue_guid {self.queue_guid} {reason} "
                         f"(exchange plan {self.exchange_name})")

    def get_last_received_no(self) -> int:
        """
        Номер последнего пакета обмена, который мы получили и подтвердили. Следующий цикл просит
        этот номер плюс один, и так КАЖДЫЙ раз — значит цена ошибки здесь не разовая.

        Узел не нашёлся — это ошибка, а не повод продолжить с нуля. Раньше здесь было
        предупреждение и `0`, то есть цикл начинал просить `MessageNo=1` вечно. Молчаливым
        простоем это не заканчивается: `SelectChanges` со старым номером 1С понимает не как
        «повтори пакет», а как «отдай всё, что зарегистрировано с тех пор» (измерено на живой
        1С, см. audit_status), и подтверждение снимает регистрацию. То есть обмен продолжал бы
        идти, счётчики узла стояли бы на единице, и ошибку конфигурации не было бы видно вообще.

        Сравниваем канонически: 1С отдаёт Ref_Key в нижнем регистре, а в конфигурацию его
        копируют как придётся.
        """
        nodes = self.read_nodes()
        for node in nodes:
            if canonical_guid(node.get('Ref_Key')) != self.queue_guid:
                continue
            if node.get('ThisNode'):
                # Узел самой базы-источника. Он в списке есть всегда и раньше находился наравне
                # с остальными: номер брался, SelectChanges уходил по нему, и что ответит 1С,
                # зависело от платформы, а не от нас.
                self._raise_bad_queue_guid('is ThisNode, the source database itself; a receiving '
                                           'node is needed', nodes)
            return int(node['ReceivedNo'])
        self._raise_bad_queue_guid('is not a node of this exchange plan (check queue_guid and '
                                   'exchange_name)', nodes)
    

