import requests

import xmltodict

from cdc_1c.data_reader import DataReader1C
from cdc_1c.metadata_reader import ACCOUNTING_REGISTER_TYPE, MetadataReader1C, resolve_timeout
from cdc_1c.common_functions import format_bytes, raise_for_status
from cdc_1c.logging_config import get_logger

logger = get_logger(__name__)


class ChangeReader1C(DataReader1C):
    def __init__(self, odata_url: str, exchange_name: str, queue_guid: str,
                 metadata: MetadataReader1C, odata_auth: tuple[str, str] | None = None,
                 request_timeout: float | None = None):
        super().__init__(odata_url, metadata, odata_auth, request_timeout)
        self.exchange_name = exchange_name
        self.queue_guid = queue_guid
        self.message_no = 0

    def read_changes(self):
        # Узел не задан — дальше идти бессмысленно: подсказываем, из чего выбирать.
        if not self.queue_guid:
            self._raise_no_queue_guid()
        # Сбрасываем накопленные данные предыдущего цикла (важно для run_forever).
        self.clear()
        self.message_no = self.get_last_received_no()+1
        self.exchange_message_no = self.message_no

        logger.info(f"Reading changes from 1C (message {self.message_no})")

        url = f"{self.odata_url}/SelectChanges?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"

        response = requests.post(url,auth=self.odata_auth,timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'SelectChanges (message {self.message_no})')
        self.last_response_bytes = len(response.content)

        change_data = xmltodict.parse(response.text,force_list=('d:element','entry'))
        change_entries = (change_data.get('feed') or {}).get('entry') or []

        parsed = self.read_data_entries(change_entries)
        # Пакет приносит набор записей регистра бухгалтерии БЕЗ субконто (их нет в описании
        # движения вовсе) — дочитываем их отдельным запросом по периодам пакета, иначе колонки
        # приехали бы пустыми и затёрли аналитику, полученную полной выгрузкой.
        for object_name in list(self.keys()):
            if object_name.startswith(ACCOUNTING_REGISTER_TYPE):
                self.fill_ext_dimensions(object_name)
        # Одна строка на пакет: что пришло, сколько строк и сколько весил ответ. Раньше лог писался
        # на каждую entry, и один пакет давал сотни одинаковых строк.
        logger.info("Read changes (message %s): %s entries, %s rows, %s%s",
                    self.message_no, len(change_entries), self.rows_read(),
                    format_bytes(self.last_response_bytes),
                    ''.join(f'\n    {name}: {n} entries' for name, n in parsed.items()))

    def notify_changes_received(self):
        """
        Подтвердить получение изменений, отправив запрос на сервер
        """
        url = f"{self.odata_url}/NotifyChangesReceived?DataExchangePoint='{self.odata_url}/ExchangePlan_{self.exchange_name}(guid'{self.queue_guid}')'&MessageNo={self.message_no}"
        response = requests.post(url,auth=self.odata_auth,timeout=resolve_timeout(self.request_timeout))
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
                             self.exchange_name,
                             ''.join(f"\n    {node.get('Ref_Key')}  {node.get('Code') or ''}"
                                     f"  {node.get('Description') or ''}".rstrip()
                                     for node in nodes))
            else:
                logger.error("queue_guid is not set, and exchange plan %s has no nodes besides "
                             "ThisNode: create a node for this replication in 1C",
                             self.exchange_name)
        raise ValueError(f"queue_guid is not set (exchange plan {self.exchange_name}): "
                         "specify the Ref_Key of the exchange node")

    def get_last_received_no(self)->int:
        """
        Получить номер последнего пакета обмена, который был получен и подтвержден
        """
        queues = self.read_nodes()
        receive_no = 0
        found = False

        for queue in queues:
            if self.queue_guid == queue['Ref_Key']:
                receive_no = int(queue['ReceivedNo'])
                found = True

        if not found:
            # Очередь по guid не нашлась — вернём 0 (запросится пакет №1), но это почти наверняка
            # неверный queue_guid или план обмена: без предупреждения ошибку конфигурации не видно.
            logger.warning("Exchange queue %s not found in plan %s (check queue_guid/exchange_name)",
                           self.queue_guid, self.exchange_name)

        return receive_no
    

