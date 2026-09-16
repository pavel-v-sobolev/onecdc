"""
Потолок размера ответа 1С.

Пакет изменений формирует 1С, и ограничить его со стороны клиента нечем: `$top`, `$select` и
`$filter` она для `SelectChanges` игнорирует, а собирает ответ в памяти рабочего процесса целиком.
Единственное, что в нашей власти, — не принимать ответ, который нас убьёт.

Отказ ничего не чинит: пакет остаётся в очереди 1С, и следующий цикл упрётся в тот же ответ. Он
меняет ТИХУЮ смерть по OOM на внятный отказ — процесс жив, полные выгрузки и обработчики работают,
а в логе стоит размер, потолок и что делать.

Множитель измерен: пакет в 28 МБ давал 102 МБ пика выделений (×3.6) — тело, текст, дерево разбора и
колоночные списки живут одновременно.
"""

import pytest
import requests

from onecdc.common_functions import ResponseTooLargeError, read_within_limit

from conftest import FakeResponseMixin


class _Response(FakeResponseMixin):
    """Ответ, отдающий тело кусками — как настоящий stream=True."""

    def __init__(self, body: bytes, declared: int | None = None, chunk: int = 16):
        self.content = body
        self.text = body.decode()
        self.status_code = 200
        self.ok = True
        self.reason = 'OK'
        self.url = 'http://fake'
        self.headers = {} if declared is None else {'Content-Length': str(declared)}
        self._chunk = chunk
        self.received = 0
        self.closed = False

    def iter_content(self, chunk_size=None):
        for i in range(0, len(self.content), self._chunk):
            piece = self.content[i:i + self._chunk]
            self.received += len(piece)
            yield piece

    def close(self):
        self.closed = True


def test_a_declared_oversize_body_is_not_downloaded_at_all():
    # Content-Length 1С отдаёт (проверено на живой), и тогда отказ бесплатный: тела не касаемся.
    response = _Response(b'x' * 1000, declared=1000)

    with pytest.raises(ResponseTooLargeError, match='was not downloaded at all'):
        read_within_limit(response, 100, 'test')

    assert response.received == 0, 'тело качать не следовало'
    assert response.closed


def test_a_body_without_content_length_is_cut_off_mid_stream():
    # Заголовка может не быть: перед 1С бывает прокси со сжатием или chunked. Тогда границу
    # держит счётчик принятых байт, а соединение обрывается.
    response = _Response(b'x' * 1000)

    with pytest.raises(ResponseTooLargeError, match='exceeded'):
        read_within_limit(response, 100, 'test')

    assert response.received < 1000, 'приняли всё тело — счётчик не сработал'
    assert response.closed


def test_a_body_within_the_limit_arrives_whole():
    response = _Response(b'x' * 1000, declared=1000)

    assert read_within_limit(response, 10_000, 'test') == b'x' * 1000


def test_no_limit_means_no_limit():
    response = _Response(b'x' * 1000, declared=1000)

    assert read_within_limit(response, None, 'test') == b'x' * 1000


def test_a_lying_content_length_does_not_let_a_huge_body_through():
    # Заголовку верим только когда он ПРОТИВ ответа. Занижен — тело всё равно считается.
    response = _Response(b'x' * 1000, declared=10)

    with pytest.raises(ResponseTooLargeError, match='exceeded'):
        read_within_limit(response, 100, 'test')


def test_the_default_limit_is_documented_and_wired_in():
    from onecdc.data_reader import MAX_RESPONSE_BYTES, DataReader
    from onecdc.change_reader import ChangeReader
    from onecdc.metadata_reader import MetadataReader

    assert MAX_RESPONSE_BYTES == 512 * 1024 * 1024
    md = MetadataReader('http://fake')
    assert DataReader('http://fake', md).max_response_bytes == MAX_RESPONSE_BYTES
    assert ChangeReader('http://fake', 'E', 'q', md).max_response_bytes == MAX_RESPONSE_BYTES
    # И его можно снять совсем — например когда памяти заведомо хватает.
    assert DataReader('http://fake', md, max_response_bytes=None).max_response_bytes is None
