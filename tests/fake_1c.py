"""
Фейковый сервер 1С для оффлайн-тестов (record/replay).

Проигрывает записанные ответы 1С из папки конфигурации, чтобы полный цикл Replicator можно было
гонять без живого сервера 1С (например, на GitHub CI). Записываются только $metadata и пакеты
SelectChanges; состояние обмена (ExchangePlan/ReceivedNo) сервер синтезирует сам, держа счётчик
received_no и инкрементируя его на NotifyChangesReceived.

Формат папки конфигурации (см. tests/responses/<config>/):
    manifest.json  — {"queue_guid", "metadata", "batches": [...], "description"}
    msg001_metadata.xml, msg002_<descr>.xml, ...

batches — упорядоченный список; индекс i соответствует MessageNo = i+1 (сервер нормализует
нумерацию, абсолютные номера исходной записи не важны).

Использование:
  - в тестах: `with running_server(config_dir) as (odata_url, fake): ...`;
  - вручную:  `python tests/fake_1c.py tests/responses/trade_demo_8.5 --port 8080`
              (затем натравить на него реальный Replicator.run_forever).
"""

import argparse
import contextlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

THIS_NODE_GUID = "00000000-0000-0000-0000-000000000000"

EMPTY_FEED = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
              b'<feed xmlns="http://www.w3.org/2005/Atom"></feed>')


class Fake1C:
    """Загруженная конфигурация записанных ответов + состояние обмена."""

    def __init__(self, config_dir):
        self.config_dir = Path(config_dir)
        manifest = json.loads((self.config_dir / "manifest.json").read_text(encoding="utf-8"))
        self.queue_guid = manifest["queue_guid"]
        self.metadata_bytes = (self.config_dir / manifest["metadata"]).read_bytes()
        self.batches = [(self.config_dir / name).read_bytes() for name in manifest["batches"]]
        self.received_no = 0  # синтезированное состояние очереди обмена

    def exchange_plan_json(self) -> bytes:
        payload = {"value": [
            # ЭтотУзел (сама база) в плане обмена есть всегда — подписаться на него нельзя.
            {"Ref_Key": THIS_NODE_GUID, "Code": "БАЗА", "Description": "Эта база",
             "ThisNode": True, "ReceivedNo": "0"},
            {"Ref_Key": self.queue_guid, "Code": "CDC", "Description": "Витрина",
             "ThisNode": False, "ReceivedNo": str(self.received_no)},
        ]}
        return json.dumps(payload).encode("utf-8")

    def select_changes(self, message_no: int) -> bytes:
        idx = message_no - 1
        if 0 <= idx < len(self.batches):
            return self.batches[idx]
        return EMPTY_FEED  # пакетов больше нет — пустой feed

    def notify(self, message_no: int) -> None:
        self.received_no = message_no


def _make_handler(fake: Fake1C):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # не шумим в stderr
            pass

        def _send(self, body: bytes, content_type: str, status: int = 200):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _message_no(self) -> int:
            qs = parse_qs(urlparse(self.path).query)
            return int(qs.get("MessageNo", ["0"])[0])

        def do_GET(self):
            if "$metadata" in self.path:
                self._send(fake.metadata_bytes, "application/xml;charset=utf-8")
            elif "ExchangePlan_" in self.path:
                self._send(fake.exchange_plan_json(), "application/json;charset=utf-8")
            else:
                self.send_error(404)

        def do_POST(self):
            if "SelectChanges" in self.path:
                self._send(fake.select_changes(self._message_no()), "application/xml;charset=utf-8")
            elif "NotifyChangesReceived" in self.path:
                fake.notify(self._message_no())
                self._send(b"", "application/xml;charset=utf-8")
            else:
                self.send_error(404)

    return Handler


@contextlib.contextmanager
def running_server(config_dir, port: int = 0):
    """Поднимает фейковый сервер в фоне на 127.0.0.1:port (0 — эфемерный). Отдаёт (odata_url, fake)."""
    fake = Fake1C(config_dir)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(fake))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    odata_url = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        yield odata_url, fake
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description="Фейковый сервер 1С (replay записанных ответов)")
    parser.add_argument("config_dir", help="папка с manifest.json и msg*.xml")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    fake = Fake1C(args.config_dir)
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), _make_handler(fake))
    print(f"Fake 1C: http://127.0.0.1:{args.port}  "
          f"(config={args.config_dir}, queue={fake.queue_guid}, batches={len(fake.batches)})")
    print("Ctrl+C для остановки")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
