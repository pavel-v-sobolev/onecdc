"""
Recorder ответов реальной 1С для оффлайн-тестов (record/replay).

Подключается к ЖИВОМУ серверу 1С и сохраняет ответы в папку конфигурации (tests/responses/<config>/),
которую потом проигрывает tests/fake_1c.py. Метаданные пишутся отдельным ключом (они запрашиваются
один раз за прогон), пакеты изменений — отдельными вызовами с описанием кейса.

Использование:
    # один раз — метаданные:
    uv run python tests/record_1c.py --metadata --yes
    # затем на каждый тест-кейс (между запусками руками кидаешь изменения в 1С):
    uv run python tests/record_1c.py --name data_load_tovary --yes

Стенд берётся из tests/<контур>.env (по умолчанию trade_demo1), ключ --contour выбирает другой.
Ключ --yes обязателен: скрипт перезаписывает записанные ответы В РЕПОЗИТОРИИ тем, что отдаст
указанная 1С.

По умолчанию после записи пакета подтверждает его (NotifyChangesReceived), чтобы очередь обмена
продвинулась и следующий запуск поймал новый пакет. Флаг --no-notify отключает подтверждение
(тогда повторная запись поймает тот же пакет).
"""

import argparse
import json
from pathlib import Path

import requests

from debug_config import contour

# Стенд берём из файла контура (tests/<контур>.env, см. debug_config) — здесь литералов нет
# намеренно: этот скрипт ходит в 1С и ПИШЕТ РЕЗУЛЬТАТ В РЕПОЗИТОРИЙ. Указали не тот контур —
# и в коммит приезжают куски чужих данных, а вычищать их придётся переписыванием истории.
DEFAULT_CONTOUR = "trade_demo1"
DEFAULT_CONFIG_DIR = "tests/responses/trade_demo_8.5"
METADATA_FILE = "msg001_metadata.xml"


def _load_manifest(config_dir: Path) -> dict:
    path = config_dir / "manifest.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"description": config_dir.name, "queue_guid": None, "metadata": None, "batches": []}


def _save_manifest(config_dir: Path, manifest: dict) -> None:
    (config_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _next_index(config_dir: Path) -> int:
    nums = []
    for f in config_dir.glob("msg*"):
        try:
            nums.append(int(f.name[3:6]))
        except ValueError:
            pass
    return (max(nums) + 1) if nums else 1


def _last_received_no(args, odata_auth) -> int:
    resp = requests.get(f"{args.odata_url}/ExchangePlan_{args.exchange}?$format=json",
                        auth=odata_auth, timeout=args.timeout)
    resp.raise_for_status()
    for queue in (resp.json().get("value") or []):
        if queue["Ref_Key"] == args.queue:
            return int(queue["ReceivedNo"])
    return 0


def record_metadata(args, config_dir: Path, odata_auth) -> None:
    resp = requests.get(f"{args.odata_url}/$metadata", auth=odata_auth, timeout=args.timeout)
    resp.raise_for_status()
    (config_dir / METADATA_FILE).write_bytes(resp.content)

    manifest = _load_manifest(config_dir)
    manifest["metadata"] = METADATA_FILE
    manifest["queue_guid"] = args.queue
    manifest.setdefault("batches", [])
    manifest.setdefault("description", config_dir.name)
    _save_manifest(config_dir, manifest)
    print(f"metadata -> {METADATA_FILE} ({len(resp.content)} bytes)")


def record_batch(args, config_dir: Path, odata_auth) -> None:
    message_no = _last_received_no(args, odata_auth) + 1
    url = (f"{args.odata_url}/SelectChanges?DataExchangePoint="
           f"'{args.odata_url}/ExchangePlan_{args.exchange}(guid'{args.queue}')'&MessageNo={message_no}")
    resp = requests.post(url, auth=odata_auth, timeout=args.timeout)
    resp.raise_for_status()

    fname = f"msg{_next_index(config_dir):03d}_{args.name}.xml"
    (config_dir / fname).write_bytes(resp.content)

    manifest = _load_manifest(config_dir)
    manifest.setdefault("queue_guid", args.queue)
    manifest.setdefault("batches", []).append(fname)
    _save_manifest(config_dir, manifest)
    print(f"batch (MessageNo={message_no}) -> {fname} ({len(resp.content)} bytes)")

    if args.no_notify:
        print("notify пропущен (--no-notify): очередь не продвинута")
        return
    notify_url = (f"{args.odata_url}/NotifyChangesReceived?DataExchangePoint="
                  f"'{args.odata_url}/ExchangePlan_{args.exchange}(guid'{args.queue}')'&MessageNo={message_no}")
    notify = requests.post(notify_url, auth=odata_auth, timeout=args.timeout)
    notify.raise_for_status()
    print(f"подтверждено MessageNo={message_no} (очередь продвинута)")


def main():
    parser = argparse.ArgumentParser(description="Запись ответов 1С для replay-тестов")
    parser.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR)
    parser.add_argument("--contour", default=DEFAULT_CONTOUR,
                        help="имя файла стенда: tests/<контур>.env")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--metadata", action="store_true", help="записать $metadata (один раз)")
    parser.add_argument("--name", help="описание кейса для записи пакета SelectChanges")
    parser.add_argument("--no-notify", action="store_true",
                        help="не подтверждать пакет (очередь не продвигается)")
    parser.add_argument("--yes", action="store_true",
                        help="подтвердить запись в репозиторий из указанного контура")
    args = parser.parse_args()

    settings = contour(args.contour)
    if not settings.is_configured:
        parser.error(settings.why_not)
    # Запускать «на посмотреть» этот скрипт не должно получаться: он перезаписывает записанные
    # ответы в репозитории тем, что отдаст УКАЗАННАЯ 1С.
    if not args.yes:
        parser.error(f"перезапишет {args.config_dir} ответами из {settings.odata_url} — "
                     f"подтвердите ключом --yes")
    args.odata_url = settings.odata_url
    args.exchange = settings.exchange_name
    args.queue = settings.queue_guid

    config_dir = Path(args.config_dir)
    config_dir.mkdir(parents=True, exist_ok=True)
    odata_auth = settings.odata_auth

    if args.metadata:
        record_metadata(args, config_dir, odata_auth)
    elif args.name:
        record_batch(args, config_dir, odata_auth)
    else:
        parser.error("нужен либо --metadata, либо --name <описание>")


if __name__ == "__main__":
    main()
