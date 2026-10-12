# Ручное пересоздание окружения разработчика: удаляет .venv и uv.lock,
# затем устанавливает последние допустимые версии зависимостей. Не используется в CI.

rm -rf .venv 
rm uv.lock
#rm .python-version
uv venv --python python3.14
uv sync --extra dev --editable --upgrade
