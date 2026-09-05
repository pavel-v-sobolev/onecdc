import hashlib

_TRANSLIT_TABLE = str.maketrans({
    'а': 'a',  'б': 'b',  'в': 'v',  'г': 'g',  'д': 'd',
    'е': 'e',  'ё': 'yo', 'ж': 'zh', 'з': 'z',  'и': 'i',
    'й': 'j',  'к': 'k',  'л': 'l',  'м': 'm',  'н': 'n',
    'о': 'o',  'п': 'p',  'р': 'r',  'с': 's',  'т': 't',
    'у': 'u',  'ф': 'f',  'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'sch','ъ': '',   'ы': 'y',  'ь': '',
    'э': 'e',  'ю': 'yu', 'я': 'ya',
    'А': 'A',  'Б': 'B',  'В': 'V',  'Г': 'G',  'Д': 'D',
    'Е': 'E',  'Ё': 'Yo', 'Ж': 'Zh', 'З': 'Z',  'И': 'I',
    'Й': 'J',  'К': 'K',  'Л': 'L',  'М': 'M',  'Н': 'N',
    'О': 'O',  'П': 'P',  'Р': 'R',  'С': 'S',  'Т': 'T',
    'У': 'U',  'Ф': 'F',  'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch',
    'Ш': 'Sh', 'Щ': 'Sch','Ъ': '',   'Ы': 'Y',  'Ь': '',
    'Э': 'E',  'Ю': 'Yu', 'Я': 'Ya',
})


def _translit(s: str) -> str:
    return s.translate(_TRANSLIT_TABLE)


# Лимит длины идентификатора в PostgreSQL (63 байта). Транслитерация даёт ASCII,
# поэтому длину можно считать в символах.
POSTGRES_MAX_IDENTIFIER = 63
HASH_LENGTH = 4

# Служебные имена полей (добавляются при загрузке/сохранении).
# Наше собственное служебное поле сохраняет имя как есть; поле 1С, которое
# транслитерируется в служебное имя, переименовывается (добавляется хэш), чтобы не было конфликта.
RESERVED_FIELD_NAMES = ('merged_on', 'inserted_on', 'exchange_message_no', 'is_deleted_or_empty')


class NameMapper1C:
    """
    Транслитерирует русские имена объектов и полей 1С в латиницу и приводит их
    к ограничениям идентификаторов PostgreSQL (длина, служебные имена).
    Применённые соответствия (оригинал -> результат) сохраняются для отладки.
    """

    def __init__(self):
        self.object_mappings: dict[str, str] = {}
        self.field_mappings: dict[str, str] = {}

    @staticmethod
    def _short_hash(name: str) -> str:
        return hashlib.md5(name.encode('utf-8')).hexdigest()[:HASH_LENGTH]

    @classmethod
    def _append_hash(cls, name: str) -> str:
        # Обрезаем имя при необходимости и добавляем хэш: Имя_Хэш, итог не длиннее лимита.
        suffix = '_' + cls._short_hash(name)
        return name[:POSTGRES_MAX_IDENTIFIER - len(suffix)] + suffix

    @classmethod
    def _fit_length(cls, name: str) -> str:
        if len(name) <= POSTGRES_MAX_IDENTIFIER:
            return name
        return cls._append_hash(name)

    def map_object_name(self, name: str) -> str:
        """
        Имя объекта вида "Document_ЗаказКлиента":
        тип (Document) оставляем без изменений, русскую часть транслитерируем.
        """
        if '_' in name:
            prefix, _, rest = name.partition('_')
            mapped = f'{prefix}_{_translit(rest)}'
        else:
            mapped = _translit(name)

        mapped = self._fit_length(mapped)
        self.object_mappings[name] = mapped
        return mapped

    def map_field_name(self, name: str) -> str:
        if name in RESERVED_FIELD_NAMES:
            # наше служебное поле (добавлено при загрузке) — сохраняем имя как есть
            mapped = name
        else:
            mapped = _translit(name)
            if mapped in RESERVED_FIELD_NAMES:
                # имя поля 1С совпало со служебным — добавляем хэш, чтобы развести
                mapped = self._append_hash(mapped)
            else:
                mapped = self._fit_length(mapped)

        self.field_mappings[name] = mapped
        return mapped

    def get_column_mapping(self, columns: list[str]) -> dict[str, str]:
        return {col: self.map_field_name(col) for col in columns}
