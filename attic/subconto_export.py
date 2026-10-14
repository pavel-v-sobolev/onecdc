"""
АРХИВ. Выгрузка субконто регистра бухгалтерии — удалена из библиотеки, здесь лежит только
как справка. Этот файл не импортируется, не тестируется и работать не обязан.

ПОЧЕМУ УДАЛЕНО. Субконто нет в описании движения: они доступны только через виртуальную таблицу
RecordsWithExtDimensions, а её нельзя листать. Проверено на живой демобазе бухгалтерии
(AccountingRegister_Хозрасчетный, 14.10.2026), с контрольным запросом в каждой серии:

    | Condition                                   | ответ                                  |
    |---------------------------------------------|----------------------------------------|
    | Period eq datetime'…'          (контроль)   | 200, строки есть                       |
    | AccountDr_Key eq guid'…'       (контроль)   | 200, строки есть                       |
    | Recorder eq guid'…'                         | 500                                    |
    | Recorder eq '…'                             | 200 и НОЛЬ строк (регистратор есть!)   |
    | Recorder_Key eq guid'…'                     | 400 — такого поля нет                  |
    | Recorder_Type eq '…'                        | 200, но это ТИП документа, не адрес    |
    | Period eq … and Recorder eq '…'             | 200 и ноль строк — строковое условие   |
    |                                             | отравляет работающее                   |
    | $skip                                       | 400                                    |
    | $orderby и параметр Order                   | 200 и молча проигнорированы            |
    | Top=N                                       | произвольное подмножество, рвёт набор  |

То есть адресовать таблицу по регистратору нельзя, а единственная рабочая адресация —
перечисление ПЕРИОДОВ. Отсюда и приговор: механизм не масштабируется.

  * много документов → много различных периодов → линейный рост числа запросов;
  * один плотный период (пакетное проведение, дата без времени) → один запрос на все строки
    этого периода во всей базе, без возможности разбить: $skip нет, Top рвёт наборы. Упирается
    в max_response_bytes, и уменьшение страницы регистра не помогает — условие не зависит
    от размера нашей страницы.

ЕСЛИ ВОЗВРАЩАТЬ. Нереализованный выход был один: внутри тяжёлого периода дробить вторым
измерением — Condition по AccountDr_Key измеренно работает. Понадобится также перехват
ResponseTooLargeError на пачке из одного периода (его здесь нет).

Удалено вместе с публичным параметром read_subconto у Replicator, ChangeReader и DataReader
и с колонками ExtDimensionsDr/ExtDimensionsCr, которые заводил metadata_reader.

Блоки выдернуты целыми диапазонами строк, поэтому сюда попали и соседи, которые в
библиотеке ОСТАЛИСЬ и к субконто отношения не имеют: NUMERIC_TYPES, MAX_RESPONSE_BYTES,
FUNCTION_RESULT_FIELD, FUNCTION_ELEMENT_FIELD, SUPPORTED_TYPES, KNOWN_ENTITY_TYPES,
METADATA_POSTFIXES, ODATA_PREFIX, TYPE_PREFIX, RECORDER_FIELDS. Они здесь только как
контекст.
"""

# ============ src/onecdc/data_reader.py: константы ============

# субконто нет вообще, и другого источника у них нет: соседняя функция ExtDimensions() не
# принимает ни одного параметра (ни периода, ни Top) — то есть либо вся аналитика базы одним
# ответом, либо ничего.
RECORDS_WITH_EXT_DIMENSIONS = 'RecordsWithExtDimensions'
# Ответ функции — не feed/entry, а <d:Result> со списком <d:element>; поля в элементе лежат
# плоско, как в m:properties.
FUNCTION_RESULT_FIELD = 'd:Result'
FUNCTION_ELEMENT_FIELD = 'd:element'
# Слоты субконто: значение, тип значения и вид субконто — три разных поля на слот.
EXT_DIMENSION_SLOTS = (1, 2, 3)
EXT_DIMENSION_VALUE = 'd:ExtDimension{side}{slot}'
EXT_DIMENSION_VALUE_TYPE = 'd:ExtDimension{side}{slot}_Type'
EXT_DIMENSION_KIND = 'd:ExtDimensionType{side}{slot}_Key'
# Все поля вида субконто разом — по ним ищется план видов характеристик
# (см. _ensure_ext_dimension_kinds).
EXT_DIMENSION_KIND_FIELDS = tuple(EXT_DIMENSION_KIND.format(side=side, slot=slot)
                                  for side in EXT_DIMENSIONS_FIELDS
                                  for slot in EXT_DIMENSION_SLOTS)
# Класс объектов 1С, в котором лежат виды субконто: план видов характеристик.
CHART_OF_CHARACTERISTIC_TYPES = 'ChartOfCharacteristicTypes'
# «Предопределённый» элемент — объявленный в конфигураторе, а не заведённый пользователем;
# PredefinedDataName хранит имя, под которым он там объявлен (`РаботникиОрганизаций`). У элемента,
# созданного пользователем в работающей базе, это поле пустое.
#
# Ключом JSON субконто берём имя, а не Ref_Key, по одной причине: Ref_Key предопределённого
# элемента живёт в ДАННЫХ и у двух независимо развёрнутых баз разный — конфигурация при первом
# запуске создаёт элементы заново, со своими guid'ами. Запрос к витрине, написанный по guid'у,
# на соседней базе перестал бы что-либо находить. Имя приходит из конфигурации и одно на все базы.
PREDEFINED_NAME_FIELD = 'PredefinedDataName'
# Ключи внутри JSON-значения субконто.
EXT_DIMENSION_VALUE_KEY = 'value'
EXT_DIMENSION_TYPE_KEY = 'type'
# Числовые типы 1С. Нужны, чтобы согласовать два ответа платформы об одном и том же движении:
# см. _zero_empty_numerics.
NUMERIC_TYPES = ('Double', 'Int64', 'Int32', 'Int16')

# Потолок размера ответа 1С. Тело загружается целиком, и дальше от него живут ещё несколько
# представлений — текст, дерево разбора, колоночные списки: измерено ×3.6 на пике. Без потолка
# достаточно крупный пакет убивает процесс по OOM молча, а следующий цикл запрашивает его снова.
#
# Предел 512 МиБ ограничивает тело ответа, но не общую память процесса: разбор и
# параллельные выгрузки требуют дополнительного места. Для больших пакетов нужен дроссель: сформировать пакет поменьше 1С со стороны
# клиента не заставить, $top и $filter она для SelectChanges игнорирует.
MAX_RESPONSE_BYTES = 512 * 1024 * 1024
# Бюджет длины сегмента адреса, в который сводятся периоды одного запроса за субконто
# (см. _ext_dimensions_chunk). Параметры функции 1С лежат в пути, и режет их http.sys своим
# UrlSegmentMaxLength (260 символов по умолчанию) — раньше, чем IIS дойдёт до maxUrl или
# maxQueryString. Проверено на реальной 1С: 249 символов проходят, 292 дают 400 «Invalid URL».
EXT_DIMENSIONS_MAX_SEGMENT_CHARS = 260
# Ниже не опускаемся даже при отказах: короче — это уже меньше одного периода.
EXT_DIMENSIONS_MIN_SEGMENT_CHARS = 80

# Приметы отказа «адрес слишком длинный». Штатный код — 414, но IIS на превышение
# UrlSegmentMaxLength отдаёт обычный 400 и html-страницу, поэтому 400 засчитываем только по
# примете в теле: иначе любая другая ошибка 400 молча урезала бы бюджет до самого дна.
URL_TOO_LONG_CODES = (414,)
URL_TOO_LONG_MARKERS = ('Invalid URL', 'URL is invalid', 'Request URL Too Long')


def _is_url_too_long(exc: requests.HTTPError) -> bool:
    response = exc.response
    if response is None:
        return False
    if response.status_code in URL_TOO_LONG_CODES:
        return True
    return (response.status_code == 400
            and any(marker in response.text for marker in URL_TOO_LONG_MARKERS))


# ============ src/onecdc/data_reader.py: методы DataReader ============
#
# Обёртка-пустышка нужна только чтобы файл оставался разбираемым Python: это были методы
# DataReader, а не самостоятельный класс.

class _ArchivedDataReaderMethods:
    def _fill_ext_dimensions_if_enabled(self) -> None:
        """
        Заслонка над _fill_ext_dimensions: проверить опцию read_subconto и обойти прочитанные
        регистры бухгалтерии. Сама работа — в _fill_ext_dimensions, по одному объекту.

        В описании движения (_RecordType) субконто отсутствуют. Они доступны только через
        виртуальную таблицу RecordsWithExtDimensions, поэтому после чтения пакета изменений
        или страницы полной выгрузки нужен дополнительный запрос.

        По умолчанию чтение отключено из-за его стоимости. Виртуальная таблица не поддерживает
        $skip, а Top возвращает произвольное подмножество строк и может разрезать набор регистратора.
        Это проверено на реальной 1С. Для получения полных наборов приходится перечислять периоды:
        число запросов растёт с размером регистра. Нужную аналитику часто проще получить
        из документа-регистратора, чем извлекать из JSON-колонок субконто.

        Метод вызывается из read_changes и read_object, чтобы опция одинаково действовала
        при чтении изменений и полной выгрузке.
        """
        if not self.read_subconto:
            return
        for object_name in list(self.keys()):
            if object_name.startswith(ACCOUNTING_REGISTER_TYPE):
                self._fill_ext_dimensions(object_name)

    def _fill_ext_dimensions(self, object_name: str) -> int:
        """
        Дочитать субконто к разобранным движениям и вернуть число запросов в 1С.

        Вызывается для пакета изменений и страницы полной выгрузки. Периоды движений
        объединяются в запросы к RecordsWithExtDimensions; результат сопоставляется
        по регистратору и номеру строки. Запросы ограничиваются длиной сегмента URL.
        Движение без найденной аналитики получает пустой JSON-объект.
        """
        data_object = self.get(object_name)
        if data_object is None or data_object.data_length == 0:
            return 0
        data = data_object.data
        # Регистратор регистра бухгалтерии 1С всегда отдаёт составным (Recorder + Recorder_Type),
        # но сшивка держится на его наличии — без него молча оставили бы субконто пустыми.
        if 'Period' not in data or 'Recorder' not in data or LINE_NUMBER_FIELD not in data:
            logger.warning('Cannot fill ext dimensions of %s: no Period/Recorder/%s in records',
                           object_name, LINE_NUMBER_FIELD)
            return 0

        # Спрашиваем по РАЗЛИЧНЫМ периодам, а не по движениям: период у проводки один, и все её
        # строки его наследуют, так что пакет из десятков тысяч движений обычно сводится к сотням
        # моментов. Без схлопывания об одном и том же мы спросили бы столько раз, сколько строк.
        # Сортировка нужна и дальше: список режется на пачки с начала, а порядок обхода множества
        # произволен — состав запросов перестал бы быть воспроизводимым.
        unique_periods = sorted({p for p in data['Period'] if p is not None})
        index: dict[tuple, dict] = {}
        requests_made = 0
        # Остаток — о чём ещё не спросили: цикл отрезает израсходованную пачку с начала.
        remaining = unique_periods
        while remaining:
            chunk = self._ext_dimensions_chunk(object_name, remaining)
            try:
                elements = self._request_ext_dimensions(
                    object_name, self._periods_condition(chunk), None)
            except requests.HTTPError as exc:
                # Пачка не влезла в лимит длины адреса, урезанный ниже умолчания. Делим бюджет и
                # повторяем ту же пачку короче (см. _ext_dimensions_chunk).
                if len(chunk) == 1 or not _is_url_too_long(exc):
                    raise
                self._ext_dimensions_segment_limit = max(
                    EXT_DIMENSIONS_MIN_SEGMENT_CHARS, self._ext_dimensions_segment_limit // 2)
                logger.warning('Ext dimensions request of %s was rejected as too long, '
                               'retrying with a %s-character budget',
                               object_name, self._ext_dimensions_segment_limit)
                continue
            self._ensure_ext_dimension_kinds(object_name, elements)
            for element in elements:
                key = self._ext_dimensions_key(element)
                if key is not None:
                    index[key] = self._ext_dimensions_columns(object_name, element)
            remaining = remaining[len(chunk):]
            requests_made += 1

        columns: dict[str, list] = {field: [] for field in EXT_DIMENSIONS_FIELDS.values()}
        for recorder, line_number in zip(data['Recorder'], data[LINE_NUMBER_FIELD]):
            found = index.get((recorder, line_number)) or {}
            for field, column in columns.items():
                column.append(found.get(field, {}))
        data.update(columns)
        logger.info('Filled ext dimensions of %s: %s periods in %s requests',
                    object_name, len(unique_periods), requests_made)
        return requests_made

    @staticmethod
    def _periods_condition(periods: list) -> str:
        """Условие «период — один из перечисленных». `or` внутри Condition платформа понимает."""
        return ' or '.join(f"Period eq {_odata_literal(p, 'DateTime')}" for p in periods)

    def _ext_dimensions_chunk(self, object_name: str, periods: list) -> list:
        """
        Выбрать периоды для одного запроса в пределах бюджета длины сегмента URL.

        Измеряется строка вызова функции до процентного кодирования. Это практический
        бюджет для публикаций с ограничением длины сегмента, включая проверенные варианты IIS.
        После отказа по длине вызывающий уменьшает бюджет и повторяет запрос.
        Первый период включается всегда: если не проходит даже он, нужно вернуть ошибку.
        """
        chunk: list = []
        for period in periods:
            candidate = chunk + [period]
            segment = self._ext_dimensions_segment(self._periods_condition(candidate), None)
            if chunk and len(segment) > self._ext_dimensions_segment_limit:
                break
            chunk = candidate
        return chunk

    @staticmethod
    def _ext_dimensions_segment(condition: str | None, top: int | None) -> str:
        """Сегмент адреса с вызовом функции, до процентного кодирования (см. _ext_dimensions_chunk)."""
        args = []
        if condition:
            args.append('Condition=' + _odata_string_literal(condition))
        if top is not None:
            args.append(f'Top={top}')
        return f"{RECORDS_WITH_EXT_DIMENSIONS}({','.join(args)})"

    def _request_ext_dimensions(self, object_name: str, condition: str | None,
                                top: int | None) -> list:
        """
        Запрос к RecordsWithExtDimensions; возвращает список элементов ответа (d:element).

        У этой таблицы работают не query-опции, а собственные параметры. Проверено на реальной 1С:

        | `$skip`                            | 400                                             |
        | `$orderby` и параметр `Order`      | **200 и молча проигнорированы** (порядок не тот)|
        | `Top`, `Condition`                 | работают                                        |
        | `Condition` по `Period` и `*_Key`  | работают, в том числе через `or`                |
        | `Condition` по `Recorder` как guid | 500                                             |
        | `Condition` по `Recorder` строкой  | **200 и ноль строк**, даже для существующего регистратора               |

        Отсюда и то, что читать её постранично нельзя: `Top` режет по строкам, обрезая набор
        регистратора посередине, и отдаёт произвольное подмножество выборки, а не её начало
        (`Top=5` дал январь 2012, `Top=20` — июнь 2012 … март 2013). Единственная рабочая
        адресация — перечисление периодов, чем и занят _fill_ext_dimensions. Сам регистр читается
        не отсюда, а обычным `$skip` по наборам записей.

        Параметр top остаётся только для проб: код библиотеки передаёт None.
        """
        # Слэш оставляем как есть: имя функции — сегмент пути, а не значение. Закодированный
        # %2F часть веб-серверов (IIS по умолчанию) отвергает, не доводя запрос до 1С.
        path = quote(f"{object_name}/{self._ext_dimensions_segment(condition, top)}",
                     safe="/()=,':")
        url = f'{self.odata_url}/{path}'
        text_body = self._get_within_limit(url, f'read {path}')
        result = parse_odata(text_body, FUNCTION_RESULT_FIELD, f'read {path}',
                             force_list=(FUNCTION_ELEMENT_FIELD,))
        return (result or {}).get(FUNCTION_ELEMENT_FIELD) or []

    def _ext_dimensions_key(self, element: dict) -> tuple | None:
        """(регистратор, номер строки) элемента виртуальной таблицы — ключ сшивки с движением."""
        recorder = element.get('d:Recorder')
        line_number = element.get('d:' + LINE_NUMBER_FIELD)
        if not isinstance(recorder, str) or not isinstance(line_number, str):
            return None
        return self._convert_value(recorder, 'Guid'), self._convert_value(line_number, 'Int64')

    def _ensure_ext_dimension_kinds(self, object_name: str, elements: list) -> None:
        """
        Построить карту «guid вида субконто → его предопределённое имя» для этого регистра.

        Зачем она. Движение называет свой вид субконто guid'ом: в поле
        `ExtDimensionTypeDr1_Key` лежит Ref_Key элемента плана видов характеристик. Ключом JSON
        нужно стабильное имя — почему именно оно, а не guid, см. PREDEFINED_NAME_FIELD. Карта и
        переводит одно в другое.

        Какой план видов характеристик спрашивать, `$metadata` регистра не сообщает, поэтому план
        ищется отбором по первому встреченному виду (_find_ext_dimension_chart), а затем читается
        целиком. Карта кэшируется на объект и на экземпляр ридера; страница без видов поиск
        откладывает — искать нечем. У пользовательского вида предопределённого имени нет, и
        ключом у него остаётся guid.
        """
        if object_name in self._ext_dimension_kinds:
            return
        sample = next((value for element in elements
                       for value in map(element.get, EXT_DIMENSION_KIND_FIELDS)
                       if isinstance(value, str)), None)
        if sample is None:
            return

        chart_name = self._find_ext_dimension_chart(sample)
        if chart_name is None:
            # Ключами останутся Guid — это рабочий вариант (вид субконто join-ится к ПВХ), просто
            # менее читаемый. Молчать нельзя: разница видна в данных, и объяснить её должно тут.
            logger.warning('Ext dimension kinds of %s are not resolvable: no chart of '
                           'characteristic types holds %s — JSON keys stay GUIDs',
                           object_name, sample)
            self._ext_dimension_kinds[object_name] = {}
            return

        names = self._read_ext_dimension_kinds(chart_name)
        logger.info('Ext dimension kinds of %s: %s names from %s',
                    object_name, len(names), chart_name)
        self._ext_dimension_kinds[object_name] = names

    def _find_ext_dimension_chart(self, kind_key: str) -> str | None:
        """
        Какой план видов характеристик содержит этот вид субконто. None — ни один.

        Спрашиваем отбором, а не прямым адресом. Прямой адрес отвечает на «нет такого» кодом 404,
        и это была ловушка: тот же 404 отдаёт веб-сервер перед 1С, а отличить их можно только
        по телу — которое IIS подменяет. Измерено на демо бухгалтерии: 404 от
        прямого адреса приходит с html-страницей IIS, распознать его как ответ 1С нельзя, и поиск
        падал на первом же плане, который ответил «нет». Работало это лишь по случайности —
        нужный план стоял в алфавитном порядке первым из двенадцати.

        У отбора такого вопроса не возникает: 200 и пустая коллекция (проверено там же).

        План, ответивший ошибкой (403 — нет прав на чужой план учёта), пропускаем с
        предупреждением. Отказ в правах на один план не повод ронять чтение всего регистра: не
        найдём нужный — останемся с GUID-ключами, а это описанный штатный режим.
        """
        for chart_name in self.metadata:
            if not chart_name.startswith(CHART_OF_CHARACTERISTIC_TYPES):
                continue
            query = f"?$filter=Ref_Key eq guid'{kind_key}'&$select=Ref_Key&$top=1"
            url = f'{self.odata_url}/{quote(chart_name)}{query}'
            response = requests.get(url, auth=self.odata_auth,
                                    timeout=resolve_timeout(self.request_timeout))
            if not response.ok:
                logger.warning('Cannot look for ext dimension kinds in %s: %s %s — skipping this '
                               'chart', chart_name, response.status_code, response.reason)
                continue
            entries = (parse_odata(response.text, 'feed', f'read {chart_name}{query}',
                                   force_list=('entry',)) or {}).get('entry') or []
            if entries:
                return chart_name
        return None

    def _read_ext_dimension_kinds(self, chart_name: str) -> dict[str, str]:
        """
        Прочитать план видов характеристик и вернуть карту «guid элемента → его предопределённое
        имя» (см. PREDEFINED_NAME_FIELD).

        План читается целиком: в проверенной демобазе бухгалтерии он содержал 50 элементов.
        Элементы без предопределённого имени в карту не включаются; для таких пользовательских
        видов субконто ключом остаётся GUID.
        """
        query = f'?$select=Ref_Key,{PREDEFINED_NAME_FIELD}'
        response = requests.get(f'{self.odata_url}/{quote(chart_name)}{query}',
                                auth=self.odata_auth,
                                timeout=resolve_timeout(self.request_timeout))
        raise_for_status(response, f'read {chart_name}{query}')
        entries = (parse_odata(response.text, 'feed', f'read {chart_name}',
                               force_list=('entry',)) or {}).get('entry') or []
        names = {}
        for entry in entries:
            properties = (entry.get('content') or {}).get('m:properties') or {}
            ref_key = properties.get('d:Ref_Key')
            name = properties.get('d:' + PREDEFINED_NAME_FIELD)
            if isinstance(ref_key, str) and isinstance(name, str) and name:
                names[ref_key] = name
        return names

    def _ext_dimensions_columns(self, object_name: str, element: dict) -> dict[str, dict]:
        """
        Значения колонок ExtDimensionsDr/ExtDimensionsCr для ОДНОГО движения: плоские слоты
        строки виртуальной таблицы сворачиваются в {колонка: {вид субконто: {"value", "type"}}}.

        Парная к _ext_dimensions_key: от одного и того же элемента та берёт ключ сшивки, эта —
        содержимое двух его jsonb-колонок.

        Ключ — вид субконто, а не номер слота: номер смысла не имеет (субконто1 счёта 10 это
        Номенклатура, счёта 60 — Контрагенты). Именем вида служит его предопределённое имя из
        плана видов характеристик (`РаботникиОрганизаций`), потому что читать и писать в запросах
        предстоит именно его; вид, которого в карте нет, остаётся Guid — см.
        _ensure_ext_dimension_kinds.

        Слот без вида или без значения пропускается: назвать такое субконто нечем. Тип значения
        храним рядом со значением — субконто бывает и не ссылкой (в демо-базе бухгалтерии
        встречаются значения перечислений, `СтавкиНДС` = `НДС18`), и тогда это единственный способ
        понять, что лежит в value.
        """
        names = self._ext_dimension_kinds.get(object_name) or {}
        columns: dict[str, dict] = {}
        for side, field in EXT_DIMENSIONS_FIELDS.items():
            found: dict[str, dict] = {}
            for slot in EXT_DIMENSION_SLOTS:
                kind = element.get(EXT_DIMENSION_KIND.format(side=side, slot=slot))
                value = element.get(EXT_DIMENSION_VALUE.format(side=side, slot=slot))
                if not isinstance(kind, str) or not isinstance(value, str):
                    continue
                key = names.get(kind, kind)
                found[key] = {EXT_DIMENSION_VALUE_KEY: value}
                value_type = element.get(EXT_DIMENSION_VALUE_TYPE.format(side=side, slot=slot))
                if isinstance(value_type, str):
                    found[key][EXT_DIMENSION_TYPE_KEY] = (value_type.removeprefix(ODATA_PREFIX)
                                                          .removeprefix(EDM_TYPE_PREFIX))
            columns[field] = found
        return columns


# ============ src/onecdc/metadata_reader.py: колонки субконто ============

# План видов характеристик: метаданные читаем, объект не сохраняем. Читаем потому, что по нему
# регистр бухгалтерии определяет виды субконто (DataReader._find_ext_dimension_chart перебирает
# планы в метаданных) — без этого ключом субконто остался бы голый Guid.
METADATA_ONLY_TYPES = ('ChartOfCharacteristicTypes',)
# Классы, которые мы умеем сохранять. Всё остальное, придя в пакете изменений, будет потеряно
# (пакет подтверждается целиком), поэтому такие объекты логируются отдельно — см. read_data_entries.
SUPPORTED_TYPES = REGISTER_TYPES + ENTITY_TYPES
# Классы, для которых строятся метаданные. Шире SUPPORTED_TYPES ровно на METADATA_ONLY_TYPES.
KNOWN_ENTITY_TYPES = ENTITY_TYPES + METADATA_ONLY_TYPES
METADATA_POSTFIXES = ('_RecordType','_RowType','_Balance','_Turnover','_BalanceAndTurnover')
ODATA_PREFIX = 'StandardODATA.'
TYPE_PREFIX = 'Edm.'

# Поля регистратора в OData: Recorder (+Recorder_Type), если регистратором может быть несколько
# типов документов, и Recorder_Key (Guid, без Recorder_Type), если тип регистратора единственный.
RECORDER_FIELDS = ('Recorder', 'Recorder_Key', 'Recorder_Type')

# --- Субконто регистра бухгалтерии ---
# Колонки, которых в $metadata нет: в описании движения (_RowType) субконто отсутствуют,
# они живут только в виртуальной таблице RecordsWithExtDimensions и собираются оттуда
# (см. DataReader). Ключ JSON — предопределённое имя вида субконто или его GUID,
# значение — {"value", "type"}.
#
# Почему JSON, а не колонки: 1С отдаёт субконто слотами (ExtDimensionDr1..3), а номер слота смысла
# не имеет — субконто1 счёта 10 это Номенклатура, счёта 60 Контрагенты. Витрина, написавшая
# `ExtDimensionDr1 = …`, была бы права ровно до следующего счёта, причём молча.
ACCOUNTING_REGISTER_TYPE = 'AccountingRegister'
EXT_DIMENSIONS_TYPE = 'ExtDimensions'
# Сторона проводки -> имя колонки.
EXT_DIMENSIONS_FIELDS = {'Dr': 'ExtDimensionsDr', 'Cr': 'ExtDimensionsCr'}

# В _read_metadata_item (ветка регистра бухгалтерии) колонки заводились так:
#
#                if item_name.startswith(ACCOUNTING_REGISTER_TYPE):
#                    # Колонки субконто заводим после классификации: в 1С это измерения, но
#                    # программно ими не пользуются (внутри JSON), и в dimensions им делать нечего.
#                    properties.update({f: EXT_DIMENSIONS_TYPE
#                                       for f in EXT_DIMENSIONS_FIELDS.values()})
