# Создание схем для для партиционировнаия
Для себя можно ответить на следующие вопросы:
- Таблица достаточно большая или будет большая.
- Ясно по какому полю можно можно ее партиционировать.
- Таблица будет партиционироваться по колонке типа data или timestamp.
- Поле учавстрвует постоянно в выборке.s


Если все да, то тогда можно приступать к партиционированию. На начальном этапе оптимальная схема будет не определена и предложить в этой ситуации оптимальную схему партиционирования не предстовляется возможным, то тогда просто создаем партицию по умолчанию. Например:
``` sql
CREATE TABLE test_table(
    opened_date DATE NOT NULL,
    barcode VARCHAR(100) NOT NULL,
    created_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
)
WITH (
    appendonly=true,
    compresslevel=1,
    orientation=column,
    compresstype=zstd
)
DISTRIBUTED BY (barcode)
PARTITION BY RANGE (opened_date) (DEFAULT PARTITION other);
```
Это гарантирует, что данные не потеряются и будут втавляться в полном объеме в ваши таблицы.
Общая рекомендация. Стараться избегать пустых партиций.

# Построение схем партиционирования
Все операции схем партиционирования, построены от текущей даты времени (now()). При создании и объединения партиций есть особенности. При месячной, недельной и годовой грянулярности, начало периода попадает на первый день указанного периода. К примеру недельная партиция всегда должна начинаться в понедельник, месячная строго первого числа и т.д.

Над таблицами спроектированы менеждеры операции над партициями это:
- create_partitions - создание партиции из партиции по умолчанию. Порядок выполнения 1.
- merge_partitions - объединение партиций. Порядок выполнения 2.
- move_partitions - перемещение партиций между табличными пространствами ('pg_default', 'warm'). Порядок выполнения 3.
- unload_to_s3_partitions - перенесение части таблицы на s3 бакет. Порядок выполнения 4. Нельзя переносить пустые партиции!
- delete_partitions -  удаление партиции. Порядок выполнения 5.

Каждый менеджер выполнен в виде хранимой процедуры, которая производит последовательность действий заложеных по выбранному алгоритму на выбранном промежутке времени. В названии хранимых процедур есть префиксы fn_part_tools_ и заканчивается на partitions. К примеру хранимая процедура fn_part_tools_merge_partitions - выполняет объединение партиций на своем промежутке.

Хранимые процедуры вызывают оператроры, которые и выполняют единичные операции по перемещению данных и т.д. Их названия начинаются fn_part_tools_ и на заканчиваются на _operation. К примеру fn_part_tools_move_operation переносит одну определенную партицию на определенное табличное пространство.
Некоторые менеждеры выполняют взаимоисключаемые операции. К примеру создание и удаление партиций на промежутке времени. При создании схемы для Airflow процессов вызывается валидатор схемы, который и производит валидацию схемы описанную в yaml файле, для каждой таблицы. Если валидация не происходит, то процесс не выполняется для этой таблицы.

Проверки описаны в хранимой процедуре fn_part_tools_check_config и производится верификация по следующим критериями:
- Проверка на delete_partitions. Промежуток не должен пересекаться ни с одним интервалом.
- Должен присутствовать хотябы один create_partitions.
- Проверка на delete_partitions и unload_to_s3_partitions не совместимые операции. (Так как есть проблемы с удалением внешних таблиц)
- Проверка на delete_partitions и unload_to_s3_partitions не должны пересекаться с move_partitions.
- Пересечение merge_partitions
- Проверка на недопустимости merge_partitions и create_partitions с limit_operations > 0


# Описание схем менеждеров для управленя партициями.
Таблица к которой применются менеджеры, долна иметь хотябы одну партицию. Если нет, тогда менеждеры не будут работать.
Менеджеры использую параметры. Разрешенные параметры (ключ -> значение) для каждого менеджера просьба уточнять в хранимой и в документации ниже. Список возможных ключей:
- lower_bound - нижняя гарица действия менеджера от текущей даты (чем больше, тем раньше). Тип интервал.
- upper_bound - верхняя гарица действия менеджера от текущей даты (чем больше, тем раньше). Тип интервал.
- table_space - табличное пространсто.
- limit_operations - количество выполняемых операций. К примеру переносят 2 партиции на s3 за один запукс.
- ddl_with_param - параметр создания партиции с указанием, ориентации и уровня сжатия.
- access_exclusive_mode - при перенесении данных таблица блокируется согласно access exclusive. Стараться использовать парамет как access_exclusive_mode = false или не указывать вовсе. При true не допускается операции чтения и записи, пока партиция не будет полность перенесена.
- s3_server_name - название s3 сервера, где будет хнариться партиция. s3srv определен по умолчанию. Название сервера нужно уточнять у администраторов БД.
- s3_bucket - название бакета, куда будут выгружаться данные. По умолчанию d-dp-partition. Название бакета просьба уточнять у администраторов S3.

К примеру fn_part_tools_unload_to_s3_partitions имеет параметры (параметр хранимой процедуры := ключ схемы):
- p_schema_name  := schema.
- p_table_name  := table.
- p_lower_bound := lower_bound.
- p_upper_bound  := upper_bound.
- p_s3_server_name := s3_server_name.
- p_s3_bucket := s3_bucket.
- p_limit_operations := limit_operations.
- p_access_exclusive_mode := access_exclusive_mode.

## Описание менеджеров
create_partitions - применяется для создания партиции. Если партиция может быть создана, на выбранном промежутке времени (не пересечется с другими партициями), то она создается. Работают встроенные механизмы GP для создания партиций. Данные физически не перемещаются между табличными пространствами.  При месячной, недельной и годовой грянулярности, начало периода попадает на первый день указанного периода.  limit_operations если равен нулю, то тогда создаются все партиции. При указании limit_operations > 0 становится не возможно использовать merge оператор.
```
Допустимые ключи для create_partitions:
  granularity
  lower_bound
  upper_bound
  limit_operations
```

merge_partitions - применяется для сложной логике работы с партициями (объединения уже созданных партиций) и для приведения единообразия уже существующих, схем партиционирования. Иногда данные физически перемещает данные. Используют самописные скрипты для объединения партиций. Применимость необходимо доказывать, так как применяются механизмы как разбиения партиций, так и выравнивания партиций под схему партиционирования. Желательно применять уже после полного разбиения таблиц на партиции.
```
Допустимые ключи для merge_partitions:
  lower_bound
  upper_bound
  granularity
  table_space
  limit_operations default 5
  access_exclusive_mode default false,
  ddl_with_param default $$WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)$$
```

move_partitions - применяется для физического перемещения данных партиций между табличными пространствами. Используют самописную логику.
```
Допустимые ключи для move_partitions:
  lower_bound
  upper_bound
  table_space
  limit_operations default 5,
  access_exclusive_mode default false,
  ddl_with_param  default $$WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)$$
```

unload_to_s3_partitions - применяется фля физического перемещения данных партиций на S3. Используют самописную логику перенесения данных.
```
Допустимые ключи для unload_to_s3_partitions:
  lower_bound
  upper_bound
  s3_server_name default 's3srv',
  s3_bucket default 'd-dp-partition',
  limit_operations default 1,
  access_exclusive_mode default false
```
delete_partitions - применяется для физического удаления партиций. Использует встроенные механизмы GP.
```
Допустимые ключи для delete_partitions:
  lower_bound
  upper_bound
  limit_operations default 5
```

## Алгоритм действий
- Выбираем партиционированную таблицу.
- Анализируем по временной шкале.
- Составляем схему пратриций.
С выбором все ясно. Это ТЗ. И понятно, какая структура запросов к таблице.

**Проверяем схемы на отсутствие пробелов в датах между менеджерами create и merge.**
Перекрытие create и merge приветствуется. Вызов менеждеров согласно очередности по операциям и очередности в YAML схеме. Проверяем для каждой таблицы запросом:
``` sql 
SELECT *
FROM partitining_tool.fn_part_tools_get_config_intvals(
$$ [
    {
      "operation": "merge_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    },
    {
      "operation": "create_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    }
  ]
$$::JSON)
ORDER BY 1, 6
```

Интервалы в этом случае не всегда можно понять интуитивно, так как мы должны иметь в виду, что недельные, месячные и годовые партиции начинаются всегда либо с понедельника либо с первого числа соответственно. Поэтому операция ```select  now() - '1 month'::interval``` может оказатся не совсем интуитивно ожидаемой. Для размещения партиции необходимо, что бы начало и конец находился в диапазоне верхней и нижней границы. Менеждеры create_partitions учитывают, что было создано ранее, поэтому два раза партиция не создастся, а создается если только она целиком моежт быть создана.

# Полезные запросы
Определение доступных табличных пространств:
```sql
SELECT 
    spcname  
FROM pg_tablespace
```

Определение интервалов для одной таблицы:
```sql
SELECT *
FROM partitining_tool.fn_part_tools_get_config_intvals(
$$ [
    {
      "operation": "merge_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    },
    {
      "operation": "move_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    },
    {
      "operation": "unload_to_s3_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    },
    {
      "operation": "delete_partitions",
      "granularity": "1 month",
      "lower_bound": "10 month",
      "upper_bound": "-3 month",
      "table_space": "pg_default"
    }
  ]
$$::json)
ORDER BY 1, 6
```

Функция проверки конфигурации:
```sql
SELECT partitining_tool.fn_part_tools_check_config(
  p_schema_name := 'partitining_tool',
  p_table_name := 'sales_test',
  p_config := '[{"granularity": "1 month", "lower_bound": "1 year", "operation": "create_partitions", "upper_bound": "-3 month"}, {"granularity": "1 year", "lower_bound": "10 year", "operation": "create_partitions", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "granularity": "1 year", "limit_operations": 2, "lower_bound": "10 year", "operation": "merge_partitions", "table_space": "warm", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "limit_operations": 2, "lower_bound": "5 year", "operation": "move_partitions", "table_space": "warm", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "limit_operations": 2, "lower_bound": "6 year", "operation": "unload_to_s3_partitions", "upper_bound": "5 year"}]'::json
)
```

Функция проверки расположения партиций по табличным пространстам:
```sql
SELECT *
FROM partitining_tool.fn_part_tools_get_part_table_spase('partitining_tool', 'sales_test')
```

