# Partition tools
Проект служит для управления партициями на Greenplum. Предусмотренны операции операции разбиения партиций, перемещения между табличными пространствами, объединения партиций с выравниманием и удаление ненужных партций и перемещение партиций на S3.

# Архитектура прокта
Проект содержит модули:
- Airflow - автоматической генерации DAG для airflow
- Airflow - обеспеченте миграций при помощи yoyo
- Харнимые процедуры на plpgsql.

Вся логика, проверок и управления партициями реализована через хранимые процедуры. Airflow используется в качестве генератора последовательности выполнения хранимых процедур и планировщика заданий. Можно написать собественный планировщик сторонный планировщик, так как логика зашита на хранимых процедурах.

# Запуск проекта
Подробнее в видео, которое посвященно обзору решения [Видео "Partition tools для GP" Youtube](https://youtu.be/nePlGkWjZdc)

## Применение пользователя
Проетк собран в docker-compose который и обеспечивает развертывание кластера. Для его запуска требуется в файл .env загрузить идентификатор текущего пользователя, для нормального чтения airflow кода исполнения, который находится в папке dags/
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
## Пересборка docker образа для worker
Для чего нужна пересборка образа для worker с целью показать возможности:
- worker может содержать спецефические пакеты к примеру yoyo, которых нет в исходном образе.
- worker может содержать  папки, для обеспечения работоспособности решения.
Тем самым мы можем автоматизировать сборку необходимого образа для развертывания CI/CD в том же самом kuber для исполнения наших процессов. Можно было использовать и виртуальные операторы для испольнения спецефичного кода, но так на мой взгляд будет точнее. 
```sh
docker-compose build
```

## Запуск проекта
После пересборки проекта, приступаем к запуску кластера для проведения тестов:
```sh
docker-compose up -d
```
Тестирование:
- создать подключение bi_bot, которое необходимо для работы с GP. Доступы см. ниже.
- создание s3 бакета (dp-partition) на minio. Это назавание определенно в качестве параметра по умолчанию в хранимой процедуре. partitioning_tool.fn_part_tools_unload_to_s3_partitions
- запуск DAG который выполнит миграцию.
- создание таблиц необходимых для проведения тестов находится в файле  [test_sql/test_partition.sql](./test_sql/test_partition.sql)
- запуск DAG нарезки партиций по написанной  [yaml](./dags/partitioning_configs/greenplum/test_part.yaml) схеме.
Более подробнее на видео.

## Доступы
Доступы все можно увидеть в файле  [docker-compose.yaml](./docker-compose.yaml)
- Airflow - host: localhost:8080 login: airflow password: airflow
- Greenplum - host: localhost (из airflow gpdb) port: 5432, db: greenplum, login test, password: test.
- Minio - host: localhost:9001 login: minio, password: minio123
- Доступ из Greenplum в Minio расположен в файле config/minio-site.xml

Документация по использованию хранимых процедур нахоится в файле [документации](./doc/table_operation.md)  

## Остановка проекта
После проведения тестов останавливаем проект через комманду:
```sh
docker-compose down
```

## Ссылки
- [Документация по хранимым процедурам](./doc/table_operation.md)
- [Видео "Partition tools для GP" Youtube](https://youtu.be/nePlGkWjZdc)
- [Видео "Greenplum хранение таблиц" Youtube](https://youtu.be/yV0leI-lRWM)
- [Видео "Greenplum PXF S3" Youtube](https://youtu.be/iz-J_yFHgTE)



