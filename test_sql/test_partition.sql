/*
Генерация таблиц.
- Создать подключние!!
- Создать бакет!!
*/

CREATE EXTENSION IF NOT EXISTS pxf;
CREATE TABLESPACE warm LOCATION '/data1/warm';

CREATE SCHEMA IF NOT EXISTS test_part;

/*test 1*/
DROP table IF EXISTS test_part.sales_test;
CREATE TABLE test_part.sales_test (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);



INSERT INTO test_part.sales_test (id, "date", amt)
with test as(
select
    generate_series(now()::date - '5 year'::interval, now()::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;
    

/*test 2*/

DROP table IF EXISTS test_part.sales_test_1;
CREATE TABLE test_part.sales_test_1 (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);



INSERT INTO test_part.sales_test_1 (id, "date", amt)
with test as(
select
    generate_series(now()::date - '5 year'::interval, now()::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;
    

/*test 3*/

DROP table IF EXISTS test_part.sales_test_2;
CREATE TABLE test_part.sales_test_2 (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);



INSERT INTO test_part.sales_test_2 (id, "date", amt)
with test as(
select
    generate_series(now()::date - '5 year'::interval, now()::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;

/*test 4*/
DROP table IF EXISTS test_part.sales_test_3;
CREATE TABLE test_part.sales_test_3 (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);



INSERT INTO test_part.sales_test_3 (id, "date", amt)
with test as(
select
    generate_series(now()::date - '5 year'::interval, now()::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;


/*test 5*/
DROP table IF EXISTS test_part.sales_test_4;
CREATE TABLE test_part.sales_test_4 (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);



INSERT INTO test_part.sales_test_4 (id, "date", amt)
with test as(
select
    generate_series(now()::date - '5 year'::interval, now()::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;