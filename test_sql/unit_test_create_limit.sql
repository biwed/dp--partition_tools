CREATE SCHEMA IF NOT EXISTS test_part;

DROP IF EXISTS TABLE test_part.sales_test;
CREATE TABLE test_part.sales_test (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date) (DEFAULT PARTITION other);


ALTER TABLE test_part.sales_test  OWNER TO test_part_owner;

INSERT INTO test_part.sales_test (id, "date", amt)
with test as(
select
    generate_series('2016-01-01'::date, '2022-01-01'::date, '1 day'::interval) as date
)
select
    to_char(date, 'YYYYMMDD')::integer as id
    , date
    , (
        random() * 1000
      )::int + 1 as amt
from
    test;
    
SELECT *
FROM test_part.sales_test;

SELECT *
FROM partitioning_tool.fn_part_tools_create_partitions(
    p_schema_name:= 'test_part',
    p_table_name:= 'sales_test',
    p_granularity:= '1 month'::interval,
    p_lower_bound:= '10 year'::interval,
    p_upper_bound:= '1 month'::interval,
    p_limit_operations:=2
);

select *
from partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test');