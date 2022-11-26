/*check*/
SELECT *
FROM test_part.sales_test_1;

SELECT 
    *
FROM 
    partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test');

SELECT 
    *
FROM 
    partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test_1');

SELECT 
    *
FROM 
    partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test_2');

SELECT 
    *
FROM 
    partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test_3');

select 
    *
from partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test_4');

/*Разбор выравнивания таблицы на примере*/
SELECT 
    *
FROM 
    partitioning_tool.fn_part_tools_get_part_table_spase('test_part', 'sales_test');

SELECT 
  *
FROM partitioning_tool.fn_part_tools_get_config_intvals(
$$ [
  {
    "operation": "create_partitions",
    "granularity": "1 year",
    "lower_bound": "2 year",
    "upper_bound": "9 month"
  },
  {
    "operation": "create_partitions",
    "granularity": "3 month",
    "lower_bound": "1 year",
    "upper_bound": "2 month"
  },
  {
    "operation": "create_partitions",
    "granularity": "1 month",
    "lower_bound": "5 month",
    "upper_bound": "-3 month"
  },
  {
    "operation": "merge_partitions",
    "granularity": "3 month",
    "lower_bound": "1 year",
    "upper_bound": "3 month",
    "table_space": "warm",
    "limit_operations": 2,
    "access_exclusive_mode": false
  },
  {
    "operation": "unload_to_s3_partitions",
    "lower_bound": "5 year",
    "upper_bound": "1 year",
    "s3_server_name": "default"
  }
]
$$::json)
ORDER BY 1, 6


SELECT partitioning_tool.fn_part_tools_check_config(
  p_schema_name := 'partitioning_tool',
  p_table_name := 'sales_test',
  p_config := '[{"granularity": "1 month", "lower_bound": "1 year", "operation": "create_partitions", "upper_bound": "-3 month"}, {"granularity": "1 year", "lower_bound": "10 year", "operation": "create_partitions", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "granularity": "1 year", "limit_operations": 2, "lower_bound": "10 year", "operation": "merge_partitions", "table_space": "warm", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "limit_operations": 2, "lower_bound": "5 year", "operation": "move_partitions", "table_space": "warm", "upper_bound": "1 year"}, {"access_exclusive_mode": false, "limit_operations": 2, "lower_bound": "6 year", "operation": "unload_to_s3_partitions", "upper_bound": "5 year"}]'::json
)