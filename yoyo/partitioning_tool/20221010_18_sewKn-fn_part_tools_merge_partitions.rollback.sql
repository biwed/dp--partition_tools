-- fn_part_tools_merge_partitions
-- depends: 20221010_17_5lSdk-fn_part_tools_merge_operation

DROP FUNCTION partitioning_tool.fn_part_tools_merge_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound INTERVAL, 
    p_upper_bound INTERVAL,
    p_granularity INTERVAL,
    p_table_space TEXT,
    p_limit_operations INTEGER,
    p_access_exclusive_mode BOOLEAN,
    p_ddl_with_param TEXT
);