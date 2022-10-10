-- fn_part_tools_create_missing_partitions
-- depends: 20221010_06_RVbv1-fn_part_tools_create_default_partition

DROP FUNCTION partitining_tool.fn_part_tools_create_missing_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL
);