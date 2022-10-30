-- fn_part_tools_delete_partitions
-- depends: 20221010_10_lznS9-fn_part_tools_delete_operation

DROP FUNCTION partitioning_tool.fn_part_tools_delete_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL,
    p_limit_operations INTEGER
);