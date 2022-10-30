-- fn_part_tools_create_partitions
-- depends: 20221010_08_LBcYA-fn_part_tools_create_operation

DROP FUNCTION partitioning_tool.fn_part_tools_create_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound ANYELEMENT,
    p_upper_bound ANYELEMENT
);
