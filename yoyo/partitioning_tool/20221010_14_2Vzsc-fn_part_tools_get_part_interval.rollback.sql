-- fn_part_tools_get_part_interval
-- depends: 20221010_13_vUF1L-fn_part_tools_get_interval

DROP FUNCTION partitioning_tool.fn_part_tools_get_part_interval(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL
);