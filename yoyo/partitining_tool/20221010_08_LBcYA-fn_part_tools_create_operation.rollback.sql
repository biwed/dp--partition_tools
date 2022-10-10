-- fn_part_tools_create_operation
-- depends: 20221010_07_KTxxa-fn_part_tools_create_missing_partitions

DROP FUNCTION partitining_tool.fn_part_tools_create_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound ANYELEMENT,
    p_upper_bound ANYELEMENT
);