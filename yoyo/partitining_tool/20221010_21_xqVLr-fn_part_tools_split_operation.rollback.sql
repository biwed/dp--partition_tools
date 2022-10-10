-- fn_part_tools_split_operation
-- depends: 20221010_20_mxe5b-fn_part_tools_move_partitions

DROP FUNCTION partitining_tool.fn_part_tools_split_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound ANYELEMENT,
    p_upper_bound ANYELEMENT
);