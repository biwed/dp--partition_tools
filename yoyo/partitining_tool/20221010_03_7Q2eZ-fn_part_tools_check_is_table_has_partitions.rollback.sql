-- fn_part_tools_check_is_table_has_partitions
-- depends: 20221010_02_9NGqu_fn_generate_dates

DROP FUNCTION partitining_tool.fn_part_tools_check_is_table_has_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
);