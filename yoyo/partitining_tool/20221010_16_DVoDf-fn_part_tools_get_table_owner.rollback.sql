-- fn_part_tools_get_table_owner
-- depends: 20221010_15_BMAuD-fn_part_tools_get_part_table_spase

DROP FUNCTION partitining_tool.fn_part_tools_get_table_owner(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
);