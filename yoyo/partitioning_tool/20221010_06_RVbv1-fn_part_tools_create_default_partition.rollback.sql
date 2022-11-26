-- fn_part_tools_create_default_partition
-- depends: 20221010_05_fEzPT-fn_part_tools_check_table_space

DROP FUNCTION partitioning_tool.fn_part_tools_create_default_partition(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING);
