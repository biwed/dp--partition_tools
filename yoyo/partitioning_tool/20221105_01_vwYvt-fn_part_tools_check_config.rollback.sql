-- 
-- depends: 20221010_23_TMzje-fn_part_tools_unload_to_s3_partitions
DROP FUNCTION partitioning_tool.fn_part_tools_check_config(
    p_config JSON, 
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
);