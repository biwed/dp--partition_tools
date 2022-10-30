-- fn_part_tools_unload_to_s3_operation
-- depends: 20221010_21_xqVLr-fn_part_tools_split_operation

DROP FUNCTION partitioning_tool.fn_part_tools_unload_to_s3_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_s3_server_name TEXT,
    p_s3_bucket TEXT,
    p_access_exclusive_mode BOOLEAN
);