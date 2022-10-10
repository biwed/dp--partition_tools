-- fn_part_tools_unload_to_s3_partitions
-- depends: 20221010_22_ON9Lx-fn_part_tools_unload_to_s3_operation

DROP FUNCTION partitining_tool.fn_part_tools_unload_to_s3_partitions(
    p_schema_name character varying,
    p_table_name character varying,
    p_lower_bound interval,
    p_upper_bound interval,
    p_s3_server_name text,
    p_s3_bucket text default,
    p_limit_operations INTEGER,
    p_access_exclusive_mode BOOLEAN
);