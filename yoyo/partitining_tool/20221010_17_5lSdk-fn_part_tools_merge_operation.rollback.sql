-- fn_part_tools_merge_operation
-- depends: 20221010_16_DVoDf-fn_part_tools_get_table_owner

DROP FUNCTION partitining_tool.fn_part_tools_merge_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_granularity interval,
    p_table_space TEXT,
    p_slice JSON,
    p_access_exclusive_mode BOOLEAN,
    p_ddl_with_param TEXT
);
