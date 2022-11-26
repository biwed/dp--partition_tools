-- fn_part_tools_move_operation
-- depends: 20221010_18_sewKn-fn_part_tools_merge_partitions

DROP FUNCTION partitioning_tool.fn_part_tools_move_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_table_space TEXT,
    p_access_exclusive_mode BOOLEAN,
    p_ddl_with_param text 
);
