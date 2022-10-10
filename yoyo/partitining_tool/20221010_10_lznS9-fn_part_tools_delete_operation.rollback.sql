-- fn_part_tools_delete_operation
-- depends: 20221010_09_KQwSa-fn_part_tools_create_partitions

DROP FUNCTION partitining_tool.fn_part_tools_delete_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE
);