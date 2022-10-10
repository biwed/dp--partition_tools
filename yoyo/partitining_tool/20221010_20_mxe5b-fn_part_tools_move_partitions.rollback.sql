-- fn_part_tools_move_partitions
-- depends: 20221010_19_hs0MI-fn_part_tools_move_operation

DROP FUNCTION partitining_tool.fn_part_tools_move_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL,
    p_table_space TEXT,
    p_limit_operations INTEGER,
    p_access_exclusive_mode BOOLEAN,
    p_ddl_with_param TEXT
);