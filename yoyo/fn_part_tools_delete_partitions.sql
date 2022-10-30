CREATE OR REPLACE function partitioning_tool.fn_part_tools_delete_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL,
    p_limit_operations INTEGER default 5
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_row  RECORD;
    var_curs refcursor;
    var_curs_inner refcursor;
    var_row_inner  RECORD;
    var_tmp_table_name_part TEXT;
    var_target_table_name TEXT;
    var_sql_exec TEXT = '';
    var_sql_transaction TEXT = '';
    var_insert_sql TEXT;
    var_finaly_sql TEXT;
    var_slice JSON;
BEGIN
    PERFORM partitioning_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitioning_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    OPEN
        var_curs for 
            SELECT part.partitionrangestart as move_part
            FROM
                partitioning_tool.fn_part_tools_get_part_table_spase(p_schema_name, p_table_name) as part
            WHERE
                partitionrangestart >= now() - p_lower_bound
                AND part.partitionrangestart < now() - p_upper_bound
            ORDER BY 1
            LIMIT p_limit_operations;
        LOOP
            FETCH FROM var_curs INTO var_row;
            EXIT WHEN NOT FOUND;
                PERFORM partitioning_tool.fn_part_tools_delete_operation(
                    p_schema_name,
                    p_table_name,
                    var_row.move_part::DATE
                );
                RAISE NOTICE 'Drop patririot partitions %', var_row.move_part::DATE;
        END LOOP;
    CLOSE var_curs;
END
$$;