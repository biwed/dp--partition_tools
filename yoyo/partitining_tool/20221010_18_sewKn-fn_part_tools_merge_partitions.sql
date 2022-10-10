-- fn_part_tools_merge_partitions
-- depends: 20221010_17_5lSdk-fn_part_tools_merge_operation

CREATE OR REPLACE function partitining_tool.fn_part_tools_merge_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound INTERVAL, 
    p_upper_bound INTERVAL,
    p_granularity INTERVAL,
    p_table_space TEXT,
    p_limit_operations INTEGER default 5,
    p_access_exclusive_mode BOOLEAN default false,
    p_ddl_with_param TEXT default $$WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)$$
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
    var_slice json;
BEGIN
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_check_table_space(p_table_space);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    var_target_table_name = p_schema_name || '.' || p_table_name;
    var_tmp_table_name_part = var_target_table_name ||'_tmp_pct';
    OPEN
        var_curs for 
            SELECT *
            FROM partitining_tool.fn_part_tools_get_part_interval(p_schema_name,
                                                p_table_name,
                                                p_granularity,
                                                p_lower_bound, 
                                                p_upper_bound)
                WHERE 
                  part_start IS NULL
                  order by curr_lower_bound, partitionrangestart;
        LOOP
            FETCH FROM var_curs INTO var_row;
            EXIT WHEN NOT FOUND;
                PERFORM partitining_tool.fn_part_tools_create_operation(
                    p_schema_name,
                    p_table_name, 
                    p_granularity, 
                    var_row.curr_lower_bound, 
                    var_row.curr_upper_bound);
                RAISE NOTICE 'Split default partitions %', var_row;
        END LOOP;
    CLOSE var_curs;
    perform partitining_tool.fn_part_tools_create_missing_partitions(p_schema_name, p_table_name, '1 day');

    OPEN
        var_curs for 
            SELECT *
            FROM partitining_tool.fn_part_tools_get_part_interval(
                p_schema_name,
                p_table_name,
                p_granularity,
                p_lower_bound,
                p_upper_bound)
            WHERE
                  lower_bound IS DISTINCT FROM part_start 
                  OR upper_bound IS DISTINCT FROM part_end
            ORDER BY curr_lower_bound, partitionrangestart;
        LOOP
            FETCH FROM var_curs INTO var_row;
            EXIT WHEN NOT FOUND;
                RAISE NOTICE 'Split partition %', var_row;
                PERFORM partitining_tool.fn_part_tools_split_operation(
                    p_schema_name,
                    p_table_name, 
                    GREATEST(var_row.curr_lower_bound, var_row.partitionrangestart)::DATE, 
                    LEAST(var_row.curr_upper_bound::DATE, 
                    (var_row.partitionrangeend - '1 day'::interval)::DATE)::DATE, 
                    var_row.partitionrangeend::DATE
                );
                
        END LOOP;
    CLOSE var_curs;

    OPEN var_curs for 
        SELECT 
            DISTINCT part_num,
            lower_bound,
            upper_bound
        FROM (
            SELECT 
                part_num,
                lower_bound, 
                upper_bound 
            FROM partitining_tool.fn_part_tools_get_part_interval(
            p_schema_name,
                p_table_name,
                p_granularity,
                p_lower_bound,
                p_upper_bound)
        WHERE 
            lower_bound = part_start 
            AND upper_bound = part_end
            AND 
                NOT (
                    lower_bound = partitionrangestart
                    AND upper_bound = partitionrangeend
                )
            ORDER BY curr_lower_bound, partitionrangestart
        LIMIT p_limit_operations
        ) AS t;
       LOOP
        FETCH FROM var_curs INTO var_row;
        EXIT WHEN NOT FOUND;
            SELECT
                array_to_json(array_agg(row_to_json(t))) INTO var_slice
            FROM partitining_tool.fn_part_tools_get_part_interval(
                    p_schema_name,
                    p_table_name,
                    p_granularity,
                    p_lower_bound, 
                    p_upper_bound) AS t
            WHERE t.part_num = var_row.part_num::BIGINT;

            PERFORM partitining_tool.fn_part_tools_merge_operation(
                p_schema_name,
                p_table_name,
                var_row.lower_bound::DATE,
                p_granularity,
                p_table_space,
                var_slice,
                p_access_exclusive_mode,
                p_ddl_with_param
            );
        END LOOP;
    CLOSE var_curs;
END
$$;