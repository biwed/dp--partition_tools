-- fn_part_tools_unload_to_s3_partitions
-- depends: 20221010_22_ON9Lx-fn_part_tools_unload_to_s3_operation

CREATE OR REPLACE function partitining_tool.fn_part_tools_unload_to_s3_partitions(
    p_schema_name character varying,
    p_table_name character varying,
    p_lower_bound interval,
    p_upper_bound interval,
    p_s3_server_name text default 's3srv',
    p_s3_bucket text default 'dp-partition',
    p_limit_operations INTEGER default 1,
    p_access_exclusive_mode BOOLEAN default false
)
 RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_row  RECORD;
    var_curs refcursor;
BEGIN
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    OPEN
        var_curs for 
            SELECT part.partitionrangestart as move_part
            from
                partitining_tool.fn_part_tools_get_part_table_spase(p_schema_name, p_table_name) as part
            where
                not partitiontablespace = 's3'
                and partitionrangestart >= now() - p_lower_bound
                and part.partitionrangestart < now() - p_upper_bound
            LIMIT p_limit_operations;
        LOOP
            FETCH FROM var_curs INTO var_row;
            EXIT WHEN NOT FOUND;
                PERFORM partitining_tool.fn_part_tools_unload_to_s3_operation(
                    p_schema_name,
                    p_table_name,
                    var_row.move_part::date,
                    p_s3_server_name,
                    p_s3_bucket,
                    p_access_exclusive_mode
                );
                RAISE NOTICE 'Unload partitions %', var_row.move_part::date;
        END LOOP;
    CLOSE var_curs;
END
$$;