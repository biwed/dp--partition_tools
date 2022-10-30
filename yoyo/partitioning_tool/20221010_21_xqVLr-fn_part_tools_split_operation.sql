-- fn_part_tools_split_operation
-- depends: 20221010_20_mxe5b-fn_part_tools_move_partitions

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_split_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_lower_bound ANYELEMENT,
    p_upper_bound ANYELEMENT
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_sql TEXT;
BEGIN
    PERFORM partitioning_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitioning_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    IF (p_lower_bound  - '1 day'::interval < p_upper_bound  ) THEN
        var_sql = 'ALTER TABLE ' || p_schema_name || '.' || p_table_name || ' SPLIT PARTITION FOR ('
        || '''' || p_lower_bound || '''::' || pg_typeof(p_lower_bound) || ')'
        || ' AT (' || '''' || p_upper_bound || '''::' || pg_typeof(p_upper_bound) ||') '
        || 'INTO (PARTITION ' || p_table_name || '_prt_' || to_char(p_lower_bound, 'yyyymmdd') 
        || ', PARTITION ' || p_table_name || '_prt_' || to_char(p_upper_bound, 'yyyymmdd') || ');' ;
        
        RAISE NOTICE 'Query to execute %', var_sql;
        EXECUTE var_sql;
    END if;
    RAISE NOTICE 'Patrition very small for spliting';
END
$$;