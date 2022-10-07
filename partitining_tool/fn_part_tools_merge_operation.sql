CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_merge_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_granularity interval,
    p_table_space TEXT,
    p_slice JSON,
    p_access_exclusive_mode BOOLEAN DEFAULT false,
    p_ddl_with_param TEXT DEFAULT $$WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)$$
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_row_inner RECORD;
    var_tmp_table_name_part TEXT;
    var_target_table_name TEXT;
    var_sql_exec TEXT = '';
    var_sql_alter TEXT = '';
    var_insert_sql TEXT;
    var_finaly_sql TEXT;
    var_table_owner TEXT;
    var_sql_transaction TEXT;
BEGIN
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_check_table_space(p_table_space);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    var_table_owner = partitining_tool.fn_part_tools_get_table_owner(p_schema_name, p_table_name);
    var_target_table_name = p_schema_name || '.' || p_table_name;
    var_tmp_table_name_part = var_target_table_name ||'_tmp_part_mrg'|| MD5(random()::TEXT)::varchar(12);
    var_sql_exec = 'drop table if exists ' || var_tmp_table_name_part || ' cascade;';
    var_sql_exec =  var_sql_exec || ' CREATE TABLE ' || var_tmp_table_name_part
            ||  '(LIKE ' || var_target_table_name || ') '
            || ' ' || p_ddl_with_param 
            || ' TABLESPACE ' || p_table_space
            || '; ALTER table ' || var_tmp_table_name_part||'  OWNER TO '|| var_table_owner ||'; ';
    var_sql_alter = '';
    var_insert_sql = '';
    var_finaly_sql = 'ALTER TABLE '|| var_target_table_name || ' EXCHANGE PARTITION FOR ('''||p_partition_start||'''::date)'
                || ' WITH TABLE ' || var_tmp_table_name_part
                || '; drop table '|| var_tmp_table_name_part || '; ';

        FOR var_row_inner IN 
                SELECT * 
                FROM json_array_elements(p_slice)
            loop
            var_insert_sql = var_insert_sql || ' INSERT INTO ' || var_tmp_table_name_part 
                || ' select * from ' || p_schema_name || '.'
                || (var_row_inner.value->>'partitiontablename')::TEXT || ';'; 
            var_sql_alter = var_sql_alter || ' ALTER TABLE ' || var_target_table_name
            || ' DROP PARTITION FOR ('''||(var_row_inner.value->>'partitionrangestart')::TEXT||'''::date);'; 
            end loop;
    IF p_access_exclusive_mode THEN
       var_sql_transaction = ' LOCK TABLE '
            || var_target_table_name
            || ' IN ACCESS EXCLUSIVE MODE; '
            || var_sql_exec || ' '
            || var_insert_sql || ' '
            || var_sql_alter || ' ';
       	RAISE NOTICE 'var_sql_transaction  %', var_sql_transaction;
        BEGIN
        EXECUTE var_sql_transaction;
        perform partitining_tool.fn_part_tools_create_operation(p_schema_name, p_table_name, p_granularity, p_partition_start, (p_partition_start + p_granularity)::date);
        EXECUTE var_finaly_sql;
        RAISE NOTICE 'var_sql_exec  %', var_finaly_sql;
        END;

    ELSE
        RAISE NOTICE 'var_sql_exec  %', var_sql_exec;
        EXECUTE var_sql_exec;
        RAISE NOTICE 'var_insert_sql  %', var_insert_sql;
        EXECUTE  var_insert_sql;
        RAISE NOTICE 'var_sql_exec  %', var_sql_alter;
        EXECUTE  var_sql_alter;
        perform partitining_tool.fn_part_tools_create_operation(p_schema_name, p_table_name, p_granularity, p_partition_start, (p_partition_start + p_granularity)::date);
        RAISE NOTICE 'var_finaly_sql  %', var_finaly_sql;
        EXECUTE var_finaly_sql;
    END IF;
end
$$;