-- fn_part_tools_move_operation
-- depends: 20221010_18_sewKn-fn_part_tools_merge_partitions

CREATE OR REPLACE function partitining_tool.fn_part_tools_move_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_table_space TEXT,
    p_access_exclusive_mode BOOLEAN DEFAULT false,
    p_ddl_with_param text DEFAULT $$WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)$$
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_tmp_table_name_part TEXT;
    var_target_table_name TEXT;
    var_sql_exec TEXT = '';
    var_sql_transaction TEXT = '';
    var_insert_sql TEXT;
    var_finaly_sql TEXT;
    var_table_owner TEXT;
    var_name_part TEXT;
BEGIN
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_check_table_space(p_table_space);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    SELECT partitiontablename INTO var_name_part
    FROM
        pg_catalog.pg_partitions AS part
    WHERE
        part.schemaname = p_schema_name
        AND part.tablename = p_table_name
        AND NOT part.partitionisdefault
        AND partitining_tool.fn_eval(part.partitionrangestart)::DATE = p_partition_start::DATE;

    var_table_owner = partitining_tool.fn_part_tools_get_table_owner(p_schema_name, p_table_name);
    var_target_table_name = p_schema_name || '.' || p_table_name;
    var_tmp_table_name_part = var_target_table_name ||'_tmp_part_mv'|| MD5(random()::TEXT)::VARCHAR(12);
    var_sql_exec = 'drop table if exists ' || var_tmp_table_name_part || ' cascade;';
    var_sql_exec =  var_sql_exec || ' CREATE TABLE ' || var_tmp_table_name_part
            ||  '(LIKE ' || var_target_table_name || ') '
            || ' ' || p_ddl_with_param
            || ' TABLESPACE ' || p_table_space
            || '; ALTER table ' || var_tmp_table_name_part||'  OWNER TO '|| var_table_owner || '; ';
    var_insert_sql = '';
    var_finaly_sql = 'ALTER TABLE '|| var_target_table_name || ' EXCHANGE PARTITION FOR ('''||p_partition_start||'''::date)'
                || ' WITH TABLE ' || var_tmp_table_name_part
                || '; drop table '|| var_tmp_table_name_part || ';';

    --Добавить формирование скрипта сравнения количества строк в партициях и в новой таблице. через UNION ALL.
    var_insert_sql = var_insert_sql || ' INSERT INTO ' || var_tmp_table_name_part 
        || ' select * from ' || p_schema_name || '.'
        || var_name_part || ';';
    IF p_access_exclusive_mode THEN
       var_sql_transaction = ' LOCK TABLE '
            || var_target_table_name
            || ' IN ACCESS EXCLUSIVE MODE; '
            || var_sql_exec || ' '
            || var_insert_sql || ' '
            || var_finaly_sql || ' ';
       	RAISE NOTICE 'var_sql_transaction  %', var_sql_transaction;
        BEGIN
        EXECUTE var_sql_transaction;
        END;
    ELSE
        RAISE NOTICE 'var_sql_exec  %', var_sql_exec;
        EXECUTE var_sql_exec;
        RAISE NOTICE 'var_insert_sql  %', var_insert_sql;
        EXECUTE  var_insert_sql;
        RAISE NOTICE 'var_sql_exec  %', var_finaly_sql;
        EXECUTE var_finaly_sql;
    END IF;
end
$$;