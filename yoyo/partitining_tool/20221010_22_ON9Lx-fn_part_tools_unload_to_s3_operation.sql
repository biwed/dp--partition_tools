-- fn_part_tools_unload_to_s3_operation
-- depends: 20221010_21_xqVLr-fn_part_tools_split_operation

CREATE OR REPLACE function partitining_tool.fn_part_tools_unload_to_s3_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE,
    p_s3_server_name TEXT DEFAULT 'default',
    p_s3_bucket TEXT DEFAULT 'dp-partition',
    p_access_exclusive_mode BOOLEAN default false
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
    var_s3_connect TEXT;
    var_end_table TEXT;
    var_ext_write_table_name TEXT;
    var_drop_temp_tables TEXT;
    var_name_part_table TEXT;
    var_row_count BIGINT;
   	var_count_sql TEXT;
BEGIN
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);
    
    SELECT 
        partitiontablename, partitionname INTO var_name_part_table, var_name_part
    FROM
        pg_catalog.pg_partitions AS part
    WHERE
        part.schemaname = p_schema_name
        AND part.tablename = p_table_name
        AND NOT part.partitionisdefault
        AND partitining_tool.fn_eval(part.partitionrangestart)::DATE = p_partition_start::DATE;

    var_s3_connect = 'pxf://'|| p_s3_bucket 
        ||'/' || p_schema_name ||'/'||p_table_name || '/' || var_name_part || '_ext_s3'
        || '?PROFILE=s3:text&SERVER='|| p_s3_server_name 
        || '&COMPRESSION_CODEC=org.apache.hadoop.io.compress.GzipCodec';

    var_table_owner = partitining_tool.fn_part_tools_get_table_owner(p_schema_name, p_table_name);
    var_target_table_name = p_schema_name || '.' || p_table_name;
    var_end_table = MD5(random()::TEXT)::VARCHAR(12);
    var_tmp_table_name_part = var_target_table_name ||'_tmp_part_ext'|| var_end_table;
    var_ext_write_table_name =  var_target_table_name ||'_tmp_part_ext_wrt'|| var_end_table;
    var_drop_temp_tables = ' DROP TABLE if exists ' || var_tmp_table_name_part || ' cascade;'
        || ' DROP EXTERNAL TABLE if exists ' || var_ext_write_table_name || ' cascade;';

    var_sql_exec = var_drop_temp_tables || ' CREATE WRITABLE EXTERNAL TABLE ' || var_ext_write_table_name
        ||' ( LIKE ' || var_target_table_name || ') '
        ||' LOCATION ( ''' || var_s3_connect ||''' ) ON ALL FORMAT ''TEXT'' ( delimiter='','' ) ENCODING ''UTF8'';';

    var_sql_exec = var_sql_exec ||' CREATE EXTERNAL TABLE ' || var_tmp_table_name_part
    ||' ( LIKE ' || var_target_table_name || ') '
    ||' LOCATION ( ''' || var_s3_connect ||''' ) ON ALL FORMAT ''TEXT'' ( delimiter='','' ) ENCODING ''UTF8'';';

    var_sql_exec =  var_sql_exec || '  ALTER EXTERNAL TABLE ' || var_tmp_table_name_part||'  OWNER TO '|| var_table_owner || ';';
    var_sql_transaction = '';
    var_insert_sql = '';
    var_finaly_sql = 'ALTER TABLE '|| var_target_table_name || ' EXCHANGE PARTITION FOR ('''||p_partition_start||'''::date)'
        || ' WITH TABLE ' || var_tmp_table_name_part || ' WITHOUT VALIDATION;';
    var_finaly_sql = var_finaly_sql || ' ALTER TABLE '|| var_target_table_name || ' RENAME partition FOR ('''||p_partition_start||'''::date)'
        || ' to ' || var_name_part || '_ext_s3;';
    var_finaly_sql = var_finaly_sql || var_drop_temp_tables;
    var_insert_sql = var_insert_sql || ' INSERT INTO ' || var_ext_write_table_name 
        || ' select * from ' || p_schema_name || '.'
        || var_name_part_table || ';';
    var_count_sql =  ' select 1 from ' || p_schema_name || '.'
        || var_name_part_table || ' limit 1;';
    EXECUTE var_count_sql INTO var_row_count;
    RAISE NOTICE 'var_row_count = %', var_row_count;
   /*Проверяем, что таблица не пустая*/
    IF coalesce(var_row_count, 0) < 1 THEN 
        RAISE NOTICE 'Вы пытаетесь перенести пустую партицию на S3 count = %', var_row_count;
       RETURN;
    END IF;
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
        EXECUTE var_finaly_sql;
    END IF;
END
$$;