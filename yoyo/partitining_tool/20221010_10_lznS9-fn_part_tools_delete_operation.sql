-- fn_part_tools_delete_operation
-- depends: 20221010_09_KQwSa-fn_part_tools_create_partitions

CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_delete_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_partition_start DATE
)
 RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_tmp_table_name_part TEXT;
    var_target_table_name TEXT;
    var_sql_transaction TEXT = '';
BEGIN

    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    var_target_table_name = p_schema_name || '.' || p_table_name;
    var_sql_transaction = ' ALTER TABLE ' || var_target_table_name
        || ' DROP PARTITION IF EXISTS FOR ('''|| p_partition_start ||'''::date);';
    --Добавить формирование скрипта сравнения количества строк в партициях и в новой таблице. через UNION ALL.
    RAISE NOTICE 'var_sql_transaction %', var_sql_transaction;
    EXECUTE  var_sql_transaction;
END
$$;