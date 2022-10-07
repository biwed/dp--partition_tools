CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_create_default_partition(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_sql text;
    var_def_partition text = '';
BEGIN

    IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_partitions p 
            WHERE 
                p.schemaname = p_schema_name 
                AND p.tablename = p_table_name
                AND partitionisdefault
            LIMIT 1
        ) THEN 
        RAISE NOTICE 'Table %.% has not dafault partition.', p_schema_name, p_table_name;
        var_def_partition = 'alter table ' || p_schema_name || '.' || p_table_name ||
                ' add default partition other; ';
        RAISE NOTICE 'Query to execute %', var_def_partition;
        EXECUTE var_def_partition;
    END IF;
END
$$;