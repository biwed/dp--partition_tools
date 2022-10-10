CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_check_is_table_has_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
RETURNS VOID
LANGUAGE plpgsql
as
$$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_partitions p 
        WHERE 
            p.schemaname = p_schema_name 
            AND p.tablename = p_table_name
        LIMIT 1
    ) THEN 
        RAISE EXCEPTION 'Table %.% has not any partition.', p_schema_name, p_table_name;
        return;
    END IF;
END
$$;