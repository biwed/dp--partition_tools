CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_check_is_table_ready_partitioning(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_tables AS p
        WHERE p.schemaname = p_schema_name  AND 
            tablename ~ ('^'||p_table_name||'.*'||'[a-fA-F0-9]{12}$')
            LIMIT 1
    ) THEN 
        RAISE EXCEPTION 'Table %.% has not deleted temp tables.', p_schema_name, p_table_name;
        RETURN;
    END IF;
END
$$;