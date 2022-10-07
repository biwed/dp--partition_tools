CREATE OR REPLACE function partitining_tool.fn_part_tools_get_table_owner(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
RETURNS TEXT
LANGUAGE plpgsql
as
$$
DECLARE
    var_owner TEXT;

BEGIN
    SELECT t.tableowner INTO var_owner
    FROM 
        pg_tables as t
    WHERE 
        t.schemaname = p_schema_name
        AND t.tablename = p_table_name
    LIMIT 1;
    RETURN quote_literal(var_owner);
END
$$;