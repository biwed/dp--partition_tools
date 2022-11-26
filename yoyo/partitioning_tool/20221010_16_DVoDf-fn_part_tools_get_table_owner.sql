-- fn_part_tools_get_table_owner
-- depends: 20221010_15_BMAuD-fn_part_tools_get_part_table_spase

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_get_table_owner(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
 RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
    var_owner TEXT;
BEGIN
    SELECT 
        t.tableowner INTO var_owner
    FROM 
        pg_tables AS t
    WHERE 
        t.schemaname = p_schema_name
        AND t.tablename = p_table_name
    LIMIT 1;
    RETURN quote_ident(var_owner);
END
$$;