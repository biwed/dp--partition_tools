-- fn_part_tools_get_part_table_spase
-- depends: 20221010_14_2Vzsc-fn_part_tools_get_part_interval

CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_get_part_table_spase(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    partitiontablespace TEXT,
    partitionrangestart DATE
)
LANGUAGE plpgsql
AS
$$
BEGIN

    RETURN Query 
        SELECT 
            part.schemaname::TEXT,
            part.tablename::TEXT,
            coalesce(substring(part.partitionname FROM 'ext_([[a-zA-Z0-9]+?)$'), part.partitiontablespace)::TEXT AS partitiontablespace,
            partitining_tool.fn_eval(part.partitionrangestart)::DATE AS partitionrangestart
        FROM
            pg_catalog.pg_partitions AS part
        WHERE
            part.schemaname = p_schema_name
            AND part.tablename = p_table_name
            AND not part.partitionisdefault;
    RETURN;
END
$$;