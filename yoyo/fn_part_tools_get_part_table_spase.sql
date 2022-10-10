CREATE OR REPLACE function partitining_tool.fn_part_tools_get_part_table_spase(
        p_schema_name character varying,
        p_table_name character varying
    )
returns table (
        schemaname text,
        tablename text,
        partitiontablespace text,
        partitionrangestart date
)
language plpgsql

as
$$

begin

    return Query 
        select 
            part.schemaname::text,
            part.tablename::text,
            coalesce(substring(part.partitionname from 'ext_([[a-zA-Z0-9]+?)$'), part.partitiontablespace)::text as partitiontablespace,
            partitining_tool.fn_eval(part.partitionrangestart)::date as partitionrangestart
        from
            pg_catalog.pg_partitions as part
        where
            part.schemaname = p_schema_name
            and part.tablename = p_table_name
            and not part.partitionisdefault;
    return;
end
$$;