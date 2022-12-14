-- fn_part_tools_create_operation
-- depends: 20221010_07_KTxxa-fn_part_tools_create_missing_partitions

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_create_operation(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound ANYELEMENT,
    p_upper_bound ANYELEMENT,
    p_limit_operations INTEGER default 0)
RETURNS void
LANGUAGE plpgsql

AS
$$
DECLARE
    var_sql TEXT;
    var_def_partition TEXT = '';
BEGIN
    PERFORM partitioning_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitioning_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    CREATE TEMP TABLE tmp_partition_ranges as
    SELECT
        partitioning_tool.fn_eval(part.partitionrangestart)::timestamp as partitionrangestart
        , partitioning_tool.fn_eval(partitionrangeend)::timestamp as partitionrangeend
    FROM
        pg_catalog.pg_partitions as part
    WHERE
        part.schemaname = p_schema_name 
        and part.tablename = p_table_name
        and not part.partitionisdefault;

    IF p_limit_operations > 0 THEN 
        SELECT string_agg(stmt, chr(10) ORDER BY part_num)
        FROM (
            SELECT 'alter table ' || p_schema_name || '.' || p_table_name ||
                ' split default partition start (' || lower_bound || ') inclusive end (' || upper_bound ||
                ') into (partition ' || part_name || ', default partition);' as stmt,
                part_num
            FROM (
                    SELECT 
                        '''' || lower_bound || '''::' || pg_typeof(lower_bound) AS lower_bound,
                        '''' || upper_bound || '''::' || pg_typeof(upper_bound) AS upper_bound,
                        p_table_name || '_prt_' || to_char(lower_bound, 'yyyymmdd') AS part_name,
                        part_num
                    FROM (
                            SELECT 
                                bound AS lower_bound,
                                lead(bound) OVER (ORDER BY bound) AS upper_bound,
                                row_number() OVER (ORDER BY bound) AS part_num
                            FROM (
                                    SELECT partitioning_tool.generate_dates(p_lower_bound, p_upper_bound, p_granularity) AS bound
                                ) q
                    ) p 
                    WHERE 
                        NOT EXISTS (
                            SELECT 1 
                            FROM 
                                tmp_partition_ranges AS part 
                            WHERE  
                                (p.lower_bound, p.upper_bound) overlaps (part.partitionrangestart, part.partitionrangeend)
                        )
                        AND upper_bound IS NOT NULL
                    LIMIT p_limit_operations
            ) s
        ) t
        INTO var_sql;
    ELSE 
        SELECT string_agg(stmt, chr(10) ORDER BY part_num)
        FROM (
            SELECT 'alter table ' || p_schema_name || '.' || p_table_name ||
                ' split default partition start (' || lower_bound || ') inclusive end (' || upper_bound ||
                ') into (partition ' || part_name || ', default partition);' AS stmt,
                part_num
            FROM (
                    SELECT '''' || lower_bound || '''::' || pg_typeof(lower_bound) AS lower_bound,
                            '''' || upper_bound || '''::' || pg_typeof(upper_bound) AS upper_bound,
                            p_table_name || '_prt_' || to_char(lower_bound, 'yyyymmdd') AS part_name,
                            part_num
                    FROM (
                            SELECT 
                                bound AS lower_bound,
                                lead(bound) OVER (ORDER BY bound) AS upper_bound,
                                row_number() OVER (ORDER BY bound) AS part_num
                            FROM (
                                    SELECT partitioning_tool.generate_dates(p_lower_bound, p_upper_bound, p_granularity) AS bound
                                ) q
                    ) p 
                    WHERE 
                        NOT EXISTS (
                            SELECT 1 
                            FROM 
                                tmp_partition_ranges as part 
                            WHERE  
                                (p.lower_bound, p.upper_bound) overlaps (part.partitionrangestart, part.partitionrangeend)
                        )
                        and upper_bound is not null
            ) s
        ) t
        INTO var_sql;
        END IF;

    raise notice 'Query to execute %', var_sql;

    IF var_sql IS NOT NULL THEN
        EXECUTE var_sql;
    ELSE
        raise notice 'Query string argument is %. Partitions are already sliced.', var_sql;
    END IF;
    DROP TABLE tmp_partition_ranges;
END
$$;