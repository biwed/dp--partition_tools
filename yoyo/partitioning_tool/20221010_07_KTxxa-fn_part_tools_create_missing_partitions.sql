-- fn_part_tools_create_missing_partitions
-- depends: 20221010_06_RVbv1-fn_part_tools_create_default_partition

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_create_missing_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL
)
 RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
  var_row  RECORD;
  var_curs refcursor;
BEGIN
    PERFORM partitioning_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitioning_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);

    CREATE TEMP TABLE tmp_create_missing_partitions AS
    SELECT
        partitioning_tool.fn_eval(part.partitionrangestart)::TIMESTAMP AS partitionrangestart,
        partitioning_tool.fn_eval(partitionrangeend)::TIMESTAMP AS partitionrangeend
    FROM
        pg_catalog.pg_partitions AS part
    WHERE
        part.schemaname = p_schema_name 
        AND part.tablename = p_table_name
        AND NOT part.partitionisdefault;

    OPEN
        var_curs for 
            SELECT 
                partitionrangeend,
                lead_start 
            FROM (
                SELECT 
                    partitionrangestart,
                    partitionrangeend,
                    lead(partitionrangestart) 
                        OVER(ORDER BY partitionrangestart)
                    AS lead_start
                    FROM tmp_create_missing_partitions
                ) AS t
            WHERE NOT(t.lead_start = t.partitionrangeend)
            AND lead_start IS NOT NULL;
        LOOP
            FETCH FROM var_curs INTO var_row;
            EXIT WHEN NOT FOUND;
                PERFORM partitioning_tool.fn_part_tools_create_operation(p_schema_name, p_table_name, p_granularity, var_row.partitionrangeend::timestamp, var_row.lead_start::timestamp);
                RAISE NOTICE 'Split default partitions (date_from, date_to) %', var_row;
      END LOOP;
    CLOSE var_curs;
    DROP TABLE tmp_create_missing_partitions;
END
$$;

