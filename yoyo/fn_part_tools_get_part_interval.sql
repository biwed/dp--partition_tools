CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_get_part_interval(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound INTERVAL, 
    p_upper_bound INTERVAL)
RETURNS TABLE (
        part_num BIGINT,
        part_start TIMESTAMP,
        lower_bound TIMESTAMP,
        part_end TIMESTAMP,
        upper_bound TIMESTAMP,
        partitiontablename TEXT,
        partitionrank BIGINT,
        partitionrangestart TIMESTAMP,
        partitionrangeend TIMESTAMP,
        partitiontablespace TEXT,
        curr_upper_bound TIMESTAMP,
        curr_lower_bound TIMESTAMP
)
LANGUAGE plpgsql

AS
$$

begin

    CREATE TEMP TABLE tmp_get_part_interval AS
    SELECT
        partitioning_tool.fn_eval(part.partitionrangestart)::TIMESTAMP AS partitionrangestart,
        partitioning_tool.fn_eval(part.partitionrangeend)::TIMESTAMP AS partitionrangeend,
        part.partitiontablename::TEXT,
        part.partitionrank,
        part.partitiontablespace::TEXT
    FROM
        pg_catalog.pg_partitions AS part
    WHERE
        part.schemaname = p_schema_name 
        AND part.tablename = p_table_name
        AND NOT part.partitionisdefault;

    RETURN QUERY 
        SELECT need_partitions.part_num,
            min(need_partitions.partitionrangestart) OVER(PARTITION BY need_partitions.part_num) AS part_start,
            min(need_partitions.lower_bound) OVER(PARTITION BY need_partitions.part_num) AS lower_bound,
            max(need_partitions.partitionrangeend) OVER(PARTITION BY need_partitions.part_num) AS part_end,
            max(need_partitions.upper_bound) OVER(PARTITION BY need_partitions.part_num) AS upper_bound,
            need_partitions.partitiontablename,
            need_partitions.partitionrank,
            need_partitions.partitionrangestart,
            need_partitions.partitionrangeend,
            need_partitions.partitiontablespace,
            need_partitions.upper_bound AS curr_upper_bound,
            need_partitions.lower_bound AS curr_lower_bound
        FROM (
            SELECT 
            	date_interval.part_num,
                part.partitionrangestart,
                part.partitionrangeend,
                part.partitiontablename,
                part.partitionrank,
                date_interval.lower_bound::TIMESTAMP,
                date_interval.upper_bound::TIMESTAMP,
                part.partitiontablespace
            FROM 
                (
                    SELECT 
                        t.part_num,
                        t.lower_bound,
                        t.upper_bound
                    FROM
                        partitioning_tool.fn_part_tools_get_interval(
                            p_granularity,
                            p_lower_bound,
                            p_upper_bound
                        ) AS t
                ) AS date_interval
                LEFT JOIN tmp_get_part_interval AS part
                    ON 
                        (date_interval.lower_bound, date_interval.upper_bound) OVERLAPS (part.partitionrangestart, part.partitionrangeend)
        ) AS need_partitions
        WHERE need_partitions.upper_bound IS NOT NULL;

        DROP TABLE tmp_get_part_interval;
    RETURN;
END
$$;