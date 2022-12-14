-- fn_part_tools_get_config_intvals
-- depends: 20221010_11_fdg2c-fn_part_tools_delete_partitions

CREATE OR REPLACE FUNCTION  partitioning_tool.fn_part_tools_get_config_intvals(
    p_config JSON
)
RETURNS TABLE (
        operation TEXT,
        granularity INTERVAL,
        lower_bound INTERVAL,
        upper_bound INTERVAL,
        table_space TEXT,
        limit_operations INTEGER,
        lower_bound_ts TIMESTAMP,
        upper_bound_ts TIMESTAMP,
        id BIGINT
)
LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN Query 
        SELECT 
            (value::JSON)->>'operation' AS operation,
            ((value::JSON)->>'granularity')::INTERVAL AS granularity,
            ((value::JSON)->>'lower_bound')::INTERVAL AS lower_bound,
            ((value::JSON)->>'upper_bound')::INTERVAL AS upper_bound,
            ((value::JSON)->>'table_space')::TEXT AS table_space,
            ((value::JSON)->>'limit_operations')::INTEGER AS limit_operations,
            CASE WHEN (value::JSON)->>'operation' IN ('merge_partitions', 'create_partitions')
                THEN (
                    SELECT min(t_min.lower_bound)::TIMESTAMP 
                    FROM partitioning_tool.fn_part_tools_get_interval(
                            p_granularity:= ((value::JSON)->>'granularity')::INTERVAL,
                            p_lower_bound:= ((value::JSON)->>'lower_bound')::INTERVAL,
                            p_upper_bound:= ((value::JSON)->>'upper_bound')::INTERVAL
                        ) AS t_min
                )
                ELSE (now() -  ((value::JSON)->>'lower_bound')::INTERVAL)::TIMESTAMP  
            END AS lower_bound_ts,
            CASE WHEN (value::JSON)->>'operation' IN ('merge_partitions', 'create_partitions')
                THEN (
                    SELECT 
                        max(t_max.upper_bound)::TIMESTAMP
                    FROM partitioning_tool.fn_part_tools_get_interval(
                            p_granularity:= ((value::JSON)->>'granularity')::INTERVAL,
                            p_lower_bound:= ((value::JSON)->>'lower_bound')::INTERVAL,
                            p_upper_bound:= ((value::JSON)->>'upper_bound')::INTERVAL
                        ) AS t_max
                ) 
            ELSE (now() -  ((value::JSON)->>'upper_bound')::INTERVAL)::TIMESTAMP
            END AS upper_bound_ts,
            ROW_NUMBER() OVER() AS id
        FROM json_array_elements(p_config);
END
$$;