CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_get_interval(
    p_granularity INTERVAL,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL)
RETURNS TABLE (
        part_num BIGINT,
        lower_bound  TIMESTAMP,
        upper_bound TIMESTAMP
)
LANGUAGE plpgsql
AS
$$

BEGIN
    RETURN QUERY 
        SELECT
            row_number() OVER (ORDER BY bound) AS part_num,
            bound::TIMESTAMP AS lower_bound,
            lead(bound::TIMESTAMP) OVER (ORDER BY bound) AS upper_bound
        FROM (
                SELECT partitining_tool.generate_dates('1990-01-01', now() - p_upper_bound, coalesce(p_granularity, '1 day'::interval)) AS bound
            ) q
        WHERE bound > now() - p_lower_bound;
END
$$;