CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_create_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    var_min_date DATE;
    var_max_date DATE;
begin
    PERFORM partitining_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitining_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);
    SELECT
        min(bound)::DATE, max(bound)::DATE INTO var_min_date, var_max_date
            FROM (
                    SELECT partitining_tool.generate_dates('1990-01-01', now() - p_upper_bound, p_granularity) AS bound
            ) q
        WHERE bound > now() - p_lower_bound;
    PERFORM partitining_tool.fn_part_tools_create_operation(
        p_schema_name,
        p_table_name,
        p_granularity,
        var_min_date,
        var_max_date
    );
END
$$;