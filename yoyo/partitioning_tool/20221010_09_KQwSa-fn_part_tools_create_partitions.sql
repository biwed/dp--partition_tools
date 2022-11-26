-- fn_part_tools_create_partitions
-- depends: 20221010_08_LBcYA-fn_part_tools_create_operation

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_create_partitions(
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING,
    p_granularity INTERVAL,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL,
    p_limit_operations INTEGER DEFAULT 0
)
RETURNS void
LANGUAGE plpgsql
AS
$$
DECLARE
    var_min_date DATE;
    var_max_date DATE;
begin
    PERFORM partitioning_tool.fn_part_tools_check_is_table_has_partitions(p_schema_name, p_table_name);
    PERFORM partitioning_tool.fn_part_tools_create_default_partition(p_schema_name, p_table_name);
    SELECT
        min(bound)::DATE, max(bound)::DATE INTO var_min_date, var_max_date
            FROM (
                    SELECT partitioning_tool.generate_dates('1990-01-01', now() - p_upper_bound, p_granularity) AS bound
            ) q
        WHERE bound > now() - p_lower_bound;
    PERFORM partitioning_tool.fn_part_tools_create_operation(
        p_schema_name,
        p_table_name,
        p_granularity,
        var_min_date,
        var_max_date,
        p_limit_operations
    );
END
$$;
