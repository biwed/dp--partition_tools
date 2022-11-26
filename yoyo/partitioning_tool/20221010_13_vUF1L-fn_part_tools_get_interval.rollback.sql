-- fn_part_tools_get_interval
-- depends: 20221010_12_8IC6c-fn_part_tools_get_config_intvals

DROP FUNCTION partitioning_tool.fn_part_tools_get_interval(
    p_granularity INTERVAL,
    p_lower_bound INTERVAL,
    p_upper_bound INTERVAL
);
