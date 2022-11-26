-- ADD fn_generate_dates
-- depends: 20221010_01_rtSBI_fn_eav
DROP FUNCTION partitioning_tool.generate_dates(
  p_start anyelement, 
  p_finish anyelement, 
  p_granularity INTERVAL
);