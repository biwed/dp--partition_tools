-- ADD fn_generate_dates
-- depends: 20221010_01_rtSBI_fn_eav
CREATE OR REPLACE FUNCTION partitining_tool.generate_dates(
  p_start anyelement, 
  p_finish anyelement, 
  p_granularity INTERVAL
)
RETURNS SETOF anyelement
LANGUAGE plpgsql
AS
$$
DECLARE
    var_sql TEXT;
BEGIN

    IF pg_typeof(p_start)<>pg_typeof(p_finish) THEN
        RAISE EXCEPTION 'p_start and p_finish arguments must be same type';
    END IF;

    IF pg_typeof(p_start) NOT IN ('date','timestamp with time zone','timestamp without time zone') THEN
        RAISE EXCEPTION 'function works with date and timestamp only';
    END IF;

    var_sql := 'SELECT generate_series(timestamp '
        ||quote_literal(p_start)||', timestamp '
        ||quote_literal(p_finish)||', interval '
        ||quote_literal(p_granularity)||')::'
        ||pg_typeof(p_start)::TEXT;
RETURN QUERY 
    EXECUTE var_sql;
END
$$;
