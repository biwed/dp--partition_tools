CREATE OR REPLACE FUNCTION partitioning_tool.fn_eval(p_str text)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE 
	var_res TEXT;
	var_swap TEXT;
BEGIN
    IF (p_str IS NULL) THEN RETURN NULL;
    END IF;

    IF lower(p_str) IN ('true','false') THEN
    	RETURN lower(p_str);
    ELSE
    	var_swap = COALESCE(p_str, '');
    	EXECUTE 'select  ('|| var_swap || ')::text as res1' INTO var_res;
     	RETURN var_res;
    END IF;
END;
$$