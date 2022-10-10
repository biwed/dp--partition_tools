-- ADD function fn_eav
-- depends: 
CREATE OR REPLACE FUNCTION partitining_tool.fn_eval(p_str text)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
/* Функция преобразует sql предложение в результат.
3*4; => '12'

*/
DECLARE 
    var_res TEXT;
    var_swap TEXT;
BEGIN
    IF lower(p_str) IN ('true','false') THEN
    	RETURN lower(p_str);
    IF (p_str IS NULL) THEN RETURN NULL;
    ELSE
        EXECUTE 'select  ('|| p_str || ')::text as res1' INTO var_res;
        RETURN var_res;
    END IF;
END;
$$
