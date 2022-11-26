-- fn_part_tools_check_table_space
-- depends: 20221010_04_4PaJM-fn_part_tools_check_is_table_ready_partitioning

CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_check_table_space(p_table_space TEXT)
 RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN

    IF NOT EXISTS(
            SELECT spcname
            FROM pg_tablespace
            WHERE spcname = p_table_space
        ) THEN
        RAISE EXCEPTION 'Not exists % table space', p_table_space;
    END IF;
    RETURN;
END
$$;
