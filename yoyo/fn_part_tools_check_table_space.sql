CREATE OR REPLACE FUNCTION partitining_tool.fn_part_tools_check_table_space(p_table_space TEXT)
    RETURNS void
LANGUAGE plpgsql

as
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