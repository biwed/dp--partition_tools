-- 
-- depends: 20221010_23_TMzje-fn_part_tools_unload_to_s3_partitions
CREATE OR REPLACE FUNCTION partitioning_tool.fn_part_tools_check_config(
    p_config JSON, 
    p_schema_name CHARACTER VARYING,
    p_table_name CHARACTER VARYING
)
RETURNS void
LANGUAGE plpgsql
AS
$$
DECLARE
  var_test_passed bool;
  var_all_test_passes bool;
BEGIN
    var_all_test_passes = true;
    -- drop table if EXISTS tmp_config_part;
    CREATE TEMP TABLE tmp_config_part AS
    SELECT
        operation,
        granularity,
        lower_bound,
        upper_bound,
        table_space,
        lower_bound_ts,
        upper_bound_ts,
        id,
        limit_operations
    FROM 
        partitioning_tool.fn_part_tools_get_config_intvals(p_config);

    /*Проверка на delete_partitions. Промежуток не должен пересекаться ни с одним интервалом*/
    IF EXISTS (
            SELECT 1
            FROM (
                SELECT 
                    cp.lower_bound_ts,
                    cp.upper_bound_ts
                FROM tmp_config_part AS cp
                WHERE cp.operation = 'delete_partitions'
            ) AS del_part
            INNER JOIN 
                (
                    SELECT 
                        lower_bound_ts,
                        upper_bound_ts
                    FROM tmp_config_part AS cp
                    WHERE cp.operation <> 'delete_partitions'
                ) AS oper_part
                ON  (del_part.lower_bound_ts, del_part.upper_bound_ts) OVERLAPS
                    (oper_part.lower_bound_ts,oper_part.upper_bound_ts) 
    ) THEN 
        var_test_passed = false;
        RAISE NOTICE 'ERROR. Проверка на delete_partitions. Промежуток не должен пересекаться ни с одним интервалом';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF;

    /*Должен присутствовать хотябы один create_partitions*/
    IF NOT EXISTS (
        SELECT 1 
        FROM tmp_config_part
        WHERE operation = 'create_partitions'
        LIMIT 1
    ) THEN 
        var_test_passed = false;
        RAISE NOTICE 'ERROR. Схема должна содержать хотябы один create partition';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF; 

    /*Проверка на delete_partitions и unload_to_s3_partitions не совместимые операции */
    SELECT NOT (max(delete_partitions)::bool AND max(unload_to_s3_partitions)::bool) into var_test_passed
    FROM (
        SELECT
            CASE 
                WHEN cp.operation = 'delete_partitions' THEN 1
                ELSE 0
            END AS delete_partitions,
            CASE 
                WHEN cp.operation = 'unload_to_s3_partitions' THEN 1
                ELSE 0
            END AS unload_to_s3_partitions	
        FROM tmp_config_part AS cp
        ) AS t;
    IF NOT var_test_passed THEN 
        RAISE NOTICE 'ERROR. В схеме пристутствуют delete_partitions и unload_to_s3_partitions';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF;

    /*Проверка на delete_partitions и unload_to_s3_partitions не должны пересекаться с move_partitions*/
    if EXISTS (
        SELECT 
            1
        FROM (
                SELECT 
                    lower_bound,
                    upper_bound
                FROM tmp_config_part AS cp
                WHERE cp.operation  = 'move_partitions'
            ) AS move_part
        INNER JOIN 
            (
                SELECT 
                    lower_bound , 
                    upper_bound 
                FROM tmp_config_part AS cp
                WHERE cp.operation  IN ( 'unload_to_s3_partitions', 'delete_partitions')
            ) AS oper_part
            ON  (now() - move_part.lower_bound, now() - move_part.upper_bound) OVERLAPS
                (now() - oper_part.lower_bound, now() - oper_part.upper_bound) 
    ) THEN 
        var_test_passed = false;
        RAISE NOTICE 'ERROR. delete_partitions и unload_to_s3_partitions не должны пересекаться с move_partitions';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF;

    /*Пересечение merge_partitions*/
    IF EXISTS (
        SELECT 
            mrg_part.operation, 
            mrg_part.lower_bound_ts, 
            count(1)
        FROM (
                SELECT 
                    cp.operation,
                    cp.lower_bound_ts,
                    cp.upper_bound_ts
                FROM tmp_config_part AS cp
                WHERE cp.operation = 'merge_partitions'
           ) AS mrg_part
        INNER JOIN 
            (
                SELECT 
                    lower_bound_ts, 
                    upper_bound_ts 
                FROM tmp_config_part AS cp
                WHERE cp.operation = 'merge_partitions'
            ) AS oper_part
            ON  (mrg_part.lower_bound_ts, mrg_part.upper_bound_ts) OVERLAPS
                (oper_part.lower_bound_ts,oper_part.upper_bound_ts)
        GROUP BY mrg_part.operation, mrg_part.lower_bound_ts
        HAVING count(1) > 1
    ) THEN 
        var_test_passed = false;
        RAISE NOTICE 'ERROR. Имеются пересечения по merge partition';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF;

    /*Проверка на limit для create partition и merge partition*/
    if EXISTS (
        SELECT 1
        FROM tmp_config_part
        WHERE 
            operation = 'merge_partitions'
            AND exists(
                    SELECT 1 
                    FROM tmp_config_part
                    WHERE
                        operation = 'create_partitions'
                        AND limit_operations > 0
                    )
    ) THEN
        var_test_passed = false;
        RAISE NOTICE 'ERROR. Limit в create partition нельзя задавать вместе с merge_partition';
        var_all_test_passes = var_all_test_passes and var_test_passed;
    END IF;
    
    DROP TABLE tmp_config_part;
    IF not var_all_test_passes THEN 
        raise exception 'Schema has error!';
    END IF;
    PERFORM partitioning_tool.fn_part_tools_check_is_table_ready_partitioning(
        p_schema_name,
        p_table_name
    );
end
$$;
