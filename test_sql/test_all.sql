DROP TABLE IF EXISTS partitioning_tools_test.item_loc_soh_test;
CREATE TABLE partitioning_tools_test.item_loc_soh_test
(LIKE rms_p009qtzb_rms_ods.item_loc_soh) 
WITH (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
PARTITION BY RANGE (created_dttm) (DEFAULT PARTITION other);
ALTER TABLE partitioning_tools_test.item_loc_soh_test  OWNER TO partitioning_tool_owner;

BEGIN;
INSERT INTO partitioning_tools_test.item_loc_soh_test (nk, item, item_parent, item_grandparent, loc, loc_type, av_cost, unit_cost, stock_on_hand, soh_update_datetime, last_hist_export_date, in_transit_qty, pack_comp_intran, pack_comp_soh, tsf_reserved_qty, pack_comp_resv, tsf_expected_qty, pack_comp_exp, rtv_qty, non_sellable_qty, customer_resv, customer_backorder, pack_comp_cust_resv, pack_comp_cust_back, create_datetime, last_update_datetime, last_update_id, first_received, last_received, qty_received, first_sold, last_sold, qty_sold, primary_supp, primary_cntry, average_weight, finisher_av_retail, finisher_units, pack_comp_non_sellable, valid_from_dttm, created_dttm, updated_dttm, md5_hash, is_actual)
SELECT nk, item, item_parent, item_grandparent, loc, loc_type, av_cost, unit_cost, stock_on_hand, soh_update_datetime, last_hist_export_date, in_transit_qty, pack_comp_intran, pack_comp_soh, tsf_reserved_qty, pack_comp_resv, tsf_expected_qty, pack_comp_exp, rtv_qty, non_sellable_qty, customer_resv, customer_backorder, pack_comp_cust_resv, pack_comp_cust_back, create_datetime, last_update_datetime, last_update_id, first_received, last_received, qty_received, first_sold, last_sold, qty_sold, primary_supp, primary_cntry, average_weight, finisher_av_retail, finisher_units, pack_comp_non_sellable, valid_from_dttm, created_dttm, updated_dttm, md5_hash, is_actual
FROM rms_p009qtzb_rms_ods.item_loc_soh;
END;

ANALYZE partitioning_tools_test.item_loc_soh_test;
-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;

select partitioning_tool.fn_part_tools_check_config(
    p_schema_name := 'partitioning_tools_test',
    p_table_name := 'item_loc_soh_test',
    p_config := '[{"granularity": "1 year", "lower_bound": "2 year", "operation": "create_partitions", "upper_bound": "10 month"}, {"granularity": "3 month", "lower_bound": "1 year", "operation": "create_partitions", "upper_bound": "1 month"}, {"granularity": "1 month", "lower_bound": "3 month", "operation": "create_partitions", "upper_bound": "-3 month"}, {"access_exclusive_mode": false, "granularity": "3 month", "limit_operations": 2, "lower_bound": "1 year", "operation": "merge_partitions", "table_space": "warm", "upper_bound": "3 month"}, {"access_exclusive_mode": false, "limit_operations": 2, "lower_bound": "5 year", "operation": "move_partitions", "table_space": "warm", "upper_bound": "1 year"}]'::json
)


SELECT partitioning_tool.fn_part_tools_create_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_granularity := '1 year'::interval, 
	p_lower_bound := '2 year'::interval, 
	p_upper_bound := '10 month'::INTERVAL
);

-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;

select
	*
from
	pg_catalog.pg_partitions p
where
	p.schemaname = 'partitioning_tools_test'
and p.tablename = 'item_loc_soh_test'; 

SELECT partitioning_tool.fn_part_tools_create_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_granularity := '3 month'::interval, 
	p_lower_bound := '1 year'::interval, 
	p_upper_bound := '1 month'::INTERVAL
);


-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;

select
	*
from
	pg_catalog.pg_partitions p
where
	p.schemaname = 'partitioning_tools_test'
and p.tablename = 'item_loc_soh_test'; 

SELECT partitioning_tool.fn_part_tools_create_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_granularity := '1 month'::interval, 
	p_lower_bound := '3 month'::interval, 
	p_upper_bound := '-3 month'::INTERVAL
);

-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;

SELECT partitioning_tool.fn_part_tools_merge_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_granularity := '3 month'::interval, 
	p_limit_operations := 2, 
	p_lower_bound := '1 year'::interval, 
	p_table_space := 'warm', 
	p_upper_bound := '3 month'::INTERVAL
);

-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;

select
	*
from
	pg_catalog.pg_partitions p
where
	p.schemaname = 'partitioning_tools_test'
and p.tablename = 'item_loc_soh_test'; 

SELECT partitioning_tool.fn_part_tools_move_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_limit_operations := 2, 
	p_lower_bound := '5 year'::interval, 
	p_table_space := 'warm', 
	p_upper_bound := '1 year'::interval);

-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;


select
	*
from
pg_catalog.pg_partitions p
where
p.schemaname = 'partitioning_tools_test'
and p.tablename = 'item_loc_soh_test'; 

SELECT partitioning_tool.fn_part_tools_unload_to_s3_partitions(
	p_table_name := 'item_loc_soh_test', 
	p_schema_name:= 'partitioning_tools_test', 
	p_lower_bound := '5 year'::interval, 
	p_s3_server_name := 's3srv', 
	p_upper_bound := '1 year'::interval);
	
-- 438219082	118541203510168
SELECT count(*), sum(loc)
FROM partitioning_tools_test.item_loc_soh_test;


select
	*
from
pg_catalog.pg_partitions p
where
p.schemaname = 'partitioning_tools_test'
and p.tablename = 'item_loc_soh_test'; 

select *
from partitioning_tool.fn_part_tools_get_part_table_spase('partitioning_tools_test', 'item_loc_soh_test')

explain
SELECT count(*)
FROM partitioning_tools_test.item_loc_soh_test;

--62434390
SELECT count(*)
FROM partitioning_tools_test.item_loc_soh_test
WHERE created_dttm < '2021-01-01'::date 
	AND created_dttm >= '2020-01-01'::date  

select partitioning_tool.fn_part_tools_delete_partitions(
		'partitioning_tools_test' ,
        'item_loc_soh_test',
        '6 year'::interval, 
        '1 year'::INTERVAL,
        15
       );

select *
from partitioning_tool.fn_part_tools_get_part_table_spase('partitioning_tools_test', 'item_loc_soh_test')

-- select 438219082 - 62434390 = 375784692
SELECT count(*)
FROM partitioning_tools_test.item_loc_soh_test;