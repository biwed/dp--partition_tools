import os
import sys
import json
import yaml
import glob
from datetime import timedelta, datetime
import logging
import traceback
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task




# Partition processing module name 0.0.3
DAG_OWNER_NAME = "bi_lab"
CONNECTION_ID = "gp_db"
POOL = "partitioning"
PG_TARGET_CONNECTION_ID = 'bi_bot'
MODULE_NAME = "partitioning_tool"


def create_sql_query(table_name: str, schema_name: str, meta_object: dict) -> str:
    """
    Функция создает sql запрос для каждого оператора
    """
    sql_query = f"SELECT partitioning_tool.fn_part_tools_{meta_object['operation']}"
    sql_query += f"(p_table_name := '{table_name}', p_schema_name:= '{schema_name}'"
        
    for key, value in meta_object.items():
        if key in ('granularity', 'lower_bound', 'upper_bound'):
            sql_query += f", p_{key} := '{value}'::interval"
        if key in ('table_space', 's3_server_name', 's3_bucket'):
            sql_query += f", p_{key} := '{value}'"
        if key in ('limit_operations', 'transactions_mode'):
            sql_query += f", p_{key} := {value}".lower()
    sql_query += ");"
    return sql_query


def get_order(operations):
    orders = {
        'create_partitions': 0,
        'merge_partitions': 1,
        'move_partitions': 2,
        'unload_to_s3_partitions': 3,
        'delete_partitions': 4
    }
    item = operations.get('operation')
    return orders[item]



def get_dag_key(path, file_name: str)-> str:
    key_file = (file_name [len(path):]).replace("/","_").replace('.yaml','')
    return key_file

config_filepath = f'dags/partitioning_configs/'

file_contents = []
for filename in glob.glob(f'{config_filepath}*/*.yaml', recursive=True):
    dag_id = MODULE_NAME + "_" + get_dag_key(config_filepath, filename)
    with open(filename) as python_file:
        file_contents.append(
            (dag_id,
            yaml.safe_load(python_file))
        )
logging.info(file_contents)
logging.info('Create partitonin_DAG')
if file_contents is None:
    @dag(
        dag_id="error_file", 
        start_date=datetime(2022, 2, 1),
        tags=["partition_tables", "error"]
        )
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(message)

        print_message(file_contents)
    globals()[dag_id] = dynamic_generated_dag()
else:
    for (dag_id, config) in file_contents:
        access_list = config['access_control']
        @dag(
            dag_id=dag_id, 
            start_date=datetime.strptime(config['start_date'], '%Y-%m-%d'),
            tags = config['tags']
            )
        def create_partitioning_dag():
            logging.info(config)
            schema_name = config['schema']
            for table_spec in config['tables']:
                table_name = table_spec['table']
                table_verif = PostgresOperator(
                    task_id=f'{table_name}_check_config',
                    sql=f"""select partitioning_tool.fn_part_tools_check_config(
                        p_schema_name := '{schema_name}',
                        p_table_name := '{table_name}',
                        p_config := '{json.dumps(table_spec['operations'])}'::json
                    )""",
                    postgres_conn_id=PG_TARGET_CONNECTION_ID,
                    pool=POOL
                )
                operations_config = table_spec['operations']
                operations_config.sort(key=get_order)
                cur_vertex = table_verif
                for operation_id, oparetion_spec in enumerate(operations_config):
                    sql_query = create_sql_query(
                        schema_name=schema_name,
                        table_name=table_name,
                        meta_object=oparetion_spec,
                    )
                    table_operations = PostgresOperator(
                        task_id=f"{table_name}_{oparetion_spec['operation']}_{operation_id}",
                        sql=sql_query,
                        postgres_conn_id=PG_TARGET_CONNECTION_ID,
                        pool=POOL
                    )
                    cur_vertex >> table_operations
                    cur_vertex = table_operations
        
        globals()[dag_id]=create_partitioning_dag()
