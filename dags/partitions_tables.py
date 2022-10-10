import os
import sys
import json
from datetime import timedelta, datetime
import logging
import traceback
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook


dag_folder = os.path.dirname(os.path.abspath(__file__)) + "part_config"
sys.path.insert(0, dag_folder)


# Partition processing module name 0.0.3
DAG_OWNER_NAME = "bi_lab"
CONNECTION_ID = "gp_db"
POOL = "partitioning"
PG_TARGET_CONNECTION_ID = 'bi_bot'

def yoyo_migratioon(): 
    from yoyo import read_migrations 
    from yoyo import get_backend 
    """Функция инициализирует клиента, для работы с S3. 
    """ 
    yoyo_uri = BaseHook.get_connection(PG_TARGET_CONNECTION_ID).get_uri() 
    migrations = read_migrations('/yoyo/migrations') 
    backend = get_backend(yoyo_uri) 
    with backend.lock(): 
        logging.info("Migration") 
        backend.apply_migrations(backend.to_apply(migrations))




def create_sql_query(table_name: str, schema_name: str, meta_object: dict) -> str:
    """
    Функция создает sql запрос для каждого оператора
    """
    sql_query = f"SELECT partitining_tool.fn_part_tools_{meta_object['operation']}"
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


def create_dag(dag_id, config):
    try:
        schema_name = config['schema']
        tags = config['tags']
        start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')
        access_list = config['access_control']
        access_control = {item: {'can_dag_edit'} for item in access_list}
        default_args = {
            'owner': config['owner'],
            'depends_on_past': False,
            'start_date': start_date,
            'retries': 4,
            'retry_delay': timedelta(minutes=1),
            'queue': 'partitioning',
            'email': config['email'],
            'email_on_failure': True,
            'email_on_retry': False,
            'pool': POOL
        }
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=config['schedule'],
            max_active_runs=1,
            tags=tags,
            access_control=access_control
        )
        for table_spec in config['tables']:
            table_name = table_spec['table']
            table_verif = PostgresOperator(
                task_id=f'{table_name}_check_config',
                sql=f"""select partitining_tool.fn_part_tools_check_config(
                    p_schema_name := '{schema_name}',
                    p_table_name := '{table_name}',
                    p_config := '{json.dumps(table_spec['operations'])}'::json
                )""",
                postgres_conn_id=CONNECTION_ID,
                dag=dag
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
                    postgres_conn_id=CONNECTION_ID,
                    dag=dag
                )
                cur_vertex >> table_operations
                cur_vertex = table_operations
        return dag
    except Exception as e:
        logging.error(f"Unable to create DAG {dag_id}: {traceback.format_exc()}")
        logging.error(e)


if base_url is not None:
    schema_list = get_meta(base_url, api_endpoint, "dataplatform/processes/partitioning/v_2?list=true")
    for schema in schema_list:
        conf = get_meta(base_url, api_endpoint, f"dataplatform/processes/partitioning/v_2/{schema}")
        dag_id = MODULE_NAME + '__' + schema
        dag = create_dag(
                dag_id,
                conf
            )
        if dag is not None:
            globals()[dag_id] = dag
