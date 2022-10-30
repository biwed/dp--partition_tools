import os
import sys
import json
from datetime import timedelta, datetime
import logging
import traceback
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import pendulum


dag_folder = os.path.dirname(os.path.abspath(__file__)) + "part_config"
sys.path.insert(0, dag_folder)

log = logging.getLogger(__name__)
# Partition processing module name 0.0.3
DAG_OWNER_NAME = "bi_lab"
CONNECTION_ID = "gp_db"
POOL = "partitioning"
PG_TARGET_CONNECTION_ID = 'bi_bot'
MODULE_NAME = "partitioning_tables"


with DAG(
    dag_id=MODULE_NAME + '__yoyo__migration',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["partition tables", "migration"],
) as dag:
    @task(task_id="migration")
    def yoyo_migratioon(): 
        from yoyo import read_migrations 
        from yoyo import get_backend 
        """Функция инициализирует клиента, для работы с S3. 
        """ 
        log.info('Create migration')
        yoyo_uri = BaseHook.get_connection(PG_TARGET_CONNECTION_ID).get_uri() 
        log.info('Get connection')
        migrations = read_migrations('/yoyo/partitioning_tool') 
        backend = get_backend(yoyo_uri) 
        with backend.lock(): 
            logging.info("Migration") 
            backend.apply_migrations(backend.to_apply(migrations))
    log.info('Start exec migration')
    start = yoyo_migratioon()

