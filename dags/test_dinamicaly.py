from datetime import datetime
from airflow.decorators import dag, task

configs = {
    "config1": {"message": "first DAG will receive this message"},
    "config2": {"message": "second DAG will receive this message"},
}

for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"

    @dag(dag_id=dag_id, start_date=datetime(2022, 2, 1))
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(message)

        print_message(config["message"])

    globals()[dag_id] = dynamic_generated_dag()

