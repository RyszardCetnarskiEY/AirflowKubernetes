from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='hello_world',
    default_args=default_args,
    description='My custom DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
)
def hello_world():
    """A simple tutorial DAG"""

    @task.bash(
        task_id='hello_world_task',
    )
    def hello_task():
        """Echo a hello message"""
        return 'echo "Hello from my custom DAG!"'

    # Trigger the task
    hello_task()

# Instantiate the DAG
dag = hello_world()
