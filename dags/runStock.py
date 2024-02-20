import sys
# sys.path.insert(0, "/opt/airflow/")
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date, timedelta

default_args = {
    'owner': 'DatDao',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 2, 19),
}

def crawl_spider():
    from Crawl.Stock.spiders.Stock_Daily import StockDailySpider

with DAG(
    dag_id='CrawlStock_2',
    default_args=default_args,
    schedule_interval='@daily',
) as dag: 
    Bash_test = BashOperator(task_id='bash_test', bash_command = "cd ${AIRFLOW_HOME} && ls")
    Bash_test_1 = BashOperator(task_id='bash_test_1', bash_command = "cd ${AIRFLOW_HOME}/Crawl && ls && scrapy crawl stockdaily") #/opt/ariflow/Crawl scrapy crawl stockdaily #-a fromDate=31012024 -a toDate=16022024
    Bash_test_2 = BashOperator(task_id='bash_test_2', bash_command = "cd ${AIRFLOW_HOME}/Crawl && ls && python modelData.py")
    # model_task = PythonOperator(task_id='ModelData', python_callable=ModelData)

# scrapy_task.LOG_LEVEL='DEBUG'
Bash_test >> Bash_test_1 >> Bash_test_2