"""
DAG chính cho việc điều phối tất cả các crawler
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.constants import DEFAULT_DAG_ARGS, DEFAULT_SCHEDULE_INTERVAL

# Cấu hình logging
logger = logging.getLogger(__name__)

# Tạo DAG
dag = DAG(
    'crawler_coordinator',
    default_args=DEFAULT_DAG_ARGS,
    description='Điều phối tất cả các crawler',
    schedule_interval=DEFAULT_SCHEDULE_INTERVAL,
    start_date=days_ago(1),
    catchup=False,
    tags=['crawler', 'coordinator'],
)

# Danh sách các DAG cần điều phối
crawler_dags = [
    {
        'dag_id': 'lazada_crawler',
        'description': 'Crawler dữ liệu từ Lazada',
        'wait_for_completion': True
    },
    # Thêm các crawler khác ở đây khi cần
    # {
    #     'dag_id': 'shopee_crawler',
    #     'description': 'Crawler dữ liệu từ Shopee',
    #     'wait_for_completion': True
    # },
]

# Task bắt đầu quá trình crawl
start_task = BashOperator(
    task_id='start_crawling',
    bash_command='echo "Bắt đầu quá trình crawl dữ liệu: $(date)"',
    dag=dag,
)

# Tạo các task kích hoạt mỗi crawler
trigger_tasks = []
sensor_tasks = []

for idx, crawler in enumerate(crawler_dags):
    # Task kích hoạt DAG crawler
    trigger_task = TriggerDagRunOperator(
        task_id=f"trigger_{crawler['dag_id']}",
        trigger_dag_id=crawler['dag_id'],
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=False,  # Không chờ trong task này, sẽ sử dụng sensor riêng
        dag=dag,
    )
    trigger_tasks.append(trigger_task)
    
    # Nếu cần đợi DAG hoàn thành
    if crawler.get('wait_for_completion', False):
        sensor_task = ExternalTaskSensor(
            task_id=f"wait_for_{crawler['dag_id']}",
            external_dag_id=crawler['dag_id'],
            external_task_id=None,  # Đợi toàn bộ DAG hoàn thành
            execution_date_fn=lambda dt: dt,  # Sử dụng cùng execution_date
            timeout=3600,  # Timeout sau 1 giờ
            mode='reschedule',  # Reschedule nếu chưa hoàn thành
            poke_interval=60,  # Kiểm tra mỗi 60 giây
            dag=dag,
        )
        sensor_tasks.append(sensor_task)
        # Thiết lập phụ thuộc: trigger -> sensor
        trigger_task >> sensor_task

# Hàm tổng kết kết quả crawl
def summarize_results(**context):
    """
    Tổng kết kết quả của tất cả các crawler
    """
    execution_date = context['execution_date']
    
    logger.info(f"Tổng kết kết quả crawl cho ngày {execution_date.strftime('%Y-%m-%d')}")
    
    # Thực hiện các thao tác tổng kết ở đây
    # ...
    
    return {
        'crawl_date': execution_date.strftime('%Y-%m-%d'),
        'total_crawlers': len(crawler_dags),
        'completed_time': datetime.now().isoformat()
    }

# Task kết thúc quá trình crawl
end_task = PythonOperator(
    task_id='end_crawling',
    python_callable=summarize_results,
    provide_context=True,
    dag=dag,
)

# Thiết lập thứ tự chạy
start_task >> trigger_tasks

# Nếu có các sensor task, kết nối chúng với end_task
if sensor_tasks:
    for sensor_task in sensor_tasks:
        sensor_task >> end_task
else:
    # Nếu không có sensor, kết nối trực tiếp từ trigger đến end
    for trigger_task in trigger_tasks:
        trigger_task >> end_task