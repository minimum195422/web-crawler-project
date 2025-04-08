"""
DAG cho việc crawl dữ liệu từ Lazada
"""
import os
import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from lazada.crawler_lazada import LazadaCrawler
from lazada.parser_lazada import LazadaParser
from common.proxy_manager import ProxyManager

# Cấu hình logging
logger = logging.getLogger(__name__)

# Định nghĩa tham số mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': int(os.getenv('DEFAULT_RETRIES', '3')),
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 8),
}

# Lấy cấu hình từ biến môi trường
PROXY_API_KEY = os.getenv('PROXY_API_KEY')
PROXY_API_URL = os.getenv('PROXY_API_URL', 'https://proxyxoay.shop/api/get.php')
PROXY_NETWORKS = os.getenv('PROXY_NETWORKS', 'fpt,viettel').split(',')
PROXY_MIN_INTERVAL = int(os.getenv('PROXY_MIN_INTERVAL', '60'))
PROXY_MAX_INTERVAL = int(os.getenv('PROXY_MAX_INTERVAL', '120'))
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = os.getenv('S3_PREFIX', 'crawled-data')
S3_IMAGE_PREFIX = os.getenv('S3_IMAGE_PREFIX', 'images')
MAX_PRODUCTS = int(os.getenv('MAX_PRODUCTS', '100'))
DATA_DIR = "/opt/airflow/data"
PROCESS_IMAGES = os.getenv('PROCESS_IMAGES', 'true').lower() == 'true'

# Tạo DAG
dag = DAG(
    'lazada_crawler',
    default_args=default_args,
    description='Crawl dữ liệu sản phẩm từ Lazada',
    schedule_interval=timedelta(hours=2),  # Chạy hàng ngày
    catchup=False,
    tags=['crawler', 'lazada', 'ecommerce'],
)

def crawl_lazada(**context):
    """
    Hàm crawl dữ liệu từ Lazada
    """
    # Lấy thông tin ngày chạy
    execution_date = context['execution_date']
    formatted_date = execution_date.strftime('%Y-%m-%d')
    
    # Tạo proxy manager
    proxy_manager = None
    
    # Kiểm tra xem có nhiều API key không
    proxy_api_keys = os.getenv('PROXY_API_KEYS')
    if proxy_api_keys:
        # Phân tách các API key (được phân cách bởi dấu phẩy)
        api_keys = [key.strip() for key in proxy_api_keys.split(',')]
        
        # Tạo proxy manager với nhiều API key
        if len(api_keys) > 0:
            proxy_manager = ProxyManager(
                api_key=api_keys,
                networks=PROXY_NETWORKS,
                base_url=PROXY_API_URL,
                tab_distribution=[3, 3]  # 3 tab cho mỗi proxy
            )
            proxy_manager.start_auto_refresh(
                min_interval=PROXY_MIN_INTERVAL,
                max_interval=PROXY_MAX_INTERVAL
            )
            logger.info(f"Đã khởi tạo proxy manager với {len(api_keys)} API key")
    else:
        # Sử dụng API key đơn nếu có
        if PROXY_API_KEY:
            proxy_manager = ProxyManager(
                api_key=PROXY_API_KEY,
                networks=PROXY_NETWORKS,
                base_url=PROXY_API_URL
            )
            proxy_manager.start_auto_refresh(
                min_interval=PROXY_MIN_INTERVAL,
                max_interval=PROXY_MAX_INTERVAL
            )
            logger.info("Đã khởi tạo proxy manager với 1 API key")
    
    try:
        # Khởi tạo crawler
        crawler = LazadaCrawler(
            proxy_manager=proxy_manager,
            max_concurrent_tabs=6,  # Tăng lên 6 tab (3 tab cho mỗi proxy)
            min_request_interval=25,
            max_request_interval=40,
            max_products=MAX_PRODUCTS,
            headless=True,
            output_dir=DATA_DIR
        )
        
        # Chạy crawler
        results = crawler.run()
        
        logger.info(f"Đã crawl được {len(results)} sản phẩm")
        
        # Lưu danh sách sản phẩm tổng hợp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = f"{DATA_DIR}/lazada_results_{timestamp}.json"
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # Truyền tên file kết quả cho task tiếp theo
        context['ti'].xcom_push(key='result_file', value=result_file)
        context['ti'].xcom_push(key='crawl_count', value=len(results))
        
        return result_file
    
    finally:
        # Dừng proxy manager nếu đã khởi tạo
        if proxy_manager:
            proxy_manager.stop_auto_refresh()

def parse_data(**context):
    """
    Hàm xử lý dữ liệu đã crawl
    """
    # Lấy thông tin từ task trước
    ti = context['ti']
    crawl_count = ti.xcom_pull(key='crawl_count', task_ids='crawl_lazada')
    
    logger.info(f"Xử lý {crawl_count} sản phẩm đã crawl")
    
    # Khởi tạo parser
    parser = LazadaParser(
        input_dir=DATA_DIR,
        output_dir=DATA_DIR,
        s3_bucket=S3_BUCKET_NAME,
        s3_prefix=S3_IMAGE_PREFIX,
        process_images=PROCESS_IMAGES
    )
    
    # Chạy parser
    result = parser.run(save_json=True, save_csv=True)
    
    logger.info(f"Kết quả xử lý: {result}")
    
    # Truyền thông tin file kết quả cho task tiếp theo
    if 'json_file' in result:
        context['ti'].xcom_push(key='json_file', value=result['json_file'])
    
    if 'csv_file' in result:
        context['ti'].xcom_push(key='csv_file', value=result['csv_file'])
    
    return result

def upload_to_s3(**context):
    """
    Hàm tải dữ liệu lên S3
    """
    # Kiểm tra thông tin S3
    if not S3_BUCKET_NAME:
        logger.warning("S3_BUCKET_NAME không được cấu hình, bỏ qua việc tải lên S3")
        return None
    
    # Lấy thông tin từ task trước
    ti = context['ti']
    json_file = ti.xcom_pull(key='json_file', task_ids='parse_data')
    csv_file = ti.xcom_pull(key='csv_file', task_ids='parse_data')
    
    # Tạo kết nối S3
    s3_hook = S3Hook()
    
    # Upload các file lên S3
    uploaded_files = []
    
    if json_file:
        try:
            # Tạo key cho file trên S3
            json_filename = os.path.basename(json_file)
            s3_json_key = f"{S3_PREFIX}/lazada/{datetime.now().strftime('%Y/%m/%d')}/{json_filename}"
            
            # Upload file
            s3_hook.load_file(
                filename=json_file,
                key=s3_json_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            
            logger.info(f"Đã tải lên file JSON: s3://{S3_BUCKET_NAME}/{s3_json_key}")
            uploaded_files.append(s3_json_key)
            
        except Exception as e:
            logger.error(f"Lỗi khi tải file JSON lên S3: {str(e)}")
    
    if csv_file:
        try:
            # Tạo key cho file trên S3
            csv_filename = os.path.basename(csv_file)
            s3_csv_key = f"{S3_PREFIX}/lazada/{datetime.now().strftime('%Y/%m/%d')}/{csv_filename}"
            
            # Upload file
            s3_hook.load_file(
                filename=csv_file,
                key=s3_csv_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            
            logger.info(f"Đã tải lên file CSV: s3://{S3_BUCKET_NAME}/{s3_csv_key}")
            uploaded_files.append(s3_csv_key)
            
        except Exception as e:
            logger.error(f"Lỗi khi tải file CSV lên S3: {str(e)}")
    
    return uploaded_files

# Tạo các task
crawl_task = PythonOperator(
    task_id='crawl_lazada',
    python_callable=crawl_lazada,
    provide_context=True,
    dag=dag,
)

parse_task = PythonOperator(
    task_id='parse_data',
    python_callable=parse_data,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Thiết lập thứ tự chạy các task
crawl_task >> parse_task >> upload_task