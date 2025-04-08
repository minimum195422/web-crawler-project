from typing import Dict, List, Optional, Any
import logging
import os
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class GenericCrawlerOperator(BaseOperator):
    """
    Operator tùy chỉnh để thực hiện các tác vụ crawl với bất kỳ class crawler nào
    """
    
    @apply_defaults
    def __init__(
        self,
        crawler_class,
        crawler_kwargs: Dict = None,
        crawler_method: str = 'run',
        method_kwargs: Dict = None,
        proxy_manager_class = None,
        proxy_manager_kwargs: Dict = None,
        data_dir: str = '/opt/airflow/data',
        output_key: str = 'crawler_result',
        *args, **kwargs
    ):
        """
        Khởi tạo operator
        
        Args:
            crawler_class: Lớp crawler để khởi tạo
            crawler_kwargs: Tham số cho crawler
            crawler_method: Phương thức của crawler để gọi
            method_kwargs: Tham số cho phương thức
            proxy_manager_class: Lớp quản lý proxy (tùy chọn)
            proxy_manager_kwargs: Tham số cho proxy manager
            data_dir: Thư mục để lưu dữ liệu
            output_key: Khóa để lưu kết quả trong XCom
        """
        super().__init__(*args, **kwargs)
        self.crawler_class = crawler_class
        self.crawler_kwargs = crawler_kwargs or {}
        self.crawler_method = crawler_method
        self.method_kwargs = method_kwargs or {}
        self.proxy_manager_class = proxy_manager_class
        self.proxy_manager_kwargs = proxy_manager_kwargs or {}
        self.data_dir = data_dir
        self.output_key = output_key
        
        # Đảm bảo thư mục đầu ra tồn tại
        os.makedirs(data_dir, exist_ok=True)
    
    def execute(self, context):
        """
        Thực thi tác vụ
        
        Args:
            context: Context của task
            
        Returns:
            Kết quả của crawler
        """
        logger.info(f"Bắt đầu crawl sử dụng {self.crawler_class.__name__}")
        
        proxy_manager = None
        
        try:
            # Khởi tạo proxy manager nếu được cung cấp
            if self.proxy_manager_class:
                logger.info(f"Khởi tạo proxy manager: {self.proxy_manager_class.__name__}")
                proxy_manager = self.proxy_manager_class(**self.proxy_manager_kwargs)
                
                # Bắt đầu tự động refresh proxy nếu có phương thức này
                if hasattr(proxy_manager, 'start_auto_refresh'):
                    proxy_manager.start_auto_refresh()
            
            # Thêm proxy manager vào crawler_kwargs nếu có
            if proxy_manager:
                self.crawler_kwargs['proxy_manager'] = proxy_manager
            
            # Đảm bảo thư mục đầu ra được thiết lập
            self.crawler_kwargs['output_dir'] = self.data_dir
            
            # Khởi tạo crawler
            logger.info(f"Khởi tạo crawler: {self.crawler_class.__name__}")
            crawler = self.crawler_class(**self.crawler_kwargs)
            
            # Gọi phương thức crawl
            logger.info(f"Gọi phương thức {self.crawler_method} trên crawler")
            method = getattr(crawler, self.crawler_method)
            result = method(**self.method_kwargs)
            
            # Lưu kết quả vào XCom
            logger.info(f"Crawl hoàn tất, lưu kết quả với key {self.output_key}")
            return {self.output_key: result}
            
        except Exception as e:
            logger.error(f"Lỗi khi thực hiện crawl: {str(e)}")
            raise e
        
        finally:
            # Dừng proxy manager nếu đã khởi tạo
            if proxy_manager and hasattr(proxy_manager, 'stop_auto_refresh'):
                logger.info("Dừng proxy manager auto refresh")
                proxy_manager.stop_auto_refresh()


class LazadaCrawlerOperator(GenericCrawlerOperator):
    """
    Operator tùy chỉnh cho việc crawl dữ liệu từ Lazada
    """
    
    @apply_defaults
    def __init__(
        self,
        max_products: int = 100,
        max_concurrent_tabs: int = 6,  # Tăng lên 6 tab (3 tab cho mỗi proxy)
        min_request_interval: int = 25,
        max_request_interval: int = 40,
        headless: bool = True,
        homepage_url: str = "https://www.lazada.vn/",
        proxy_api_keys: Optional[List[str]] = None,
        proxy_api_key: Optional[str] = None,  # Giữ lại để tương thích ngược
        proxy_networks: Optional[List[str]] = None,
        *args, **kwargs
    ):
        """
        Khởi tạo Lazada crawler operator
        
        Args:
            max_products: Số sản phẩm tối đa cần crawl
            max_concurrent_tabs: Số tab tối đa mở cùng lúc
            min_request_interval: Thời gian tối thiểu giữa các request (giây)
            max_request_interval: Thời gian tối đa giữa các request (giây)
            headless: Chạy Chrome ở chế độ không có giao diện
            homepage_url: URL trang chủ Lazada
            proxy_api_keys: Danh sách API key cho các dịch vụ proxy
            proxy_api_key: API key cho dịch vụ proxy (tương thích ngược)
            proxy_networks: Danh sách nhà mạng cho proxy
        """
        # Import ở đây để tránh import circular
        from lazada.crawler_lazada import LazadaCrawler
        from common.proxy_manager import ProxyManager
        
        # Chuẩn bị crawler_kwargs
        crawler_kwargs = {
            'max_products': max_products,
            'max_concurrent_tabs': max_concurrent_tabs,
            'min_request_interval': min_request_interval,
            'max_request_interval': max_request_interval,
            'headless': headless,
        }
        
        # Chuẩn bị method_kwargs
        method_kwargs = {
            'homepage_url': homepage_url
        }
        
        # Chuẩn bị proxy_manager_kwargs
        proxy_manager_class = None
        proxy_manager_kwargs = None
        
        # Ưu tiên proxy_api_keys nếu được cung cấp
        if proxy_api_keys:
            proxy_manager_class = ProxyManager
            proxy_manager_kwargs = {
                'api_key': proxy_api_keys,
                'networks': proxy_networks,
                'tab_distribution': [3, 3]  # 3 tab cho mỗi proxy
            }
        # Nếu không, sử dụng proxy_api_key đơn nếu có
        elif proxy_api_key:
            proxy_manager_class = ProxyManager
            proxy_manager_kwargs = {
                'api_key': proxy_api_key,
                'networks': proxy_networks
            }
        
        super().__init__(
            crawler_class=LazadaCrawler,
            crawler_kwargs=crawler_kwargs,
            crawler_method='run',
            method_kwargs=method_kwargs,
            proxy_manager_class=proxy_manager_class,
            proxy_manager_kwargs=proxy_manager_kwargs,
            *args, **kwargs
        )