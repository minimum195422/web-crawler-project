from typing import Dict, List, Optional, Union, Any, Tuple
import logging
import random
import time
import requests
import threading

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

class ProxyServiceHook(BaseHook):
    """
    Hook để tương tác với dịch vụ proxy
    """
    
    conn_name_attr = 'proxy_conn_id'
    default_conn_name = 'proxy_default'
    conn_type = 'proxy'
    hook_name = 'Proxy Service'
    
    def __init__(
        self,
        proxy_conn_id: str = default_conn_name,
        api_key: Optional[Union[str, List[str]]] = None,
        api_url: Optional[str] = None,
        networks: Optional[Union[str, List[str]]] = None,
        tab_distribution: Optional[List[int]] = None
    ):
        """
        Khởi tạo hook
        
        Args:
            proxy_conn_id: ID của connection trong Airflow
            api_key: API key của dịch vụ proxy (ưu tiên hơn thông tin trong connection)
                     Có thể là chuỗi đơn hoặc danh sách API key
            api_url: URL API của dịch vụ proxy (ưu tiên hơn thông tin trong connection)
            networks: Danh sách nhà mạng (ưu tiên hơn thông tin trong connection)
            tab_distribution: Phân bổ số tab cho mỗi proxy, mặc định là phân đều
        """
        super().__init__()
        self.proxy_conn_id = proxy_conn_id
        
        # Chuyển đổi api_key thành danh sách nếu là chuỗi đơn
        if isinstance(api_key, str):
            self.api_keys = [api_key]
        elif isinstance(api_key, list):
            self.api_keys = api_key
        else:
            self.api_keys = None
            
        self.api_url = api_url
        self.networks = networks if isinstance(networks, list) else [networks] if networks else None
        self.tab_distribution = tab_distribution
        
        # Khởi tạo các tham số cho proxy instances
        self.proxy_instances = []
        
        # Nếu không cung cấp thông tin trực tiếp, lấy từ connection
        if not self.api_keys or not self.api_url:
            self._get_conn_info()
            
        # Khởi tạo các proxy instances
        self._setup_proxy_instances()
    
    def _get_conn_info(self):
        """Lấy thông tin từ connection"""
        conn = self.get_connection(self.proxy_conn_id)
        
        # Ưu tiên API keys được truyền trực tiếp
        if not self.api_keys:
            # Kiểm tra xem trong extra có danh sách api_keys không
            if 'api_keys' in conn.extra_dejson:
                api_keys_str = conn.extra_dejson.get('api_keys')
                self.api_keys = [k.strip() for k in api_keys_str.split(',')]
            else:
                # Nếu không, sử dụng password làm API key đơn
                self.api_keys = [conn.password]
        
        # Ưu tiên API URL được truyền trực tiếp
        if not self.api_url:
            self.api_url = conn.host
            if conn.port:
                self.api_url = f"{self.api_url}:{conn.port}"
            if conn.schema:
                self.api_url = f"{conn.schema}://{self.api_url}"
            
            # Thêm đường dẫn nếu có
            if conn.extra_dejson.get('path'):
                self.api_url = f"{self.api_url}{conn.extra_dejson.get('path')}"
        
        # Ưu tiên networks được truyền trực tiếp
        if not self.networks and 'networks' in conn.extra_dejson:
            networks_str = conn.extra_dejson.get('networks')
            self.networks = [n.strip() for n in networks_str.split(',')]
            
        # Ưu tiên tab_distribution được truyền trực tiếp
        if not self.tab_distribution and 'tab_distribution' in conn.extra_dejson:
            tab_dist_str = conn.extra_dejson.get('tab_distribution')
            self.tab_distribution = [int(t.strip()) for t in tab_dist_str.split(',')]
            
    def _setup_proxy_instances(self):
        """Khởi tạo các proxy instances dựa trên API keys"""
        if not self.api_keys:
            logger.warning("Không có API key nào được cung cấp")
            return
            
        # Mặc định cho tab_distribution nếu không được chỉ định
        if not self.tab_distribution:
            # Mỗi proxy sẽ phụ trách 3 tab
            self.tab_distribution = [3] * len(self.api_keys)
            
        # Kiểm tra độ dài tab_distribution
        if len(self.tab_distribution) != len(self.api_keys):
            logger.warning("Độ dài tab_distribution không khớp với số lượng API key, sẽ dùng cách phân bổ mặc định")
            self.tab_distribution = [3] * len(self.api_keys)
            
        # Khởi tạo proxy instances
        for api_key in self.api_keys:
            self.proxy_instances.append({
                "api_key": api_key,
                "proxy_data": None,
                "expiration_time": 0,
                "lock": threading.Lock(),
                "stop_refresh": False,
                "refresh_thread": None
            })
            
        logger.info(f"Đã khởi tạo {len(self.proxy_instances)} proxy instances với phân bổ tab: {self.tab_distribution}")
    
    def get_proxy_for_tab(self, tab_idx: int) -> Tuple[int, Optional[Dict]]:
        """
        Xác định proxy instance nào sẽ xử lý tab với chỉ mục nhất định
        
        Args:
            tab_idx: Chỉ mục của tab (0-based)
            
        Returns:
            Tuple (instance_idx, proxy_data)
        """
        if not self.proxy_instances:
            return -1, None
            
        instance_idx = 0
        tab_count = 0
        
        # Tìm proxy instance phù hợp dựa trên phân bổ tab
        for i, tab_limit in enumerate(self.tab_distribution):
            if tab_idx < tab_count + tab_limit:
                instance_idx = i
                break
            tab_count += tab_limit
        
        # Lấy proxy từ instance đã chọn
        proxy_data = self.get_proxy(instance_idx)
        
        logger.debug(f"Tab {tab_idx} được xử lý bởi proxy instance {instance_idx}")
        return instance_idx, proxy_data
    
    def get_proxy(self, instance_idx: int) -> Optional[Dict]:
        """
        Lấy thông tin proxy từ instance cụ thể
        
        Args:
            instance_idx: Chỉ mục của proxy instance
            
        Returns:
            Dict chứa thông tin proxy hoặc None nếu có lỗi
        """
        if instance_idx < 0 or instance_idx >= len(self.proxy_instances):
            logger.error(f"Chỉ mục proxy instance không hợp lệ: {instance_idx}")
            return None
            
        instance = self.proxy_instances[instance_idx]
        
        with instance["lock"]:
            current_time = time.time()
            
            # Nếu proxy hiện tại vẫn còn hạn sử dụng, trả về luôn
            if instance["proxy_data"] and current_time < instance["expiration_time"]:
                return instance["proxy_data"]
            
            # Ngược lại, lấy proxy mới
            return self._get_new_proxy(instance_idx)
    
    def _get_new_proxy(self, instance_idx: int) -> Optional[Dict]:
        """
        Gọi API để lấy proxy mới cho instance cụ thể
        
        Args:
            instance_idx: Chỉ mục của proxy instance
            
        Returns:
            Dict chứa thông tin proxy hoặc None nếu có lỗi
        """
        instance = self.proxy_instances[instance_idx]
        
        try:
            # Chuẩn bị tham số API
            params = {"key": instance["api_key"]}
            
            # Thêm nhà mạng nếu được chỉ định
            if self.networks:
                network = random.choice(self.networks)
                params["nhamang"] = network
            else:
                # Nếu không có danh sách nhà mạng, sử dụng "Random"
                params["nhamang"] = "Random"
            
            # Thêm tỉnh thành "0" để lấy ngẫu nhiên
            params["tinhthanh"] = "0"
            
            # Gọi API
            response = requests.get(self.api_url, params=params)
            data = response.json()
            
            # Kiểm tra kết quả
            if data.get("status") == 100:
                # Tính thời gian hết hạn
                message = data.get("message", "")
                
                # Trích xuất thời gian die từ message
                import re
                die_seconds_match = re.search(r'die sau (\d+)s', message)
                die_seconds = int(die_seconds_match.group(1)) if die_seconds_match else 1800  # mặc định 30 phút
                
                # Đặt thời gian hết hạn
                instance["expiration_time"] = time.time() + die_seconds - 10  # Trừ 10 giây để đảm bảo an toàn
                
                # Lưu proxy hiện tại
                instance["proxy_data"] = data
                
                logger.info(f"Proxy {instance_idx} - Đã nhận proxy mới: {data.get('proxyhttp')} - Hết hạn sau: {die_seconds}s")
                return data
            else:
                logger.error(f"Proxy {instance_idx} - Lỗi khi lấy proxy: {data}")
                return None
        except Exception as e:
            logger.error(f"Proxy {instance_idx} - Lỗi khi gọi API proxy: {str(e)}")
            return None
    
    def get_http_proxy(self, tab_idx: int = 0) -> Optional[str]:
        """
        Lấy proxy HTTP cho tab
        
        Args:
            tab_idx: Chỉ mục của tab
            
        Returns:
            String proxy HTTP hoặc None nếu không có proxy
        """
        _, proxy_data = self.get_proxy_for_tab(tab_idx)
        return proxy_data.get("proxyhttp") if proxy_data else None
    
    def get_formatted_http_proxy(self, tab_idx: int = 0) -> Optional[str]:
        """
        Lấy proxy HTTP ở định dạng 'http://username:password@ip:port' cho tab
        
        Args:
            tab_idx: Chỉ mục của tab
            
        Returns:
            String proxy HTTP được định dạng hoặc None nếu không có proxy
        """
        proxy_str = self.get_http_proxy(tab_idx)
        if not proxy_str:
            return None
        
        parts = proxy_str.split(":")
        if len(parts) != 4:
            return None
            
        ip, port, username, password = parts
        return f"http://{username}:{password}@{ip}:{port}"
    
    def get_requests_proxy_dict(self, tab_idx: int = 0) -> Dict[str, str]:
        """
        Lấy dictionary proxy để sử dụng với thư viện requests cho tab
        
        Args:
            tab_idx: Chỉ mục của tab
            
        Returns:
            Dictionary proxy có dạng {'http': 'http://...', 'https': 'http://...'}
        """
        formatted_proxy = self.get_formatted_http_proxy(tab_idx)
        if not formatted_proxy:
            return {}
            
        return {
            "http": formatted_proxy,
            "https": formatted_proxy
        }
    
    def start_auto_refresh(self, min_interval: int = 60, max_interval: int = 120):
        """
        Bắt đầu tự động làm mới proxy sau mỗi khoảng thời gian cho tất cả instance
        
        Args:
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        for idx, instance in enumerate(self.proxy_instances):
            instance["stop_refresh"] = False
            instance["refresh_thread"] = threading.Thread(
                target=self._auto_refresh_proxy,
                args=(idx, min_interval, max_interval),
                daemon=True
            )
            instance["refresh_thread"].start()
            logger.info(f"Proxy {idx} - Đã bắt đầu auto refresh")
    
    def _auto_refresh_proxy(self, instance_idx: int, min_interval: int, max_interval: int):
        """
        Thread tự động làm mới proxy cho một instance cụ thể
        
        Args:
            instance_idx: Chỉ mục của proxy instance
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        instance = self.proxy_instances[instance_idx]
        logger.info(f"Proxy {instance_idx} - Bắt đầu auto refresh")
        
        while not instance["stop_refresh"]:
            # Đợi một khoảng thời gian ngẫu nhiên
            wait_time = random.randint(min_interval, max_interval)
            logger.debug(f"Proxy {instance_idx} - Sẽ đổi proxy sau {wait_time} giây")
            
            # Ngủ theo từng đoạn ngắn để có thể dừng nhanh hơn khi cần
            for _ in range(wait_time):
                if instance["stop_refresh"]:
                    break
                time.sleep(1)
                
            if instance["stop_refresh"]:
                break
                
            # Lấy proxy mới
            with instance["lock"]:
                logger.info(f"Proxy {instance_idx} - Đang đổi proxy theo lịch trình...")
                old_proxy = instance["proxy_data"].get("proxyhttp") if instance["proxy_data"] else None
                instance["proxy_data"] = self._get_new_proxy(instance_idx)
                new_proxy = instance["proxy_data"].get("proxyhttp") if instance["proxy_data"] else None
                logger.info(f"Proxy {instance_idx} - Đã đổi proxy: {old_proxy} -> {new_proxy}")
    
    def stop_auto_refresh(self):
        """Dừng tự động làm mới proxy cho tất cả instance"""
        for idx, instance in enumerate(self.proxy_instances):
            instance["stop_refresh"] = True
            if instance["refresh_thread"]:
                instance["refresh_thread"].join(timeout=5)
                logger.info(f"Proxy {idx} - Đã dừng auto refresh")
                
    def test_connection(self) -> bool:
        """
        Kiểm tra kết nối với dịch vụ proxy
        
        Returns:
            True nếu kết nối thành công, False nếu có lỗi
        """
        try:
            proxy = self.get_proxy(0)
            return proxy is not None
        except Exception as e:
            logger.error(f"Lỗi khi kết nối với dịch vụ proxy: {str(e)}")
            return False