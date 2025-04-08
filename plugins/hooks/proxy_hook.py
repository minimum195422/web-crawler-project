from typing import Dict, List, Optional, Union, Any
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
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        networks: Optional[Union[str, List[str]]] = None
    ):
        """
        Khởi tạo hook
        
        Args:
            proxy_conn_id: ID của connection trong Airflow
            api_key: API key của dịch vụ proxy (ưu tiên hơn thông tin trong connection)
            api_url: URL API của dịch vụ proxy (ưu tiên hơn thông tin trong connection)
            networks: Danh sách nhà mạng (ưu tiên hơn thông tin trong connection)
        """
        super().__init__()
        self.proxy_conn_id = proxy_conn_id
        self.api_key = api_key
        self.api_url = api_url
        self.networks = networks if isinstance(networks, list) else [networks] if networks else None
        
        self.current_proxy = None
        self.expiration_time = 0
        self.lock = threading.Lock()
        
        # Nếu không cung cấp thông tin trực tiếp, lấy từ connection
        if not self.api_key or not self.api_url:
            self._get_conn_info()
    
    def _get_conn_info(self):
        """Lấy thông tin từ connection"""
        conn = self.get_connection(self.proxy_conn_id)
        
        # Ưu tiên API key được truyền trực tiếp
        if not self.api_key:
            self.api_key = conn.password
        
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
    
    def get_proxy(self) -> Optional[Dict]:
        """
        Lấy thông tin proxy
        
        Returns:
            Dict chứa thông tin proxy hoặc None nếu có lỗi
        """
        with self.lock:
            current_time = time.time()
            
            # Nếu proxy hiện tại vẫn còn hạn sử dụng, trả về luôn
            if self.current_proxy and current_time < self.expiration_time:
                return self.current_proxy
            
            # Ngược lại, lấy proxy mới
            return self._get_new_proxy()
    
    def _get_new_proxy(self) -> Optional[Dict]:
        """
        Gọi API để lấy proxy mới
        
        Returns:
            Dict chứa thông tin proxy hoặc None nếu có lỗi
        """
        try:
            # Chuẩn bị tham số API
            params = {"key": self.api_key}
            
            # Thêm nhà mạng nếu được chỉ định
            if self.networks:
                network = random.choice(self.networks)
                params["nhamang"] = network
            
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
                self.expiration_time = time.time() + die_seconds - 10  # Trừ 10 giây để đảm bảo an toàn
                
                # Lưu proxy hiện tại
                self.current_proxy = data
                
                logger.info(f"Đã nhận proxy mới: {data.get('proxyhttp')} - Hết hạn sau: {die_seconds}s")
                return data
            else:
                logger.error(f"Lỗi khi lấy proxy: {data}")
                return None
        except Exception as e:
            logger.error(f"Lỗi khi gọi API proxy: {str(e)}")
            return None
    
    def get_http_proxy(self) -> Optional[str]:
        """
        Lấy proxy HTTP ở định dạng 'ip:port:username:password'
        
        Returns:
            String proxy HTTP hoặc None nếu không có proxy
        """
        proxy_data = self.get_proxy()
        return proxy_data.get("proxyhttp") if proxy_data else None
    
    def get_formatted_http_proxy(self) -> Optional[str]:
        """
        Lấy proxy HTTP ở định dạng 'http://username:password@ip:port'
        
        Returns:
            String proxy HTTP được định dạng hoặc None nếu không có proxy
        """
        proxy_str = self.get_http_proxy()
        if not proxy_str:
            return None
        
        parts = proxy_str.split(":")
        if len(parts) != 4:
            return None
            
        ip, port, username, password = parts
        return f"http://{username}:{password}@{ip}:{port}"
    
    def get_socks5_proxy(self) -> Optional[str]:
        """
        Lấy proxy SOCKS5 ở định dạng 'ip:port:username:password'
        
        Returns:
            String proxy SOCKS5 hoặc None nếu không có proxy
        """
        proxy_data = self.get_proxy()
        return proxy_data.get("proxysocks5") if proxy_data else None
    
    def get_requests_proxy_dict(self) -> Dict[str, str]:
        """
        Lấy dictionary proxy để sử dụng với thư viện requests
        
        Returns:
            Dictionary proxy có dạng {'http': 'http://...', 'https': 'http://...'}
        """
        formatted_proxy = self.get_formatted_http_proxy()
        if not formatted_proxy:
            return {}
            
        return {
            "http": formatted_proxy,
            "https": formatted_proxy
        }
    
    def test_connection(self) -> bool:
        """
        Kiểm tra kết nối với dịch vụ proxy
        
        Returns:
            True nếu kết nối thành công, False nếu có lỗi
        """
        try:
            proxy = self.get_proxy()
            return proxy is not None
        except Exception as e:
            logger.error(f"Lỗi khi kết nối với dịch vụ proxy: {str(e)}")
            return False