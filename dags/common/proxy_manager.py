import requests
import time
import random
import logging
import threading
from typing import Dict, Optional, Union, List
from datetime import datetime

class ProxyManager:
    def __init__(self, api_key: str, networks: Union[str, List[str]] = None, base_url: str = "https://proxyxoay.shop/api/get.php"):
        """
        Khởi tạo ProxyManager để quản lý proxy xoay
        
        Args:
            api_key: API key nhận được khi mua hàng
            networks: Danh sách nhà mạng hoặc một nhà mạng cụ thể (vd: 'fpt', 'viettel', ...)
            base_url: URL API của dịch vụ proxy xoay
        """
        self.api_key = api_key
        self.base_url = base_url
        self.networks = networks if isinstance(networks, list) else [networks] if networks else None
        
        self.current_proxy = None
        self.expiration_time = 0
        self.lock = threading.Lock()
        self.proxy_refresh_thread = None
        self.stop_refresh = False
        self.logger = logging.getLogger("proxy_manager")
        
    def _get_new_proxy(self) -> Optional[Dict]:
        """
        Gọi API để lấy proxy mới
        
        Returns:
            Thông tin proxy hoặc None nếu không thành công
        """
        try:
            params = {"key": self.api_key}
            
            # Thêm nhà mạng nếu được chỉ định
            if self.networks:
                network = random.choice(self.networks)
                params["nhamang"] = network
            
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if data.get("status") == 100:
                # Tính thời gian hết hạn dựa vào thời gian die của proxy
                die_seconds = int(data.get("message", "").split("die sau ")[1].split("s")[0])
                self.expiration_time = time.time() + die_seconds - 10  # Trừ 10 giây để đảm bảo an toàn
                
                self.logger.info(f"Đã nhận proxy mới: {data.get('proxyhttp')} - Hết hạn sau: {die_seconds}s")
                return data
            else:
                self.logger.error(f"Lỗi khi lấy proxy: {data}")
                return None
        except Exception as e:
            self.logger.exception(f"Lỗi khi gọi API proxy: {str(e)}")
            return None
        
    def get_proxy(self) -> Optional[Dict]:
        """
        Lấy proxy hiện tại hoặc lấy proxy mới nếu cần
        
        Returns:
            Thông tin proxy hoặc None nếu không thể lấy proxy
        """
        with self.lock:
            current_time = time.time()
            
            # Nếu chưa có proxy hoặc proxy đã hết hạn, lấy proxy mới
            if not self.current_proxy or current_time >= self.expiration_time:
                self.current_proxy = self._get_new_proxy()
                
            return self.current_proxy
    
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
    
    def get_formatted_socks5_proxy(self) -> Optional[str]:
        """
        Lấy proxy SOCKS5 ở định dạng 'socks5://username:password@ip:port'
        
        Returns:
            String proxy SOCKS5 được định dạng hoặc None nếu không có proxy
        """
        proxy_str = self.get_socks5_proxy()
        if not proxy_str:
            return None
        
        parts = proxy_str.split(":")
        if len(parts) != 4:
            return None
            
        ip, port, username, password = parts
        return f"socks5://{username}:{password}@{ip}:{port}"
    
    def get_proxy_dict_for_requests(self) -> Dict[str, str]:
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
    
    def _auto_refresh_proxy(self, min_interval: int = 60, max_interval: int = 120):
        """
        Thread tự động làm mới proxy theo khoảng thời gian
        
        Args:
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        self.logger.info("Bắt đầu auto refresh proxy")
        
        while not self.stop_refresh:
            # Đợi một khoảng thời gian ngẫu nhiên
            wait_time = random.randint(min_interval, max_interval)
            self.logger.debug(f"Sẽ đổi proxy sau {wait_time} giây")
            
            # Ngủ theo từng đoạn ngắn để có thể dừng nhanh hơn khi cần
            for _ in range(wait_time):
                if self.stop_refresh:
                    break
                time.sleep(1)
                
            if self.stop_refresh:
                break
                
            # Lấy proxy mới
            with self.lock:
                self.logger.info("Đang đổi proxy theo lịch trình...")
                old_proxy = self.current_proxy.get("proxyhttp") if self.current_proxy else None
                self.current_proxy = self._get_new_proxy()
                new_proxy = self.current_proxy.get("proxyhttp") if self.current_proxy else None
                self.logger.info(f"Đã đổi proxy: {old_proxy} -> {new_proxy}")
    
    def start_auto_refresh(self, min_interval: int = 60, max_interval: int = 120):
        """
        Bắt đầu tự động làm mới proxy sau mỗi khoảng thời gian ngẫu nhiên
        
        Args:
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        if self.proxy_refresh_thread and self.proxy_refresh_thread.is_alive():
            self.logger.warning("Auto refresh đã được bật")
            return
            
        self.stop_refresh = False
        self.proxy_refresh_thread = threading.Thread(
            target=self._auto_refresh_proxy,
            args=(min_interval, max_interval),
            daemon=True
        )
        self.proxy_refresh_thread.start()
    
    def stop_auto_refresh(self):
        """Dừng tự động làm mới proxy"""
        self.stop_refresh = True
        if self.proxy_refresh_thread:
            self.proxy_refresh_thread.join(timeout=5)
            self.logger.info("Đã dừng auto refresh proxy")

# Ví dụ sử dụng
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Khởi tạo proxy manager
    proxy_manager = ProxyManager(
        api_key="your_api_key_here",
        networks=["fpt", "viettel"]  # Có thể chọn một hoặc nhiều nhà mạng
    )
    
    # Bắt đầu tự động đổi proxy mỗi 60-120 giây
    proxy_manager.start_auto_refresh(min_interval=60, max_interval=120)
    
    try:
        # Ví dụ sử dụng proxy trong requests
        for i in range(10):
            proxies = proxy_manager.get_proxy_dict_for_requests()
            print(f"Lần {i+1} - Đang sử dụng proxy: {proxies}")
            
            try:
                # Thử kết nối với proxy
                response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=10)
                print(f"IP hiện tại: {response.json().get('ip')}")
            except Exception as e:
                print(f"Lỗi kết nối: {str(e)}")
                
            # Đợi 30 giây giữa các request
            time.sleep(30)
    finally:
        # Dừng auto refresh khi kết thúc
        proxy_manager.stop_auto_refresh()