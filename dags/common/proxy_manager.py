import requests
import time
import random
import logging
import threading
from typing import Dict, Optional, Union, List, Tuple
from datetime import datetime

class ProxyManager:
    def __init__(self, 
                api_key: Union[str, List[str]], 
                base_url: str = "https://proxyxoay.shop/api/get.php",
                tab_distribution: Optional[List[int]] = None):
        """
        Khởi tạo ProxyManager để quản lý proxy xoay
        
        Args:
            api_key: API key đơn hoặc danh sách API key nhận được khi mua hàng
            base_url: URL API của dịch vụ proxy xoay
            tab_distribution: Phân bổ số tab cho mỗi proxy, mặc định là phân đều
        """
        self.base_url = base_url
        
        # Chuyển đổi api_key thành list nếu là chuỗi đơn
        self.api_keys = api_key if isinstance(api_key, list) else [api_key]
        
        # Tạo proxy managers cho từng api key
        self.proxy_instances = []
        for key in self.api_keys:
            self.proxy_instances.append({
                "api_key": key,
                "proxy_data": None,
                "expiration_time": 0,
                "lock": threading.Lock(),
                "stop_refresh": False,
                "refresh_thread": None
            })
        
        # Phân bổ tab cho mỗi proxy
        if tab_distribution:
            if len(tab_distribution) != len(self.api_keys):
                raise ValueError("Số lượng tab phân bổ phải bằng số lượng proxy")
            self.tab_distribution = tab_distribution
        else:
            # Mặc định là 3 tab cho mỗi proxy
            self.tab_distribution = [3] * len(self.api_keys)
        
        self.logger = logging.getLogger("proxy_manager")
        self.logger.info(f"Khởi tạo {len(self.proxy_instances)} proxy với phân bổ tab: {self.tab_distribution}")
        
    def _get_new_proxy(self, proxy_idx: int) -> Optional[Dict]:
        """
        Gọi API để lấy proxy mới cho một proxy instance cụ thể
        
        Args:
            proxy_idx: Chỉ mục của proxy instance
            
        Returns:
            Thông tin proxy hoặc None nếu không thành công
        """
        proxy_instance = self.proxy_instances[proxy_idx]
        try:
            params = {"key": proxy_instance["api_key"]}
            
            # Sử dụng "Random" cho nhà mạng
            params["nhamang"] = "Random"
            
            # Thêm tỉnh thành "0" để lấy ngẫu nhiên
            params["tinhthanh"] = "0"
            
            response = requests.get(self.base_url, params=params)
            data = response.json()
            
            if data.get("status") == 100:
                # Tính thời gian hết hạn dựa vào thời gian die của proxy
                die_seconds = int(data.get("message", "").split("die sau ")[1].split("s")[0])
                proxy_instance["expiration_time"] = time.time() + die_seconds - 10  # Trừ 10 giây để đảm bảo an toàn
                
                self.logger.info(f"Proxy {proxy_idx} - Đã nhận proxy mới: {data.get('proxyhttp')} - Hết hạn sau: {die_seconds}s")
                return data
            else:
                self.logger.error(f"Proxy {proxy_idx} - Lỗi khi lấy proxy: {data}")
                return None
        except Exception as e:
            self.logger.exception(f"Proxy {proxy_idx} - Lỗi khi gọi API proxy: {str(e)}")
            return None
        
    def get_proxy(self, proxy_idx: int) -> Optional[Dict]:
        """
        Lấy proxy hiện tại hoặc lấy proxy mới nếu cần cho một proxy instance cụ thể
        
        Args:
            proxy_idx: Chỉ mục của proxy instance
            
        Returns:
            Thông tin proxy hoặc None nếu không thể lấy proxy
        """
        proxy_instance = self.proxy_instances[proxy_idx]
        
        with proxy_instance["lock"]:
            current_time = time.time()
            
            # Nếu chưa có proxy hoặc proxy đã hết hạn, lấy proxy mới
            if not proxy_instance["proxy_data"] or current_time >= proxy_instance["expiration_time"]:
                proxy_instance["proxy_data"] = self._get_new_proxy(proxy_idx)
                
            return proxy_instance["proxy_data"]
    
    def get_proxy_for_tab(self, tab_idx: int) -> Tuple[int, Optional[Dict]]:
        """
        Xác định proxy instance nào sẽ xử lý tab với chỉ mục nhất định
        
        Args:
            tab_idx: Chỉ mục của tab (0-based)
            
        Returns:
            Tuple (proxy_idx, proxy_data)
        """
        proxy_idx = 0
        tab_count = 0
        
        # Tìm proxy instance phù hợp dựa trên phân bổ tab
        for i, tab_limit in enumerate(self.tab_distribution):
            if tab_idx < tab_count + tab_limit:
                proxy_idx = i
                break
            tab_count += tab_limit
        
        # Lấy proxy từ instance đã chọn
        proxy_data = self.get_proxy(proxy_idx)
        
        self.logger.debug(f"Tab {tab_idx} được xử lý bởi proxy {proxy_idx}")
        return proxy_idx, proxy_data
    
    def get_http_proxy(self, tab_idx: int = 0) -> Optional[str]:
        """
        Lấy proxy HTTP cho tab với chỉ mục nhất định
        
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
    
    def get_socks5_proxy(self, tab_idx: int = 0) -> Optional[str]:
        """
        Lấy proxy SOCKS5 cho tab
        
        Args:
            tab_idx: Chỉ mục của tab
            
        Returns:
            String proxy SOCKS5 hoặc None nếu không có proxy
        """
        _, proxy_data = self.get_proxy_for_tab(tab_idx)
        return proxy_data.get("proxysocks5") if proxy_data else None
    
    def get_formatted_socks5_proxy(self, tab_idx: int = 0) -> Optional[str]:
        """
        Lấy proxy SOCKS5 ở định dạng 'socks5://username:password@ip:port' cho tab
        
        Args:
            tab_idx: Chỉ mục của tab
            
        Returns:
            String proxy SOCKS5 được định dạng hoặc None nếu không có proxy
        """
        proxy_str = self.get_socks5_proxy(tab_idx)
        if not proxy_str:
            return None
        
        parts = proxy_str.split(":")
        if len(parts) != 4:
            return None
            
        ip, port, username, password = parts
        return f"socks5://{username}:{password}@{ip}:{port}"
    
    def get_proxy_dict_for_requests(self, tab_idx: int = 0) -> Dict[str, str]:
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
    
    def _auto_refresh_proxy(self, proxy_idx: int, min_interval: int = 60, max_interval: int = 120):
        """
        Thread tự động làm mới proxy cho một proxy instance cụ thể
        
        Args:
            proxy_idx: Chỉ mục của proxy instance
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        proxy_instance = self.proxy_instances[proxy_idx]
        self.logger.info(f"Proxy {proxy_idx} - Bắt đầu auto refresh")
        
        while not proxy_instance["stop_refresh"]:
            # Đợi một khoảng thời gian ngẫu nhiên
            wait_time = random.randint(min_interval, max_interval)
            self.logger.debug(f"Proxy {proxy_idx} - Sẽ đổi proxy sau {wait_time} giây")
            
            # Ngủ theo từng đoạn ngắn để có thể dừng nhanh hơn khi cần
            for _ in range(wait_time):
                if proxy_instance["stop_refresh"]:
                    break
                time.sleep(1)
                
            if proxy_instance["stop_refresh"]:
                break
                
            # Lấy proxy mới
            with proxy_instance["lock"]:
                self.logger.info(f"Proxy {proxy_idx} - Đang đổi proxy theo lịch trình...")
                old_proxy = proxy_instance["proxy_data"].get("proxyhttp") if proxy_instance["proxy_data"] else None
                proxy_instance["proxy_data"] = self._get_new_proxy(proxy_idx)
                new_proxy = proxy_instance["proxy_data"].get("proxyhttp") if proxy_instance["proxy_data"] else None
                self.logger.info(f"Proxy {proxy_idx} - Đã đổi proxy: {old_proxy} -> {new_proxy}")
    
    def start_auto_refresh(self, min_interval: int = 60, max_interval: int = 120):
        """
        Bắt đầu tự động làm mới proxy sau mỗi khoảng thời gian cho tất cả proxy instance
        
        Args:
            min_interval: Thời gian tối thiểu giữa các lần làm mới (giây)
            max_interval: Thời gian tối đa giữa các lần làm mới (giây)
        """
        for idx, proxy_instance in enumerate(self.proxy_instances):
            if proxy_instance["refresh_thread"] and proxy_instance["refresh_thread"].is_alive():
                self.logger.warning(f"Proxy {idx} - Auto refresh đã được bật")
                continue
                
            proxy_instance["stop_refresh"] = False
            proxy_instance["refresh_thread"] = threading.Thread(
                target=self._auto_refresh_proxy,
                args=(idx, min_interval, max_interval),
                daemon=True
            )
            proxy_instance["refresh_thread"].start()
            self.logger.info(f"Proxy {idx} - Đã bắt đầu auto refresh")
    
    def stop_auto_refresh(self):
        """Dừng tự động làm mới proxy cho tất cả proxy instance"""
        for idx, proxy_instance in enumerate(self.proxy_instances):
            proxy_instance["stop_refresh"] = True
            if proxy_instance["refresh_thread"]:
                proxy_instance["refresh_thread"].join(timeout=5)
                self.logger.info(f"Proxy {idx} - Đã dừng auto refresh")

# Ví dụ sử dụng
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Khởi tạo proxy manager với 2 API key
    proxy_manager = ProxyManager(
        api_key=["your_api_key_1", "your_api_key_2"],
        tab_distribution=[3, 3]  # 3 tab cho mỗi proxy
    )
    
    # Bắt đầu tự động đổi proxy mỗi 60-120 giây
    proxy_manager.start_auto_refresh(min_interval=60, max_interval=120)
    
    try:
        # Ví dụ sử dụng proxy trong requests
        for i in range(6):  # Thử với 6 tab
            proxies = proxy_manager.get_proxy_dict_for_requests(i)
            print(f"Tab {i} - Đang sử dụng proxy: {proxies}")
            
            try:
                # Thử kết nối với proxy
                response = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=10)
                print(f"Tab {i} - IP hiện tại: {response.json().get('ip')}")
            except Exception as e:
                print(f"Tab {i} - Lỗi kết nối: {str(e)}")
                
            # Đợi 2 giây giữa các request
            time.sleep(2)
    finally:
        # Dừng auto refresh khi kết thúc
        proxy_manager.stop_auto_refresh()