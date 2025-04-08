"""
Các tiện ích chung cho dự án crawler
"""

import os
import logging
import random
import string
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

def setup_requests_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: List[int] = [500, 502, 503, 504],
    session: Optional[requests.Session] = None
) -> requests.Session:
    """
    Thiết lập session requests với retry tự động
    
    Args:
        retries: Số lần thử lại
        backoff_factor: Hệ số chờ giữa các lần thử
        status_forcelist: Danh sách mã trạng thái cần thử lại
        session: Session có sẵn (tùy chọn)
        
    Returns:
        Session đã cấu hình
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_random_user_agent() -> str:
    """
    Tạo một User-Agent ngẫu nhiên
    
    Returns:
        Chuỗi User-Agent
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
    ]
    return random.choice(user_agents)

def random_delay(min_seconds: float = 1.0, max_seconds: float = 5.0) -> None:
    """
    Tạm dừng trong một khoảng thời gian ngẫu nhiên
    
    Args:
        min_seconds: Thời gian tối thiểu (giây)
        max_seconds: Thời gian tối đa (giây)
    """
    import time
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def generate_random_string(length: int = 10) -> str:
    """
    Tạo một chuỗi ngẫu nhiên
    
    Args:
        length: Độ dài chuỗi
        
    Returns:
        Chuỗi ngẫu nhiên
    """
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def clean_filename(filename: str) -> str:
    """
    Làm sạch tên file, loại bỏ các ký tự không hợp lệ
    
    Args:
        filename: Tên file gốc
        
    Returns:
        Tên file đã làm sạch
    """
    # Loại bỏ các ký tự không được phép trong tên file
    filename = re.sub(r'[\\/*?:"<>|]', '', filename)
    # Thay thế khoảng trắng bằng dấu gạch dưới
    filename = re.sub(r'\s+', '_', filename)
    return filename

def save_json(data: Union[Dict, List], filename: str, ensure_dir: bool = True) -> str:
    """
    Lưu dữ liệu vào file JSON
    
    Args:
        data: Dữ liệu cần lưu
        filename: Tên file
        ensure_dir: Tạo thư mục nếu chưa tồn tại
        
    Returns:
        Đường dẫn đến file đã lưu
    """
    if ensure_dir:
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return filename

def load_json(filename: str) -> Optional[Union[Dict, List]]:
    """
    Đọc dữ liệu từ file JSON
    
    Args:
        filename: Tên file
        
    Returns:
        Dữ liệu từ file hoặc None nếu có lỗi
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Lỗi khi đọc file JSON {filename}: {str(e)}")
        return None

def make_dirs_if_not_exists(path: str) -> bool:
    """
    Tạo thư mục nếu chưa tồn tại
    
    Args:
        path: Đường dẫn thư mục
        
    Returns:
        True nếu thành công, False nếu có lỗi
    """
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Lỗi khi tạo thư mục {path}: {str(e)}")
        return False

def get_timestamp_str(format_str: str = "%Y%m%d_%H%M%S") -> str:
    """
    Lấy chuỗi timestamp hiện tại
    
    Args:
        format_str: Định dạng thời gian
        
    Returns:
        Chuỗi timestamp
    """
    return datetime.now().strftime(format_str)

def extract_digits(text: str) -> Optional[str]:
    """
    Trích xuất các chữ số từ một chuỗi
    
    Args:
        text: Chuỗi cần trích xuất
        
    Returns:
        Chuỗi chỉ chứa các chữ số hoặc None nếu không có chữ số
    """
    if not text:
        return None
        
    digits = re.findall(r'\d+', text)
    if digits:
        return ''.join(digits)
    return None

def extract_float(text: str) -> Optional[float]:
    """
    Trích xuất số thực từ một chuỗi
    
    Args:
        text: Chuỗi cần trích xuất
        
    Returns:
        Số thực hoặc None nếu không thể trích xuất
    """
    if not text:
        return None
    
    # Xử lý trường hợp dấu phẩy là dấu phân cách thập phân
    text = text.replace(',', '.')
    
    # Tìm tất cả các số thực có thể có trong chuỗi
    matches = re.findall(r'-?\d+\.?\d*', text)
    
    if matches:
        try:
            return float(matches[0])
        except ValueError:
            return None
    
    return None

def create_output_dir(base_dir: str, subdir: Optional[str] = None, use_timestamp: bool = False) -> str:
    """
    Tạo và trả về thư mục đầu ra
    
    Args:
        base_dir: Thư mục cơ sở
        subdir: Thư mục con (tùy chọn)
        use_timestamp: Có sử dụng timestamp trong tên thư mục không
        
    Returns:
        Đường dẫn đến thư mục đầu ra
    """
    # Tạo thư mục cơ sở nếu chưa tồn tại
    os.makedirs(base_dir, exist_ok=True)
    
    # Xác định đường dẫn đầu ra
    output_dir = base_dir
    
    # Thêm thư mục con nếu được chỉ định
    if subdir:
        output_dir = os.path.join(output_dir, subdir)
    
    # Thêm timestamp nếu được yêu cầu
    if use_timestamp:
        timestamp = get_timestamp_str()
        output_dir = os.path.join(output_dir, timestamp)
    
    # Tạo thư mục đầu ra nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    return output_dir