"""
Tiện ích tải và xử lý hình ảnh
"""

import os
import logging
import requests
import hashlib
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote
import boto3
from io import BytesIO
from PIL import Image
import random
from datetime import datetime

logger = logging.getLogger(__name__)

class ImageDownloader:
    """
    Class hỗ trợ tải hình ảnh từ URL và lưu trữ trên S3
    """
    
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str = "images",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: str = "ap-southeast-1",
        local_temp_dir: str = "/tmp/images",
        max_retries: int = 3,
        timeout: int = 10,
        user_agent: Optional[str] = None
    ):
        """
        Khởi tạo downloader
        
        Args:
            s3_bucket: Tên bucket S3
            s3_prefix: Tiền tố đường dẫn S3
            aws_access_key_id: AWS Access Key ID
            aws_secret_access_key: AWS Secret Access Key
            aws_region: Khu vực AWS
            local_temp_dir: Thư mục tạm local để lưu ảnh
            max_retries: Số lần thử lại tối đa khi tải ảnh
            timeout: Thời gian chờ tối đa cho mỗi request (giây)
            user_agent: User-Agent để sử dụng khi tải ảnh
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = aws_region
        self.local_temp_dir = local_temp_dir
        self.max_retries = max_retries
        self.timeout = timeout
        
        # Tạo thư mục tạm nếu chưa tồn tại
        os.makedirs(local_temp_dir, exist_ok=True)
        
        # Khởi tạo session để tái sử dụng kết nối
        self.session = requests.Session()
        
        # User-Agent ngẫu nhiên nếu không được cung cấp
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        ]
        self.user_agent = user_agent or random.choice(self.user_agents)
        
        # Khởi tạo S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )
    
    def _get_image_extension(self, url: str) -> str:
        """
        Trích xuất phần mở rộng từ URL ảnh
        
        Args:
            url: URL ảnh
            
        Returns:
            Phần mở rộng file (ví dụ: '.jpg', '.png')
        """
        # Phân tích URL
        parsed_url = urlparse(url)
        path = unquote(parsed_url.path)
        
        # Lấy phần mở rộng
        _, ext = os.path.splitext(path)
        
        # Nếu không có phần mở rộng hoặc không hợp lệ, mặc định là jpg
        if not ext or ext.lower() not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            return '.jpg'
        
        # Chuẩn hóa .jpeg thành .jpg
        if ext.lower() == '.jpeg':
            return '.jpg'
            
        return ext.lower()
    
    def _generate_s3_key(self, url: str, product_id: str, variation: str) -> str:
        # Tạo key S3 duy nhất cho hình ảnh
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        ext = self._get_image_extension(url)
        timestamp = datetime.now().strftime("%Y%m%d")
        variation_clean = variation.replace(' ', '_').replace('/', '_').lower()
        
        # Format mới: images/source/product_id/date/variation_hash.ext
        s3_key = f"{self.s3_prefix}/{source}/{product_id}/{timestamp}/{variation_clean}_{url_hash}{ext}"
        return s3_key
    
    def download_image(self, url: str, retries: int = None) -> Optional[bytes]:
        """
        Tải hình ảnh từ URL
        
        Args:
            url: URL hình ảnh
            retries: Số lần thử lại (mặc định lấy từ cấu hình)
            
        Returns:
            Dữ liệu hình ảnh dạng bytes hoặc None nếu có lỗi
        """
        if retries is None:
            retries = self.max_retries
            
        # Headers giả lập trình duyệt
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Referer': 'https://www.lazada.vn/',
        }
        
        for attempt in range(retries):
            try:
                # Tải hình ảnh
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                
                # Kiểm tra content-type
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"URL không trả về hình ảnh: {url}, Content-Type: {content_type}")
                    return None
                
                # Trả về dữ liệu hình ảnh
                return response.content
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Lỗi khi tải hình ảnh (lần {attempt+1}/{retries}): {url}, Lỗi: {str(e)}")
                
                # Đợi trước khi thử lại
                if attempt < retries - 1:
                    time.sleep(1 * (attempt + 1))
        
        return None
    
    def upload_to_s3(self, image_data: bytes, s3_key: str) -> Optional[str]:
        """
        Upload hình ảnh lên S3
        
        Args:
            image_data: Dữ liệu hình ảnh dạng bytes
            s3_key: Key trên S3
            
        Returns:
            URL S3 của hình ảnh hoặc None nếu có lỗi
        """
        try:
            # Xác định content type
            ext = os.path.splitext(s3_key)[1].lower()
            content_type = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.webp': 'image/webp'
            }.get(ext, 'application/octet-stream')
            
            # Upload lên S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=image_data,
                ContentType=content_type
            )
            
            # Tạo URL S3
            s3_url = f"s3://{self.s3_bucket}/{s3_key}"
            logger.info(f"Đã upload hình ảnh lên: {s3_url}")
            
            return s3_url
            
        except Exception as e:
            logger.error(f"Lỗi khi upload hình ảnh lên S3: {str(e)}")
            return None
    
    def process_image(self, url: str, product_id: str, variation: str) -> Optional[str]:
        """
        Tải và upload hình ảnh, trả về URL S3
        
        Args:
            url: URL hình ảnh gốc
            product_id: ID sản phẩm
            variation: Thông tin biến thể
            
        Returns:
            URL S3 của hình ảnh hoặc None nếu có lỗi
        """
        # Tạo key S3
        s3_key = self._generate_s3_key(url, product_id, variation)
        
        try:
            # Kiểm tra nếu hình ảnh đã tồn tại trên S3
            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                logger.info(f"Hình ảnh đã tồn tại trên S3: s3://{self.s3_bucket}/{s3_key}")
                return f"s3://{self.s3_bucket}/{s3_key}"
            except:
                # Hình ảnh chưa tồn tại, tiếp tục tải
                pass
            
            # Tải hình ảnh
            image_data = self.download_image(url)
            if not image_data:
                return None
            
            # Upload lên S3
            s3_url = self.upload_to_s3(image_data, s3_key)
            return s3_url
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý hình ảnh {url} cho sản phẩm {product_id}: {str(e)}")
            return None
    
    def generate_public_url(self, s3_url: str) -> str:
        """
        Chuyển đổi URL S3 thành URL công khai cho trình duyệt
        
        Args:
            s3_url: URL S3 dạng s3://bucket/key
            
        Returns:
            URL công khai
        """
        if not s3_url.startswith('s3://'):
            return s3_url
            
        # Trích xuất bucket và key
        s3_parts = s3_url.replace('s3://', '').split('/', 1)
        if len(s3_parts) != 2:
            return s3_url
            
        bucket, key = s3_parts
        
        # Tạo URL công khai
        return f"https://{bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
    
    def process_product_images(self, product_data: Dict) -> Dict:
        """
        Xử lý tất cả hình ảnh trong dữ liệu sản phẩm
        
        Args:
            product_data: Dữ liệu sản phẩm
            
        Returns:
            Dữ liệu sản phẩm đã cập nhật URL ảnh
        """
        # Tạo bản sao của dữ liệu
        updated_product = product_data.copy()
        
        # Lấy nguồn dữ liệu và ID sản phẩm
        source = product_data.get('source', 'unknown')
        product_url = product_data.get('url', '')
        product_id = f"{source}_{self._extract_product_id(product_url)}"
        
        # Cố gắng trích xuất ID sản phẩm từ URL
        if 'lazada.vn/products/' in product_url:
            try:
                product_id = product_url.split('/')[-1].split('.')[0]
            except:
                product_id = hashlib.md5(product_url.encode()).hexdigest()
        else:
            product_id = hashlib.md5(product_url.encode()).hexdigest()
        
        # Xử lý các biến thể
        if 'variations' in product_data and product_data['variations']:
            updated_variations = []
            
            for variation in product_data['variations']:
                # Tạo bản sao của biến thể
                updated_variation = variation.copy()
                
                # Lấy thông tin biến thể để tạo tên file
                attribute = variation.get('attribute', 'default')
                value = variation.get('value', 'default')
                variation_text = f"{attribute}_{value}"
                
                # Lấy URL ảnh gốc
                image_url = variation.get('image')
                
                if image_url:
                    # Xử lý ảnh
                    s3_url = self.process_image(image_url, product_id, variation_text)
                    
                    # Cập nhật URL trong biến thể
                    if s3_url:
                        updated_variation['original_image'] = image_url
                        updated_variation['image'] = s3_url
                        updated_variation['public_image'] = self.generate_public_url(s3_url)
                    
                updated_variations.append(updated_variation)
            
            # Cập nhật biến thể trong sản phẩm
            updated_product['variations'] = updated_variations
        
        return updated_product


if __name__ == "__main__":
    # Ví dụ sử dụng
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Khởi tạo downloader
    downloader = ImageDownloader(
        s3_bucket="your-bucket-name",
        s3_prefix="images"
    )
    
    # URL ảnh demo
    image_url = "https://lzd-img-global.slatic.net/g/p/18c8d5cd471345e8a2c48b8c3886eb02.jpg_720x720q80.jpg"
    
    # Xử lý ảnh
    s3_url = downloader.process_image(
        url=image_url, 
        product_id="iphone15", 
        variation="color_black"
    )
    
    print(f"URL S3: {s3_url}")
    print(f"URL công khai: {downloader.generate_public_url(s3_url)}")