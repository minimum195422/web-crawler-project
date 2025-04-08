import json
import logging
import os
import re
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from common.image_utils import ImageDownloader

logger = logging.getLogger(__name__)

class LazadaParser:
    """Parser để xử lý dữ liệu crawl được từ Lazada"""
    
    def __init__(
        self, 
        input_dir: str = "/opt/airflow/data", 
        output_dir: str = "/opt/airflow/data",
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "images",
        process_images: bool = True
    ):
        """
        Khởi tạo parser
        
        Args:
            input_dir: Thư mục chứa dữ liệu JSON crawl được
            output_dir: Thư mục lưu dữ liệu đã xử lý
            s3_bucket: Tên bucket S3 để lưu ảnh
            s3_prefix: Tiền tố đường dẫn S3 cho ảnh
            process_images: Có xử lý và tải ảnh lên S3 không
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET_NAME')
        self.s3_prefix = s3_prefix
        self.process_images = process_images
        
        # Tạo thư mục output nếu chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
        
        # Khởi tạo image downloader nếu cần xử lý ảnh
        self.image_downloader = None
        if self.process_images and self.s3_bucket:
            self.image_downloader = ImageDownloader(
                s3_bucket=self.s3_bucket,
                s3_prefix=self.s3_prefix
            )
    
    def load_product_file(self, filename: str) -> Optional[Dict]:
        """
        Đọc file JSON chứa thông tin sản phẩm
        
        Args:
            filename: Đường dẫn đến file JSON
            
        Returns:
            Dữ liệu sản phẩm dạng dict hoặc None nếu có lỗi
        """
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Lỗi khi đọc file {filename}: {str(e)}")
            return None
    
    def load_all_products(self) -> List[Dict]:
        """
        Đọc tất cả các file JSON sản phẩm trong thư mục input
        
        Returns:
            Danh sách các sản phẩm
        """
        products = []
        
        # Liệt kê tất cả các file JSON trong thư mục input
        for filename in os.listdir(self.input_dir):
            if filename.startswith('lazada_product_') and filename.endswith('.json'):
                file_path = os.path.join(self.input_dir, filename)
                product_data = self.load_product_file(file_path)
                if product_data:
                    products.append(product_data)
        
        logger.info(f"Đã đọc {len(products)} file sản phẩm")
        return products
    
    def clean_price(self, price_str: Optional[str]) -> Optional[float]:
        """
        Chuyển chuỗi giá tiền thành số
        
        Args:
            price_str: Chuỗi giá (ví dụ: "₫1.234.567")
            
        Returns:
            Giá dạng số thực hoặc None nếu không hợp lệ
        """
        if not price_str:
            return None
            
        try:
            # Loại bỏ các ký tự không phải số và dấu chấm
            price_clean = re.sub(r'[^\d.]', '', price_str.replace(',', '.'))
            
            # Chuyển về dạng số
            return float(price_clean)
        except ValueError:
            return None
    
    def clean_html(self, html_content: str) -> str:
        """
        Làm sạch nội dung HTML, loại bỏ các thẻ
        
        Args:
            html_content: Nội dung HTML
            
        Returns:
            Văn bản đã làm sạch
        """
        if not html_content:
            return ""
            
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator=' ', strip=True)
    
    def clean_rating(self, rating_str: str) -> Optional[float]:
        """
        Chuyển chuỗi đánh giá thành số
        
        Args:
            rating_str: Chuỗi đánh giá (ví dụ: "4.7")
            
        Returns:
            Đánh giá dạng số thực hoặc None nếu không hợp lệ
        """
        if not rating_str or rating_str == "Chưa có đánh giá":
            return None
            
        try:
            # Trích xuất số từ chuỗi (ví dụ: "4.7 out of 5" -> 4.7)
            rating_match = re.search(r'(\d+(\.\d+)?)', rating_str)
            if rating_match:
                return float(rating_match.group(1))
            return None
        except ValueError:
            return None
    
    def parse_product(self, product: Dict) -> Dict:
        """
        Xử lý dữ liệu sản phẩm thô
        
        Args:
            product: Dữ liệu sản phẩm thô
            
        Returns:
            Dữ liệu sản phẩm đã xử lý
        """
        try:
            # Tạo bản sao của sản phẩm để không làm thay đổi dữ liệu gốc
            processed = product.copy()
            
            # Làm sạch và chuyển đổi dữ liệu
            processed['original_price_value'] = self.clean_price(product.get('original_price'))
            processed['sale_price_value'] = self.clean_price(product.get('sale_price'))
            processed['rating_value'] = self.clean_rating(product.get('rating'))
            
            # Làm sạch mô tả HTML
            if 'description' in product:
                processed['description_text'] = self.clean_html(product['description'])
            
            # Tính khuyến mãi phần trăm
            if processed['original_price_value'] and processed['sale_price_value']:
                discount = processed['original_price_value'] - processed['sale_price_value']
                discount_percent = (discount / processed['original_price_value']) * 100
                processed['discount_percent'] = round(discount_percent, 2)
            else:
                processed['discount_percent'] = None
            
            # Xử lý và tải ảnh lên S3 nếu cần
            if self.process_images and self.image_downloader and 'variations' in processed:
                processed = self.image_downloader.process_product_images(processed)
            
            return processed
        except Exception as e:
            logger.error(f"Lỗi khi xử lý sản phẩm: {str(e)}")
            return product
    
    def process_all_products(self) -> List[Dict]:
        """
        Xử lý tất cả sản phẩm và trả về danh sách đã xử lý
        
        Returns:
            Danh sách sản phẩm đã xử lý
        """
        products = self.load_all_products()
        processed_products = []
        
        total_products = len(products)
        logger.info(f"Bắt đầu xử lý {total_products} sản phẩm")
        
        for i, product in enumerate(products):
            logger.info(f"Đang xử lý sản phẩm {i+1}/{total_products}: {product.get('name', 'Không tên')}")
            processed = self.parse_product(product)
            processed_products.append(processed)
        
        logger.info(f"Đã xử lý {len(processed_products)} sản phẩm")
        return processed_products
    
    def save_to_json(self, products: List[Dict], filename: Optional[str] = None) -> str:
        """
        Lưu danh sách sản phẩm vào file JSON
        
        Args:
            products: Danh sách sản phẩm
            filename: Tên file đầu ra (mặc định tự động tạo)
            
        Returns:
            Đường dẫn đến file đã lưu
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.output_dir, f"lazada_processed_{timestamp}.json")
        else:
            filename = os.path.join(self.output_dir, filename)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Đã lưu {len(products)} sản phẩm vào {filename}")
            return filename
        except Exception as e:
            logger.error(f"Lỗi khi lưu file JSON: {str(e)}")
            return ""
    
    def save_to_csv(self, products: List[Dict], filename: Optional[str] = None) -> str:
        """
        Lưu danh sách sản phẩm vào file CSV
        
        Args:
            products: Danh sách sản phẩm
            filename: Tên file đầu ra (mặc định tự động tạo)
            
        Returns:
            Đường dẫn đến file đã lưu
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.output_dir, f"lazada_processed_{timestamp}.csv")
        else:
            filename = os.path.join(self.output_dir, filename)
        
        try:
            # Biến đổi dữ liệu sản phẩm để phù hợp với CSV
            # Loại bỏ các trường phức tạp như variations, description HTML
            csv_products = []
            for product in products:
                p = {
                    'source': product.get('source'),
                    'url': product.get('url'),
                    'name': product.get('name'),
                    'rating': product.get('rating'),
                    'rating_value': product.get('rating_value'),
                    'brand': product.get('brand'),
                    'category': product.get('category'),
                    'original_price': product.get('original_price'),
                    'original_price_value': product.get('original_price_value'),
                    'sale_price': product.get('sale_price'),
                    'sale_price_value': product.get('sale_price_value'),
                    'discount_percent': product.get('discount_percent'),
                    'crawl_time': product.get('crawl_time'),
                }
                
                # Thêm thông tin biến thể (variations) cơ bản
                if 'variations' in product and product['variations']:
                    p['variation_count'] = len(product['variations'])
                    p['variation_types'] = '; '.join(set([v.get('attribute', '') for v in product['variations']]))
                    
                    # Thêm thông tin ảnh công khai từ S3 nếu có
                    if 'public_image' in product['variations'][0]:
                        p['main_image'] = product['variations'][0]['public_image']
                
                csv_products.append(p)
            
            # Tạo DataFrame và lưu vào CSV
            df = pd.DataFrame(csv_products)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            logger.info(f"Đã lưu {len(products)} sản phẩm vào {filename}")
            return filename
        except Exception as e:
            logger.error(f"Lỗi khi lưu file CSV: {str(e)}")
            return ""
    
    def run(self, save_json: bool = True, save_csv: bool = True) -> Dict:
        """
        Xử lý tất cả sản phẩm và lưu kết quả
        
        Args:
            save_json: Có lưu kết quả dạng JSON không
            save_csv: Có lưu kết quả dạng CSV không
            
        Returns:
            Thông tin về kết quả xử lý
        """
        try:
            # Xử lý tất cả sản phẩm
            processed_products = self.process_all_products()
            
            result = {
                'product_count': len(processed_products),
                'timestamp': datetime.now().isoformat(),
            }
            
            # Lưu kết quả theo định dạng yêu cầu
            if save_json:
                json_file = self.save_to_json(processed_products)
                result['json_file'] = json_file
                
            if save_csv:
                csv_file = self.save_to_csv(processed_products)
                result['csv_file'] = csv_file
                
            return result
        except Exception as e:
            logger.error(f"Lỗi khi chạy parser: {str(e)}")
            return {'error': str(e)}


if __name__ == "__main__":
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Khởi tạo parser
    parser = LazadaParser(
        process_images=True,
        s3_bucket=os.getenv('S3_BUCKET_NAME')
    )
    
    # Chạy parser
    result = parser.run()
    
    print(f"Kết quả: {result}")