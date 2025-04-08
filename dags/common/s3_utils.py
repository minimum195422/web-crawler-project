"""
Tiện ích làm việc với Amazon S3
"""

import os
import logging
import boto3
from typing import List, Dict, Optional, Union, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class S3Uploader:
    """
    Lớp hỗ trợ tải dữ liệu lên S3
    """
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = 'ap-southeast-1'
    ):
        """
        Khởi tạo uploader S3
        
        Args:
            bucket_name: Tên bucket S3
            aws_access_key_id: AWS Access Key ID (nếu không cung cấp, sẽ lấy từ biến môi trường)
            aws_secret_access_key: AWS Secret Access Key (nếu không cung cấp, sẽ lấy từ biến môi trường)
            region_name: Tên khu vực AWS
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Sử dụng credentials từ tham số hoặc biến môi trường
        self.aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # Khởi tạo session
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        
        # Khởi tạo client S3
        self.s3_client = self.session.client('s3')
    
    def upload_file(
        self,
        file_path: str,
        s3_key: Optional[str] = None,
        extra_args: Optional[Dict] = None
    ) -> Tuple[bool, str]:
        """
        Tải một file lên S3
        
        Args:
            file_path: Đường dẫn local đến file cần tải lên
            s3_key: Đường dẫn trên S3 (nếu không cung cấp, sẽ sử dụng tên file)
            extra_args: Các tham số bổ sung cho S3
            
        Returns:
            Tuple (thành công, URL hoặc thông báo lỗi)
        """
        try:
            # Nếu không có s3_key, sử dụng tên file
            if not s3_key:
                s3_key = os.path.basename(file_path)
            
            # Mặc định ContentType
            if not extra_args:
                extra_args = {}
                
            # Tự động xác định ContentType dựa vào phần mở rộng
            if 'ContentType' not in extra_args:
                _, ext = os.path.splitext(file_path)
                content_type = {
                    '.json': 'application/json',
                    '.csv': 'text/csv',
                    '.txt': 'text/plain',
                    '.jpg': 'image/jpeg',
                    '.jpeg': 'image/jpeg',
                    '.png': 'image/png',
                    '.html': 'text/html',
                }.get(ext.lower(), 'application/octet-stream')
                
                extra_args['ContentType'] = content_type
            
            # Upload file
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=self.bucket_name,
                Key=s3_key,
                ExtraArgs=extra_args
            )
            
            # Tạo URL
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Đã tải lên thành công: {s3_url}")
            
            return True, s3_url
            
        except Exception as e:
            error_msg = f"Lỗi khi tải file {file_path} lên S3: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def upload_directory(
        self,
        local_dir: str,
        s3_prefix: str,
        file_pattern: str = "*",
        recursive: bool = True
    ) -> List[Tuple[bool, str]]:
        """
        Tải tất cả các file trong một thư mục lên S3
        
        Args:
            local_dir: Đường dẫn local đến thư mục
            s3_prefix: Tiền tố S3 cho các file
            file_pattern: Mẫu tên file (ví dụ: "*.json")
            recursive: Có tìm kiếm đệ quy không
            
        Returns:
            Danh sách kết quả upload (thành công, URL hoặc thông báo lỗi)
        """
        import glob
        
        results = []
        
        # Tạo mẫu tìm kiếm
        if recursive:
            search_pattern = os.path.join(local_dir, "**", file_pattern)
        else:
            search_pattern = os.path.join(local_dir, file_pattern)
        
        # Tìm tất cả các file phù hợp
        for file_path in glob.glob(search_pattern, recursive=recursive):
            if os.path.isfile(file_path):
                # Tạo s3_key tương đối
                rel_path = os.path.relpath(file_path, local_dir)
                s3_key = os.path.join(s3_prefix, rel_path).replace("\\", "/")  # Đảm bảo đường dẫn kiểu UNIX
                
                # Upload file
                result = self.upload_file(file_path, s3_key)
                results.append(result)
        
        return results
    
    def upload_json_data(
        self,
        data: Union[Dict, List],
        s3_key: str,
        extra_args: Optional[Dict] = None
    ) -> Tuple[bool, str]:
        """
        Tải dữ liệu JSON trực tiếp lên S3 mà không cần file tạm
        
        Args:
            data: Dữ liệu Python (Dict hoặc List)
            s3_key: Đường dẫn trên S3
            extra_args: Các tham số bổ sung cho S3
            
        Returns:
            Tuple (thành công, URL hoặc thông báo lỗi)
        """
        try:
            # Chuyển đổi dữ liệu thành JSON
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            
            # Mặc định ContentType
            if not extra_args:
                extra_args = {'ContentType': 'application/json'}
            elif 'ContentType' not in extra_args:
                extra_args['ContentType'] = 'application/json'
            
            # Upload dữ liệu
            self.s3_client.put_object(
                Body=json_data.encode('utf-8'),
                Bucket=self.bucket_name,
                Key=s3_key,
                **extra_args
            )
            
            # Tạo URL
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Đã tải lên thành công: {s3_url}")
            
            return True, s3_url
            
        except Exception as e:
            error_msg = f"Lỗi khi tải dữ liệu JSON lên S3: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def check_file_exists(self, s3_key: str) -> bool:
        """
        Kiểm tra xem một file có tồn tại trên S3 không
        
        Args:
            s3_key: Đường dẫn trên S3
            
        Returns:
            True nếu file tồn tại, False nếu không
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Tạo một URL tạm thời để truy cập file trên S3
        
        Args:
            s3_key: Đường dẫn trên S3
            expiration: Thời gian hết hạn (giây)
            
        Returns:
            URL tạm thời hoặc None nếu có lỗi
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Lỗi khi tạo presigned URL: {str(e)}")
            return None


if __name__ == "__main__":
    # Ví dụ sử dụng
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    bucket_name = os.getenv('S3_BUCKET_NAME')
    if not bucket_name:
        logger.error("Vui lòng thiết lập biến môi trường S3_BUCKET_NAME")
        exit(1)
    
    # Khởi tạo uploader
    uploader = S3Uploader(bucket_name)
    
    # Tải một file lên S3
    file_path = "/path/to/your/file.json"
    if os.path.exists(file_path):
        success, result = uploader.upload_file(
            file_path=file_path,
            s3_key="test/file.json"
        )
        print(f"Kết quả: {success}, {result}")