# Lazada Web Crawler

Dự án crawler dữ liệu sản phẩm từ Lazada sử dụng Apache Airflow và Docker.

## Tính năng

- Crawl dữ liệu sản phẩm từ Lazada tự động thông qua DAG Airflow
- Quản lý proxy xoay để tránh bị chặn IP
- Mở 5 tab trình duyệt cùng lúc để tăng tốc độ crawl
- Giãn cách các request từ 25-40 giây (có thể cấu hình)
- Lưu dữ liệu dưới dạng JSON và CSV
- Tự động upload dữ liệu lên Amazon S3
- Quản lý hàng đợi crawl để lấy thêm sản phẩm từ phần "Có thể bạn cũng thích"

## Thông tin sản phẩm được crawl

- Nguồn dữ liệu (Lazada)
- Tên sản phẩm
- Đánh giá
- Thương hiệu
- Phân loại sản phẩm
- Lựa chọn mua hàng kèm ảnh tương ứng
- Mô tả sản phẩm
- Giá gốc
- Giá khuyến mãi

## Cài đặt

### Yêu cầu

- Docker và Docker Compose
- Git

### Bước 1: Clone dự án

```bash
git clone https://github.com/yourusername/web-crawler-project.git
cd web-crawler-project
```

### Bước 2: Cấu hình biến môi trường

Sao chép file `.env.example` thành `.env` và chỉnh sửa các biến môi trường:

```bash
cp .env.example .env
```

Chỉnh sửa các thông tin sau trong file `.env`:

```
# Airflow settings
AIRFLOW_UID=50000
AIRFLOW_GID=0

# AWS credentials
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=ap-southeast-1
S3_BUCKET_NAME=your-data-lake-bucket

# Proxy service settings
PROXY_API_KEY=your_proxy_api_key
PROXY_NETWORKS=fpt,viettel
PROXY_MIN_INTERVAL=60
PROXY_MAX_INTERVAL=120

# Crawl settings
MAX_PRODUCTS=100
MIN_REQUEST_INTERVAL=25
MAX_REQUEST_INTERVAL=40
```

### Bước 3: Khởi động môi trường Docker

```bash
docker-compose up -d
```

Lần đầu tiên khởi động sẽ mất một chút thời gian để xây dựng các images và cài đặt các dependencies.

### Bước 4: Truy cập Airflow UI

Truy cập Airflow UI tại địa chỉ: http://localhost:8080

- Username: admin
- Password: admin

## Cấu trúc dự án

```
web-crawler-project/
├── .env                        # Biến môi trường
├── .gitignore                  # Cấu hình Git ignore
├── docker-compose.yaml         # Cấu hình Docker Compose
├── Dockerfile                  # Dockerfile tùy chỉnh cho Airflow
├── requirements.txt            # Các thư viện Python cần thiết
├── README.md                   # Tài liệu hướng dẫn
├── scripts/
│   ├── entrypoint.sh           # Script khởi động container
│   └── init.sh                 # Script khởi tạo
├── dags/
│   ├── common/                 # Các module dùng chung
│   │   ├── constants.py        # Các hằng số và cấu hình chung
│   │   ├── proxy_manager.py    # Quản lý proxy xoay
│   │   ├── s3_utils.py         # Tiện ích làm việc với S3
│   │   └── utils.py            # Các tiện ích chung
│   ├── lazada/                 # Crawl từ Lazada
│   │   ├── crawler_lazada.py   # Logic crawl cho Lazada
│   │   ├── parser_lazada.py    # Xử lý dữ liệu từ Lazada
│   │   └── dag_lazada.py       # DAG Airflow cho Lazada
│   └── main_dag.py             # DAG chính điều phối các DAG con
└── plugins/
    ├── operators/              # Các Operator tùy chỉnh
    │   └── crawler_operator.py # Operator cho crawler
    └── hooks/                  # Các Hook tùy chỉnh
        └── proxy_hook.py       # Hook kết nối với dịch vụ proxy
```

## Sử dụng

### Kích hoạt DAG thủ công

1. Truy cập Airflow UI tại http://localhost:8080
2. Tìm `lazada_crawler` DAG và kích hoạt bằng cách bật công tắc
3. Trigger DAG bằng nút "Trigger DAG"

### Theo dõi quá trình crawl

- Xem các task trong DAG bằng cách click vào tên DAG
- Xem logs của từng task để xem tiến trình
- Dữ liệu crawl được sẽ được lưu trong thư mục `/opt/airflow/data` trong container

### Upload dữ liệu lên S3

Task `upload_to_s3` sẽ tự động upload dữ liệu lên S3 bucket đã cấu hình.

## Tùy chỉnh

### Thay đổi số lượng sản phẩm crawl

Chỉnh sửa biến `MAX_PRODUCTS` trong file `.env`

### Thay đổi thời gian giữa các request

Chỉnh sửa các biến `MIN_REQUEST_INTERVAL` và `MAX_REQUEST_INTERVAL` trong file `.env`

### Thay đổi cấu hình proxy

Chỉnh sửa các biến `PROXY_API_KEY`, `PROXY_NETWORKS`, `PROXY_MIN_INTERVAL` và `PROXY_MAX_INTERVAL` trong file `.env`

### Thêm crawler mới

1. Tạo thư mục mới trong `dags/` cho nguồn dữ liệu mới (ví dụ: `dags/shopee/`)
2. Tạo các file tương tự như đã làm với Lazada: `crawler_shopee.py`, `parser_shopee.py`, `dag_shopee.py`
3. Cập nhật `main_dag.py` để thêm DAG mới vào danh sách điều phối

## Xử lý lỗi thường gặp

### Proxy không hoạt động

- Kiểm tra API key và các thông tin proxy trong `.env`
- Kiểm tra logs của proxy manager trong task `crawl_lazada`

### Chrome không khởi động được

- Kiểm tra logs để xem lỗi
- Đảm bảo Chrome và ChromeDriver đã được cài đặt đúng trong container
- Thử chạy trong chế độ không headless bằng cách đặt `HEADLESS_BROWSER=false` trong `.env`

### Không tìm thấy sản phẩm

- Kiểm tra các CSS selector trong `constants.py`
- Cập nhật các selector phù hợp với cấu trúc hiện tại của trang Lazada
- Thêm các selector thay thế để tăng khả năng tìm kiếm

### Lỗi khi upload lên S3

- Kiểm tra AWS credentials trong `.env`
- Đảm bảo bucket đã tồn tại và có quyền truy cập
- Kiểm tra logs của task `upload_to_s3`

## Lưu ý bảo mật

- Không commit file `.env` lên repository
- Sử dụng các biến môi trường để lưu trữ thông tin nhạy cảm
- Thiết lập quyền truy cập S3 phù hợp
- Hạn chế số lượng request để tránh bị chặn IP

## Giám sát và bảo trì

### Logs

- Logs của Airflow được lưu trong thư mục `logs/`
- Logs của crawler được ghi vào stdout/stderr và có thể xem trong Airflow UI

### Sao lưu dữ liệu

- Dữ liệu crawl được lưu ở hai nơi: thư mục `data/` trong container và S3 bucket
- Nên thiết lập lifecycle policy cho S3 bucket để quản lý dữ liệu theo thời gian

### Cập nhật dự án

```bash
# Pull thay đổi mới từ repository
git pull

# Rebuild Docker image
docker-compose down
docker-compose build
docker-compose up -d
```

## Đóng góp

Mọi đóng góp đều được hoan nghênh! Vui lòng tạo issue hoặc pull request để cải thiện dự án.

## Giấy phép

[MIT License](LICENSE)