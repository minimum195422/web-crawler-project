"""
Các hằng số và cấu hình chung cho toàn bộ dự án
"""

import os
from datetime import timedelta

# Thư mục dữ liệu
DATA_DIR = "/opt/airflow/data"

# Cấu hình Airflow
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': int(os.getenv('DEFAULT_RETRIES', '3')),
    'retry_delay': timedelta(minutes=5),
}

# Cấu hình Proxy
PROXY_API_KEY = os.getenv('PROXY_API_KEY')
PROXY_API_URL = os.getenv('PROXY_API_URL', 'https://proxyxoay.shop/api/get.php')
PROXY_NETWORKS = os.getenv('PROXY_NETWORKS', 'fpt,viettel').split(',')
PROXY_MIN_INTERVAL = int(os.getenv('PROXY_MIN_INTERVAL', '60'))
PROXY_MAX_INTERVAL = int(os.getenv('PROXY_MAX_INTERVAL', '120'))

# Cấu hình S3
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = os.getenv('S3_PREFIX', 'crawled-data')

# Cấu hình Crawler
MAX_PRODUCTS = int(os.getenv('MAX_PRODUCTS', '100'))
MIN_REQUEST_INTERVAL = int(os.getenv('MIN_REQUEST_INTERVAL', '25'))
MAX_REQUEST_INTERVAL = int(os.getenv('MAX_REQUEST_INTERVAL', '40'))
MAX_CONCURRENT_TABS = int(os.getenv('MAX_CONCURRENT_TABS', '5'))
HEADLESS_BROWSER = os.getenv('HEADLESS_BROWSER', 'true').lower() == 'true'

# Danh sách User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
]

# Danh sách URL Lazada
LAZADA_HOMEPAGE_URL = "https://www.lazada.vn/"
LAZADA_CATEGORY_URLS = [
    "https://www.lazada.vn/dien-thoai-may-tinh-bang/",
    "https://www.lazada.vn/may-tinh-laptop/",
    "https://www.lazada.vn/thiet-bi-dien-tu/",
    "https://www.lazada.vn/phu-kien-thiet-bi-so/",
    "https://www.lazada.vn/tv-thiet-bi-nghe-nhin/",
    "https://www.lazada.vn/do-gia-dung-lon/",
    "https://www.lazada.vn/do-dung-nha-bep-va-hang-gia-dung/",
    "https://www.lazada.vn/cham-soc-nha-cua/",
    "https://www.lazada.vn/do-dung-nha-tam-nha-ve-sinh/",
    "https://www.lazada.vn/cham-soc-ca-nhan/"
]

# CSS Selector cho việc crawl Lazada
LAZADA_SELECTORS = {
    # Trang chủ
    'product_cards': [
        "div.card-jfy-item-wrapper", 
        "div.Bm3ON", 
        "div[data-tracking='product-card']",
        "div.lzd-site-content a[href*='/products/']"
    ],
    
    # Trang sản phẩm
    'product_title': [
        "div.pdp-product-title", 
        "h1.pdp-mod-product-badge-title",
        "h1[class*='pdp-title']"
    ],
    'product_rating': [
        "div.score-average", 
        "span[class*='score']"
    ],
    'product_brand': [
        "a.pdp-link.pdp-link_size_s.pdp-link_theme_blue.pdp-product-brand__brand-link",
        "a[data-spm-anchor-id*='brand']",
        "div[class*='brand'] a",
        "div[class*='Brand'] a"
    ],
    'product_original_price': [
        "span.pdp-price.pdp-price_type_deleted.pdp-price_color_lightgray.pdp-price_size_xs",
        "del[class*='price']",
        "span[class*='price'][class*='deleted']",
        "span[class*='price'][class*='original']"
    ],
    'product_sale_price': [
        "span.pdp-price.pdp-price_type_normal.pdp-price_color_orange.pdp-price_size_xl",
        "span[class*='price'][class*='normal']",
        "span[class*='price'][class*='sale']",
        "div[class*='price-box'] span"
    ],
    'product_description': [
        "div.pdp-product-detail.pdp-product-desc",
        "div[class*='html-content']",
        "div[class*='description']"
    ],
    'product_image': [
        "div.gallery-preview-panel__content img",
        "div[class*='gallery'] img",
        "div[class*='image'] img"
    ],
    'product_variations': [
        "div.sku-selector-container",
        "div[class*='sku-selector']",
        "div[class*='variation']",
        "div[class*='property']"
    ],
    'related_products': [
        "div.card-product-related",
        "div[class*='related']",
        "div[class*='recommend'] a",
        "div[data-mod-name*='recommend'] a"
    ],
    'breadcrumb': [
        "span.breadcrumb_item_text", 
        "a[class*='breadcrumb']", 
        "div[class*='breadcrumb'] a",
        "div[data-mod-name*='breadcrumb'] a"
    ]
}

# Cấu hình logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Cấu hình timeout
DEFAULT_TIMEOUT = int(os.getenv('DEFAULT_TIMEOUT', '30'))  # giây
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))  # giây
ELEMENT_WAIT_TIMEOUT = int(os.getenv('ELEMENT_WAIT_TIMEOUT', '10'))  # giây

# Tần suất chạy DAG
DEFAULT_SCHEDULE_INTERVAL = os.getenv('DEFAULT_SCHEDULE_INTERVAL', '@daily')