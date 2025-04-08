import logging
import random
import time
import json
import os
from typing import Dict, List, Optional, Set, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys

from bs4 import BeautifulSoup
import requests
from datetime import datetime

from common.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


class LazadaCrawler:
    def __init__(
        self,
        proxy_manager: Optional[ProxyManager] = None,
        max_concurrent_tabs: int = 6,  # Thay đổi mặc định thành 6 tab
        min_request_interval: int = 25,
        max_request_interval: int = 40,
        max_products: int = 100,
        headless: bool = True,
        output_dir: str = "/opt/airflow/data"
    ):
        """
        Khởi tạo crawler Lazada
        
        Args:
            proxy_manager: Manager xử lý proxy xoay
            max_concurrent_tabs: Số tab tối đa mở cùng lúc
            min_request_interval: Thời gian tối thiểu giữa các request (giây)
            max_request_interval: Thời gian tối đa giữa các request (giây)
            max_products: Số sản phẩm tối đa cần crawl
            headless: Chạy Chrome ở chế độ không có giao diện
            output_dir: Thư mục đầu ra cho dữ liệu
        """
        self.proxy_manager = proxy_manager
        self.max_concurrent_tabs = max_concurrent_tabs
        self.min_request_interval = min_request_interval
        self.max_request_interval = max_request_interval
        self.max_products = max_products
        self.headless = headless
        self.output_dir = output_dir
        
        self.drivers = []  # Lưu trữ danh sách các trình duyệt
        self.product_queue = set()  # Hàng đợi URL sản phẩm cần crawl
        self.processed_products = set()  # Danh sách URL sản phẩm đã xử lý
        self.results = []  # Kết quả crawl
        
        # Tạo thư mục lưu dữ liệu nếu chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
    
    def _init_driver(self, tab_idx: int = 0) -> webdriver.Chrome:
        """
        Khởi tạo trình duyệt Chrome với các tùy chọn phù hợp
        
        Args:
            tab_idx: Chỉ mục của tab, dùng để chọn proxy phù hợp
            
        Returns:
            Đối tượng WebDriver
        """
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
            
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Sử dụng User-Agent giống thiết bị desktop
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
        chrome_options.add_argument(f"--user-agent={user_agent}")
        
        # Tắt thông báo và các cảnh báo
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        })
        
        # Thêm proxy nếu có, dựa vào tab_idx
        if self.proxy_manager:
            proxy = self.proxy_manager.get_formatted_http_proxy(tab_idx)
            if proxy:
                chrome_options.add_argument(f"--proxy-server={proxy}")
                logger.info(f"Tab {tab_idx} sử dụng proxy: {proxy}")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)  # Timeout 60 giây cho việc tải trang
        
        return driver
    
    def _setup_drivers(self):
        """Khởi tạo nhiều trình duyệt theo số lượng tab tối đa"""
        self.drivers = []
        for i in range(self.max_concurrent_tabs):
            driver = self._init_driver(tab_idx=i)  # Truyền tab_idx để chọn proxy phù hợp
            self.drivers.append(driver)
        logger.info(f"Đã khởi tạo {len(self.drivers)} trình duyệt")
    
    def crawl_products(self):
        """Crawl thông tin từ tất cả các URL sản phẩm trong hàng đợi"""
        logger.info(f"Bắt đầu crawl {min(len(self.product_queue), self.max_products)} sản phẩm")
        
        counter = 0
        
        while self.product_queue and counter < self.max_products:
            active_crawls = []
            available_drivers = list(range(len(self.drivers)))
            
            # Xử lý tối đa max_concurrent_tabs sản phẩm cùng lúc
            while self.product_queue and available_drivers and counter < self.max_products:
                product_url = self.product_queue.pop()
                self.processed_products.add(product_url)
                
                driver_idx = available_drivers.pop(0)
                driver = self.drivers[driver_idx]
                
                logger.info(f"Crawling sản phẩm {counter+1}/{self.max_products}: {product_url} (Tab {driver_idx})")
                active_crawls.append((driver_idx, product_url))
                counter += 1
            
            # Xử lý các crawl đang hoạt động
            for driver_idx, product_url in active_crawls:
                driver = self.drivers[driver_idx]
                
                try:
                    # Nếu có proxy manager, đổi proxy trước mỗi request dựa vào tab_idx
                    if self.proxy_manager:
                        proxy = self.proxy_manager.get_formatted_http_proxy(driver_idx)
                        if proxy:
                            driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
                            driver.execute_cdp_cmd("Network.enable", {})
                            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"Proxy-Authorization": f"Basic {proxy}"}})
                    
                    product_info = self.extract_product_info(driver, product_url)
                    if product_info:
                        self.results.append(product_info)
                        
                        # Lưu kết quả ngay lập tức để tránh mất dữ liệu
                        self._save_product(product_info)
                        
                except Exception as e:
                    logger.error(f"Lỗi khi crawl sản phẩm {product_url} (Tab {driver_idx}): {str(e)}")
                
                # Tạm dừng giữa các request
                self._random_sleep()
        
        logger.info(f"Đã hoàn thành crawl {len(self.results)}/{counter} sản phẩm thành công")
    
    def _close_drivers(self):
        """Đóng tất cả các trình duyệt"""
        for driver in self.drivers:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"Lỗi khi đóng trình duyệt: {str(e)}")
        
        self.drivers = []
        logger.info("Đã đóng tất cả trình duyệt")
    
    def _random_sleep(self):
        """Tạm dừng một thời gian ngẫu nhiên giữa các request"""
        sleep_time = random.uniform(self.min_request_interval, self.max_request_interval)
        logger.debug(f"Nghỉ {sleep_time:.2f} giây...")
        time.sleep(sleep_time)
    
    def crawl_homepage(self, url: str = "https://www.lazada.vn/"):
        """
        Crawl trang chủ Lazada để lấy danh sách sản phẩm gợi ý
        
        Args:
            url: URL trang chủ Lazada
        """
        logger.info(f"Bắt đầu crawl trang chủ: {url}")
        
        try:
            # Sử dụng driver đầu tiên để crawl trang chủ
            driver = self.drivers[0]
            driver.get(url)
            
            # Đợi trang tải xong
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-jfy-item-wrapper, div.Bm3ON"))
            )
            
            # Cuộn trang để tải thêm sản phẩm
            self._scroll_page(driver)
            
            # Lấy các sản phẩm được gợi ý - thử nhiều selector khác nhau
            product_cards = []
            selectors = [
                "div.card-jfy-item-wrapper", 
                "div.Bm3ON", 
                "div[data-tracking='product-card']",
                "div.lzd-site-content a[href*='/products/']"
            ]
            
            for selector in selectors:
                cards = driver.find_elements(By.CSS_SELECTOR, selector)
                if cards:
                    product_cards = cards
                    logger.info(f"Tìm thấy {len(cards)} sản phẩm với selector: {selector}")
                    break
            
            # Nếu không tìm thấy bất kỳ sản phẩm nào, tìm tất cả các liên kết có thể
            if not product_cards:
                all_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/products/']")
                logger.info(f"Tìm thấy {len(all_links)} liên kết sản phẩm")
                
                for link in all_links:
                    product_url = link.get_attribute("href")
                    if product_url and "//www.lazada.vn/products/" in product_url:
                        logger.debug(f"Đã tìm thấy sản phẩm: {product_url}")
                        self.product_queue.add(product_url)
            else:
                # Xử lý các thẻ sản phẩm được tìm thấy
                for card in product_cards:
                    try:
                        # Tìm thẻ a trong card
                        links = card.find_elements(By.CSS_SELECTOR, "a")
                        
                        # Nếu không tìm thấy thẻ a, thử tìm thẻ div có sự kiện click
                        if not links:
                            # Thẻ chính là link
                            if card.tag_name == "a":
                                product_url = card.get_attribute("href")
                                if product_url and "//www.lazada.vn/products/" in product_url:
                                    self.product_queue.add(product_url)
                        else:
                            for link in links:
                                product_url = link.get_attribute("href")
                                if product_url and "//www.lazada.vn/products/" in product_url:
                                    logger.debug(f"Đã tìm thấy sản phẩm: {product_url}")
                                    self.product_queue.add(product_url)
                    except (NoSuchElementException, StaleElementReferenceException) as e:
                        logger.warning(f"Lỗi khi xử lý thẻ sản phẩm: {str(e)}")
            
            logger.info(f"Đã thêm {len(self.product_queue)} sản phẩm vào hàng đợi từ trang chủ")
            
            # Nếu không tìm được sản phẩm nào, thử tải một vài trang danh mục
            if not self.product_queue:
                category_urls = [
                    "https://www.lazada.vn/dien-thoai-may-tinh-bang/",
                    "https://www.lazada.vn/may-tinh-laptop/",
                    "https://www.lazada.vn/thiet-bi-dien-tu/"
                ]
                
                for category_url in category_urls:
                    self._crawl_category(category_url, driver)
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl trang chủ: {str(e)}")
    
    def _crawl_category(self, category_url: str, driver: webdriver.Chrome):
        """
        Crawl trang danh mục để lấy danh sách sản phẩm
        
        Args:
            category_url: URL trang danh mục
            driver: WebDriver đã khởi tạo
        """
        try:
            logger.info(f"Crawl trang danh mục: {category_url}")
            driver.get(category_url)
            
            # Đợi trang tải xong
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Bm3ON, div[data-tracking='product-card']"))
            )
            
            # Cuộn trang để tải thêm sản phẩm
            self._scroll_page(driver)
            
            # Lấy tất cả liên kết sản phẩm
            product_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/products/']")
            
            for link in product_links:
                product_url = link.get_attribute("href")
                if product_url and "//www.lazada.vn/products/" in product_url:
                    self.product_queue.add(product_url)
            
            logger.info(f"Đã thêm {len(self.product_queue)} sản phẩm từ danh mục {category_url}")
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl trang danh mục {category_url}: {str(e)}")
    
    def _scroll_page(self, driver, max_scrolls: int = 5):
        """
        Cuộn trang để tải thêm nội dung
        
        Args:
            driver: WebDriver
            max_scrolls: Số lần cuộn tối đa
        """
        for i in range(max_scrolls):
            driver.execute_script("window.scrollBy(0, 800)")
            time.sleep(1.5)  # Đợi nội dung tải
            
            # Thêm một số tương tác ngẫu nhiên để giống người dùng thật
            if random.random() > 0.7:
                try:
                    body = driver.find_element(By.TAG_NAME, "body")
                    body.send_keys(Keys.PAGE_DOWN)
                    time.sleep(0.5)
                except:
                    pass
    
    def extract_product_info(self, driver, product_url: str) -> Optional[Dict]:
        """
        Trích xuất thông tin sản phẩm từ trang chi tiết
        
        Args:
            driver: WebDriver
            product_url: URL sản phẩm
            
        Returns:
            Thông tin sản phẩm dạng dict hoặc None nếu có lỗi
        """
        try:
            logger.info(f"Trích xuất thông tin sản phẩm: {product_url}")
            
            # Tải trang sản phẩm
            driver.get(product_url)
            
            # Đợi trang tải xong - thử nhiều selector khác nhau
            selectors = [
                "div.pdp-product-title", 
                "h1.pdp-mod-product-badge-title",
                "h1[class*='pdp-title']"
            ]
            
            for selector in selectors:
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except:
                    continue
            
            # Cuộn trang để tải tất cả nội dung
            self._scroll_page(driver, max_scrolls=3)
            
            # Lấy tên sản phẩm
            product_name = None
            for selector in ["div.pdp-product-title", "h1.pdp-mod-product-badge-title", "h1[class*='pdp-title']"]:
                try:
                    product_name = driver.find_element(By.CSS_SELECTOR, selector).text.strip()
                    if product_name:
                        break
                except:
                    continue
            
            if not product_name:
                logger.warning(f"Không thể tìm thấy tên sản phẩm cho {product_url}")
                product_name = "Không xác định"
            
            # Lấy đánh giá (rating) nếu có
            rating = "Chưa có đánh giá"
            for selector in ["div.score-average", "span[class*='score']"]:
                try:
                    rating_element = driver.find_element(By.CSS_SELECTOR, selector)
                    rating = rating_element.text.strip()
                    if rating:
                        break
                except:
                    continue
            
            # Lấy thương hiệu
            brand = "Không xác định"
            brand_selectors = [
                "a.pdp-link.pdp-link_size_s.pdp-link_theme_blue.pdp-product-brand__brand-link",
                "a[data-spm-anchor-id*='brand']",
                "div[class*='brand'] a",
                "div[class*='Brand'] a"
            ]
            
            for selector in brand_selectors:
                try:
                    brand_element = driver.find_element(By.CSS_SELECTOR, selector)
                    brand = brand_element.text.strip()
                    if brand:
                        break
                except:
                    continue
            
            # Nếu vẫn chưa tìm thấy, tìm kiếm thông tin thương hiệu trong thông số kỹ thuật
            if brand == "Không xác định":
                try:
                    spec_selectors = ["div.pdp-product-detail", "div[class*='specification']"]
                    for spec_selector in spec_selectors:
                        try:
                            spec_table = driver.find_element(By.CSS_SELECTOR, spec_selector)
                            spec_rows = spec_table.find_elements(By.CSS_SELECTOR, "div.pdp-product-spec-item, div[class*='key-value']")
                            
                            for row in spec_rows:
                                try:
                                    key_selectors = ["span.key-title", "span[class*='key']", "div[class*='key']"]
                                    key = None
                                    for key_selector in key_selectors:
                                        try:
                                            key = row.find_element(By.CSS_SELECTOR, key_selector).text.strip()
                                            if key:
                                                break
                                        except:
                                            continue
                                    
                                    if key and ("thương hiệu" in key.lower() or "brand" in key.lower()):
                                        value_selectors = ["div.value-title", "span[class*='value']", "div[class*='value']"]
                                        for value_selector in value_selectors:
                                            try:
                                                brand = row.find_element(By.CSS_SELECTOR, value_selector).text.strip()
                                                if brand:
                                                    break
                                            except:
                                                continue
                                        
                                        if brand:
                                            break
                                except:
                                    continue
                            
                            if brand != "Không xác định":
                                break
                        except:
                            continue
                except:
                    pass
            
            # Lấy phân loại sản phẩm từ breadcrumb
            category = "Không xác định"
            try:
                breadcrumb_selectors = [
                    "span.breadcrumb_item_text", 
                    "a[class*='breadcrumb']", 
                    "div[class*='breadcrumb'] a",
                    "div[data-mod-name*='breadcrumb'] a"
                ]
                
                for selector in breadcrumb_selectors:
                    breadcrumbs = driver.find_elements(By.CSS_SELECTOR, selector)
                    if breadcrumbs:
                        categories = [b.text.strip() for b in breadcrumbs if b.text.strip()]
                        if categories:
                            category = " > ".join(categories)
                            break
            except:
                pass
            
            # Lấy giá gốc và giá khuyến mãi
            original_price = None
            price_selectors = [
                "span.pdp-price.pdp-price_type_deleted.pdp-price_color_lightgray.pdp-price_size_xs",
                "del[class*='price']",
                "span[class*='price'][class*='deleted']",
                "span[class*='price'][class*='original']"
            ]
            
            for selector in price_selectors:
                try:
                    original_price_element = driver.find_element(By.CSS_SELECTOR, selector)
                    original_price = original_price_element.text.strip()
                    if original_price:
                        break
                except:
                    continue
            
            sale_price = "Không có thông tin"
            sale_price_selectors = [
                "span.pdp-price.pdp-price_type_normal.pdp-price_color_orange.pdp-price_size_xl",
                "span[class*='price'][class*='normal']",
                "span[class*='price'][class*='sale']",
                "div[class*='price-box'] span"
            ]
            
            for selector in sale_price_selectors:
                try:
                    sale_price_element = driver.find_element(By.CSS_SELECTOR, selector)
                    sale_price = sale_price_element.text.strip()
                    if sale_price:
                        break
                except:
                    continue
            
            # Nếu không có giá khuyến mãi, thử lấy giá thường
            if sale_price == "Không có thông tin":
                try:
                    sale_price_element = driver.find_element(By.CSS_SELECTOR, "span.pdp-price, div[class*='price']")
                    sale_price = sale_price_element.text.strip()
                except:
                    pass
            
            # Lấy mô tả sản phẩm
            description = "Không có mô tả"
            description_selectors = [
                "div.pdp-product-detail.pdp-product-desc",
                "div[class*='html-content']",
                "div[class*='description']"
            ]
            
            for selector in description_selectors:
                try:
                    description_element = driver.find_element(By.CSS_SELECTOR, selector)
                    description = description_element.get_attribute("innerHTML").strip()
                    if description:
                        break
                except:
                    continue
            
            # Lấy các lựa chọn sản phẩm và ảnh tương ứng
            variations = []
            try:
                # Lưu ảnh chính trước
                main_image = None
                image_selectors = [
                    "div.gallery-preview-panel__content img",
                    "div[class*='gallery'] img",
                    "div[class*='image'] img"
                ]
                
                for selector in image_selectors:
                    try:
                        main_image_element = driver.find_element(By.CSS_SELECTOR, selector)
                        main_image = main_image_element.get_attribute("src")
                        if main_image:
                            break
                    except:
                        continue
                
                if not main_image:
                    # Nếu không tìm thấy, tìm tất cả ảnh và lấy ảnh đầu tiên
                    all_images = driver.find_elements(By.TAG_NAME, "img")
                    for img in all_images:
                        if img.is_displayed() and img.size['width'] > 200:
                            main_image = img.get_attribute("src")
                            if main_image:
                                break
                
                # Tìm các thẻ lựa chọn (ví dụ: màu sắc, kích thước)
                variation_container_selectors = [
                    "div.sku-selector-container",
                    "div[class*='sku-selector']",
                    "div[class*='variation']",
                    "div[class*='property']"
                ]
                
                variation_containers = []
                for selector in variation_container_selectors:
                    containers = driver.find_elements(By.CSS_SELECTOR, selector)
                    if containers:
                        variation_containers = containers
                        break
                
                for container in variation_containers:
                    try:
                        # Lấy tên thuộc tính (Màu sắc, Kích thước, v.v.)
                        attribute_name = "Không xác định"
                        title_selectors = [
                            "div.sku-selector__title",
                            "div[class*='title']",
                            "div[class*='name']",
                            "div[class*='label']"
                        ]
                        
                        for selector in title_selectors:
                            try:
                                title_element = container.find_element(By.CSS_SELECTOR, selector)
                                attribute_name = title_element.text.strip()
                                if attribute_name:
                                    break
                            except:
                                continue
                        
                        # Lấy các lựa chọn
                        option_elements = []
                        option_selectors = [
                            "div.sku-selector-option",
                            "div[class*='option']",
                            "div[class*='item']",
                            "li[class*='item']"
                        ]
                        
                        for selector in option_selectors:
                            options = container.find_elements(By.CSS_SELECTOR, selector)
                            if options:
                                option_elements = options
                                break
                        
                        current_image = main_image
                        
                        for option in option_elements:
                            try:
                                # Click vào tùy chọn để xem ảnh thay đổi
                                option.click()
                                time.sleep(1.5)  # Đợi ảnh tải
                                
                                # Lấy ảnh mới nếu có
                                new_image = None
                                for selector in image_selectors:
                                    try:
                                        image_element = driver.find_element(By.CSS_SELECTOR, selector)
                                        new_image = image_element.get_attribute("src")
                                        if new_image:
                                            break
                                    except:
                                        continue
                                
                                if not new_image:
                                    new_image = current_image
                                else:
                                    current_image = new_image
                                
                                option_text = option.text.strip()
                                if not option_text:
                                    # Nếu không có text, có thể là tùy chọn màu sắc có hình ảnh
                                    option_text = option.get_attribute("title") or "Không có tên"
                                
                                variations.append({
                                    "attribute": attribute_name,
                                    "value": option_text,
                                    "image": new_image
                                })
                            except Exception as e:
                                logger.warning(f"Lỗi khi xử lý tùy chọn: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Lỗi khi xử lý container tùy chọn: {str(e)}")
                
                # Nếu không tìm thấy tùy chọn, thêm ảnh chính
                if not variations and main_image:
                    variations.append({
                        "attribute": "Mặc định",
                        "value": "Mặc định",
                        "image": main_image
                    })
                elif not variations:
                    # Tìm tất cả ảnh sản phẩm và thêm vào
                    all_thumbs_selectors = [
                        "div.gallery-thumbnail-item img",
                        "div[class*='thumbnail'] img",
                        "div[class*='thumb'] img"
                    ]
                    
                    for selector in all_thumbs_selectors:
                        try:
                            thumbs = driver.find_elements(By.CSS_SELECTOR, selector)
                            if thumbs:
                                for i, thumb in enumerate(thumbs):
                                    thumb_src = thumb.get_attribute("src")
                                    if thumb_src:
                                        variations.append({
                                            "attribute": "Hình ảnh",
                                            "value": f"Hình {i+1}",
                                            "image": thumb_src
                                        })
                                break
                        except:
                            continue
            except Exception as e:
                logger.warning(f"Lỗi khi xử lý biến thể sản phẩm: {str(e)}")
                # Thêm ảnh mặc định nếu có lỗi
                try:
                    main_image = driver.find_element(By.CSS_SELECTOR, "div[class*='gallery'] img").get_attribute("src")
                    variations.append({
                        "attribute": "Mặc định",
                        "value": "Mặc định",
                        "image": main_image
                    })
                except:
                    pass
            
            # Tìm thêm sản phẩm từ phần "Có thể bạn cũng thích"
            try:
                related_selectors = [
                    "div.card-product-related",
                    "div[class*='related']",
                    "div[class*='recommend'] a",
                    "div[data-mod-name*='recommend'] a"
                ]
                
                for selector in related_selectors:
                    related_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if related_elements:
                        for element in related_elements:
                            try:
                                # Tìm thẻ a trong element
                                links = element.find_elements(By.CSS_SELECTOR, "a")
                                
                                # Nếu element là thẻ a
                                if not links and element.tag_name == "a":
                                    related_url = element.get_attribute("href")
                                    if related_url and "//www.lazada.vn/products/" in related_url and related_url not in self.processed_products:
                                        self.product_queue.add(related_url)
                                else:
                                    for link in links:
                                        related_url = link.get_attribute("href")
                                        if related_url and "//www.lazada.vn/products/" in related_url and related_url not in self.processed_products:
                                            self.product_queue.add(related_url)
                            except:
                                continue
                        break
            except Exception as e:
                logger.warning(f"Lỗi khi tìm sản phẩm liên quan: {str(e)}")
            
            # Tạo đối tượng thông tin sản phẩm
            product_info = {
                "source": "lazada",
                "url": product_url,
                "name": product_name,
                "rating": rating,
                "brand": brand,
                "category": category,
                "original_price": original_price,
                "sale_price": sale_price,
                "description": description,
                "variations": variations,
                "crawl_time": datetime.now().isoformat()
            }
            
            return product_info
            
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất thông tin sản phẩm từ {product_url}: {str(e)}")
            return None
    
    def crawl_products(self):
        """Crawl thông tin từ tất cả các URL sản phẩm trong hàng đợi"""
        logger.info(f"Bắt đầu crawl {min(len(self.product_queue), self.max_products)} sản phẩm")
        
        counter = 0
        
        while self.product_queue and counter < self.max_products:
            active_crawls = []
            available_drivers = list(range(len(self.drivers)))
            
            # Xử lý tối đa max_concurrent_tabs sản phẩm cùng lúc
            while self.product_queue and available_drivers and counter < self.max_products:
                product_url = self.product_queue.pop()
                self.processed_products.add(product_url)
                
                driver_idx = available_drivers.pop(0)
                driver = self.drivers[driver_idx]
                
                logger.info(f"Crawling sản phẩm {counter+1}/{self.max_products}: {product_url}")
                active_crawls.append((driver_idx, product_url))
                counter += 1
            
            # Xử lý các crawl đang hoạt động
            for driver_idx, product_url in active_crawls:
                driver = self.drivers[driver_idx]
                
                try:
                    # Nếu có proxy manager, đổi proxy trước mỗi request
                    if self.proxy_manager:
                        proxy = self.proxy_manager.get_formatted_http_proxy()
                        if proxy:
                            driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
                            driver.execute_cdp_cmd("Network.enable", {})
                            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"Proxy-Authorization": f"Basic {proxy}"}})
                    
                    product_info = self.extract_product_info(driver, product_url)
                    if product_info:
                        self.results.append(product_info)
                        
                        # Lưu kết quả ngay lập tức để tránh mất dữ liệu
                        self._save_product(product_info)
                        
                except Exception as e:
                    logger.error(f"Lỗi khi crawl sản phẩm {product_url}: {str(e)}")
                
                # Tạm dừng giữa các request
                self._random_sleep()
        
        logger.info(f"Đã hoàn thành crawl {len(self.results)}/{counter} sản phẩm thành công")
    
    def _save_product(self, product_info: Dict):
        """
        Lưu thông tin sản phẩm vào file
        
        Args:
            product_info: Thông tin sản phẩm
        """
        try:
            # Tạo tên file dựa trên ID sản phẩm (từ URL)
            product_id = product_info["url"].split("/")[-1].split(".")[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.output_dir}/lazada_product_{product_id}_{timestamp}.json"
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(product_info, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Đã lưu thông tin sản phẩm: {filename}")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu thông tin sản phẩm: {str(e)}")
    
    def save_results(self, filename: str = None):
        """
        Lưu tất cả kết quả vào một file
        
        Args:
            filename: Tên file đầu ra, mặc định sẽ tự động tạo
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.output_dir}/lazada_products_{timestamp}.json"
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Đã lưu tất cả {len(self.results)} sản phẩm vào: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu kết quả: {str(e)}")
            return None
    
    def run(self, homepage_url: str = "https://www.lazada.vn/"):
        """
        Chạy toàn bộ quá trình crawl
        
        Args:
            homepage_url: URL trang chủ để bắt đầu
            
        Returns:
            Danh sách sản phẩm đã crawl
        """
        try:
            logger.info("Bắt đầu quá trình crawl Lazada")
            
            # Khởi tạo trình duyệt
            self._setup_drivers()
            
            # Crawl trang chủ để lấy danh sách sản phẩm
            self.crawl_homepage(homepage_url)
            
            # Crawl chi tiết từng sản phẩm
            self.crawl_products()
            
            # Lưu kết quả
            self.save_results()
            
            return self.results
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình crawl: {str(e)}")
            return []
            
        finally:
            # Đóng tất cả trình duyệt
            self._close_drivers()
            logger.info("Đã kết thúc quá trình crawl")


if __name__ == "__main__":
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Khởi tạo proxy manager
    proxy_manager = ProxyManager(
        api_key="your_api_key_here",
        networks=["fpt", "viettel"]
    )
    proxy_manager.start_auto_refresh(min_interval=60, max_interval=120)
    
    try:
        # Khởi tạo crawler
        crawler = LazadaCrawler(
            proxy_manager=proxy_manager,
            max_concurrent_tabs=5,
            min_request_interval=25,
            max_request_interval=40,
            max_products=20,
            headless=True
        )
        
        # Chạy crawler
        results = crawler.run()
        
        print(f"Đã crawl được {len(results)} sản phẩm")
        
    finally:
        # Dừng auto refresh proxy
        proxy_manager.stop_auto_refresh()