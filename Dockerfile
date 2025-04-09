FROM apache/airflow:2.7.1-python3.9

USER root

# Cài đặt các dependencies hệ thống
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    curl \
    software-properties-common \
    ssh \
    rsync \
    git \
    unzip \
    wget \
    vim \
    ca-certificates \
    gnupg \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Cài đặt Chrome và ChromeDriver theo cách mới
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update \
  && apt-get install -y google-chrome-stable \
  && CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1) \
  && CHROMEDRIVER_VERSION=$(curl -sS "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_VERSION") \
  && wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip" \
  && unzip chromedriver-linux64.zip \
  && mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
  && chmod +x /usr/local/bin/chromedriver \
  && rm -rf chromedriver-linux64 chromedriver-linux64.zip

# Chuyển sang user airflow
USER airflow

# Cài đặt Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Tạo thư mục đầu ra cho dữ liệu crawl
RUN mkdir -p /opt/airflow/data

# Copy các scripts khởi tạo
COPY --chown=airflow:root scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]