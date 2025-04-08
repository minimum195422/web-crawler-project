#!/bin/bash

# Thoát nếu có lỗi
set -e

# Khởi tạo biến môi trường trên Airflow
if [ -f /.env ]; then
  echo "Loading variables from /.env"
  export $(cat /.env | xargs)
fi

# Kiểm tra nếu các biến cần thiết đã được cài đặt
if [ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
  echo "WARNING: AWS credentials are not set. S3 upload will not work."
fi

if [ -z "${PROXY_API_KEY}" ]; then
  echo "WARNING: Proxy API key is not set. Proxy rotation will not work."
fi

# Tạo thư mục cần thiết nếu chưa tồn tại
mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/data

# Cấp quyền cho thư mục
chown -R "${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-0}" /opt/airflow/logs /opt/airflow/data

# Chạy lệnh truyền vào
exec airflow "$@"