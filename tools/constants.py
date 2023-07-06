#   Copyright 2022 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from os import environ
from urllib.parse import urlparse

LOCAL_DEV = True

# ALLOWED_EXTENSIONS = ['zip', 'py']
CURRENT_RELEASE = 'latest'
REDIS_USER = environ.get('REDIS_USER', '')
REDIS_PASSWORD = environ['REDIS_PASSWORD']
REDIS_HOST = environ['REDIS_HOST']
REDIS_PORT = environ.get('REDIS_PORT', 6379)
REDIS_DB = environ.get('REDIS_DB', 2)
RABBIT_HOST = environ['RABBIT_HOST']
RABBIT_USER = environ['RABBITMQ_USER']
RABBIT_PASSWORD = environ['RABBITMQ_PASSWORD']
RABBIT_PORT = environ.get('RABBIT_PORT', 5672)
RABBIT_QUEUE_NAME = environ.get('RABBIT_QUEUE_NAME', 'default')
APP_HOST = environ['APP_HOST']
APP_IP = environ['APP_IP']
GF_API_KEY = environ.get('GF_API_KEY', '')
INFLUX_PASSWORD = environ.get('INFLUX_PASSWORD', '')
INFLUX_USER = environ.get('INFLUX_USER', '')
INFLUX_PORT = environ.get('INFLUX_PORT', 8086)
_url = urlparse(APP_HOST)
LOKI_HOST = environ.get(
    'LOKI_HOST',
    APP_HOST.replace("https://", "http://") if APP_IP in APP_HOST else f"http://{APP_IP}"
)
LOKI_PORT = environ.get('LOKI_PORT', 3100)
# INTERNAL_LOKI_HOST = "http://carrier-loki"
MINIO_ENDPOINT = environ.get('MINIO_HOST', 'http://carrier-minio:9000')
MINIO_ACCESS = environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = environ.get('MINIO_SECRET_KEY', 'password')
MINIO_REGION = environ.get('MINIO_REGION', 'us-east-1')
MAX_DOTS_ON_CHART = 100
VAULT_URL = environ.get('VAULT_URL', 'http://carrier-vault:8200')
VAULT_DB_PK = 1
ADMINISTRATION_MODE = 'administration'
VAULT_ADMINISTRATION_NAME = ADMINISTRATION_MODE
DEFAULT_MODE = 'default'
GRID_ROUTER_URL = environ.get("GRID_ROUTER_URL", f"{LOKI_HOST}:4444/quota")

BACKEND_PERFORMANCE_RESULTS_RETENTION = 30  # in days
BUCKET_RETENTION_DAYS = 7
