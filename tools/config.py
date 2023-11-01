#   Copyright 2023 getcarrier.io
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
from pylon.core.tools import log
from ..patterns import SingletonABC


class Config(metaclass=SingletonABC):
    LOCAL_DEV = bool(environ.get('LOCAL_DEV'))
    # this determines whether some secrets are overwritten
    # with env values upon centry restart
    PERSISTENT_SECRETS = bool(environ.get('PERSISTENT_SECRETS'))
    CURRENT_RELEASE = environ.get('CURRENT_RELEASE', 'latest')
    ADMINISTRATION_MODE = 'administration'
    DEFAULT_MODE = 'default'
    MAX_DOTS_ON_CHART = 100
    BACKEND_PERFORMANCE_RESULTS_RETENTION = 30  # in days
    BUCKET_RETENTION_DAYS = 7
    PERSONAL_PROJECT_NAME = 'Project_User_{user_id}'

    APP_IP = environ['APP_IP']
    APP_HOST = environ['APP_HOST']
    APP_SCHEME = environ.get('APP_SCHEME', 'http')
    ALLOW_CORS = environ.get('ALLOW_CORS', False)

    REDIS_USER = environ.get('REDIS_USER', '')
    REDIS_PASSWORD = environ['REDIS_PASSWORD']
    REDIS_HOST = environ.get('REDIS_HOST', 'carrier-redis')
    REDIS_PORT = environ.get('REDIS_PORT', 6379)
    REDIS_DB = environ.get('REDIS_DB', 2)
    REDIS_RABBIT_DB = environ.get('REDIS_RABBIT_DB', 4)

    RABBIT_HOST = environ.get('RABBIT_HOST', 'carrier-rabbit')
    RABBIT_USER = environ['RABBIT_USER']
    RABBIT_PASSWORD = environ['RABBIT_PASSWORD']
    RABBIT_PORT = environ.get('RABBIT_PORT', 5672)
    RABBIT_QUEUE_NAME = environ.get('RABBIT_QUEUE_NAME', 'default')
    RABBIT_USE_SSL = environ.get("RABBIT_USE_SSL", "").lower() in ["true", "yes"]
    RABBIT_SSL_VERIFY = environ.get("RABBIT_SSL_VERIFY", "").lower() in ["true", "yes"]

    # GF_API_KEY = environ.get('GF_API_KEY', '')
    INFLUX_PASSWORD = environ.get('INFLUX_PASSWORD', '')
    INFLUX_USER = environ.get('INFLUX_USER', '')
    INFLUX_PORT = environ.get('INFLUX_PORT', 8086)

    LOKI_HOST = environ.get('LOKI_HOST', APP_HOST)
    LOKI_HOST_INTERNAL = environ.get('LOKI_HOST_INTERNAL', 'http://carrier-loki')
    LOKI_PORT = environ.get('LOKI_PORT', 3100)

    MINIO_URL = environ.get('MINIO_URL', 'http://carrier-minio:9000')
    MINIO_ACCESS = environ['MINIO_ACCESS_KEY']
    MINIO_ACCESS_KEY = MINIO_ACCESS
    MINIO_SECRET = environ['MINIO_SECRET_KEY']
    MINIO_SECRET_KEY = MINIO_SECRET
    MINIO_REGION = environ.get('MINIO_REGION', 'us-east-1')

    VAULT_URL = environ.get('VAULT_URL', 'http://carrier-vault:8200')
    VAULT_DB_PK = 1
    VAULT_ADMINISTRATION_NAME = ADMINISTRATION_MODE

    DATABASE_VENDOR = environ['DATABASE_VENDOR']
    POSTGRES_SCHEMA = environ['POSTGRES_SCHEMA']
    POSTGRES_HOST = environ['POSTGRES_HOST']
    POSTGRES_PORT = environ['POSTGRES_PORT']
    POSTGRES_DB = environ['POSTGRES_DB']
    POSTGRES_USER = environ['POSTGRES_USER']
    POSTGRES_PASSWORD = environ['POSTGRES_PASSWORD']

    PROJECT_CACHE_PLUGINS = 'PROJECT_CACHE_PLUGINS'
    PROJECT_CACHE_KEY = 'PROJECT_CACHE_KEY'

    DATABASE_URI = ''
    DATABASE_ENGINE_OPTIONS = {
        'isolation_level': 'READ COMMITTED',
        'echo': False,
        'pool_size': 50,
        'max_overflow': 100,
        'pool_pre_ping': True
    }

    def __init__(self):
        match self.DATABASE_VENDOR:
            case 'sqlite':
                Config.DATABASE_ENGINE_OPTIONS['isolation_level'] = 'SERIALIZABLE'
                Config.DATABASE_URI = 'sqlite:///sqlite.db'
            case _:
                Config.DATABASE_URI = 'postgresql://{username}:{password}@{host}:{port}/{database}'.format(
                    host=self.POSTGRES_HOST,
                    port=self.POSTGRES_PORT,
                    username=self.POSTGRES_USER,
                    password=self.POSTGRES_PASSWORD,
                    database=self.POSTGRES_DB
                )
        log.info('Initializing config %s', self)
