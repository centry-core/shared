from abc import abstractmethod, ABC
from json import loads
from queue import Empty
from typing import Optional
import sys
import importlib

import boto3
from botocore.client import Config, ClientError
from pylon.core.tools import log

from .rpc_tools import RpcMixin, EventManagerMixin

from tools import config as c
from .minio_tools import space_monitor, throughput_monitor


class MinioClientABC(ABC, EventManagerMixin):
    PROJECT_SECRET_KEY: str = "minio_aws_access"
    TASKS_BUCKET: str = "tasks"
    project: dict | None = None

    def __init__(self,
                 aws_access_key_id: str = c.MINIO_ACCESS,
                 aws_secret_access_key: str = c.MINIO_SECRET,
                 region_name: str = c.MINIO_REGION,
                 endpoint_url: str = c.MINIO_URL,
                 **kwargs
                 ):
        self.s3_client = boto3.client(
            "s3", endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(signature_version="s3v4"),
            region_name=region_name
        )
        # self.event_manager = EventManagerMixin().event_manager

    def extract_access_data(self, integration_id: Optional[int] = None, is_local: bool = True) -> tuple:
        rpc_manager = RpcMixin().rpc
        try:
            if self.project:
                settings = rpc_manager.timeout(2).integrations_get_s3_settings(
                    self.project['id'], integration_id, is_local)
            else:
                settings = rpc_manager.timeout(2).integrations_get_s3_admin_settings(
                    integration_id)
        except Empty:
            settings = None
        if settings:
            self.integration_id = settings['integration_id']
            self.is_local = settings['is_local']
            return (
                settings['access_key'],
                settings['secret_access_key'],
                settings['region_name'],
                settings['storage_url'] if settings['use_compatible_storage'] else None
            )
        return c.MINIO_ACCESS, c.MINIO_SECRET, c.MINIO_REGION, c.MINIO_URL

    @property
    @abstractmethod
    def bucket_prefix(self) -> str:
        raise NotImplementedError

    def format_bucket_name(self, bucket: str) -> str:
        if bucket.startswith(self.bucket_prefix):
            return bucket
        return f"{self.bucket_prefix}{bucket}"

    def list_bucket(self) -> list:
        return [
            each["Name"].replace(self.bucket_prefix, "", 1)
            for each in self.s3_client.list_buckets().get("Buckets", {})
            if each["Name"].startswith(self.bucket_prefix)
        ]

    def create_bucket(self, bucket: str, bucket_type=None, retention_days: Optional[int] = None) -> Optional[dict]:
        response = None
        try:
            bucket_name = self.format_bucket_name(bucket)
            response = self.s3_client.create_bucket(
                ACL="public-read",
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": c.MINIO_REGION}
            )
            if bucket_type and bucket_type in ('system', 'autogenerated', 'local'):
                self.set_bucket_tags(bucket=bucket, tags={'type': bucket_type})
            if retention_days:
                self.configure_bucket_lifecycle(bucket_name, retention_days)
        except ClientError as client_error:
            response = str(client_error)
            log.warning(client_error)
        except Exception as exc:
            response = str(exc)
            log.error(exc)

        return response

    def list_files(self, bucket: str, next_continuation_token: Optional[str] = None) -> list:
        if next_continuation_token:
            response = self.s3_client.list_objects_v2(
                Bucket=self.format_bucket_name(bucket),
                ContinuationToken=next_continuation_token
            )
        else:
            response = self.s3_client.list_objects_v2(
                Bucket=self.format_bucket_name(bucket)
            )
        files = [
            {
                "name": each["Key"],
                "size": each["Size"],
                "modified": each["LastModified"].isoformat()
            }
            for each in response.get("Contents", [])
        ]
        continuation_token = response.get("NextContinuationToken")
        if continuation_token and response["Contents"]:
            files.extend(self.list_files(bucket, next_continuation_token=continuation_token))
        return files

    @space_monitor
    def upload_file(self, bucket: str, file_obj: bytes, file_name: str):
        response = self.s3_client.put_object(Key=file_name, Bucket=self.format_bucket_name(bucket), Body=file_obj)
        throughput_monitor(client=self, file_size=sys.getsizeof(file_obj))
        # self._space_monitor()
        return response

    def download_file(self, bucket: str, file_name: str, project_id: int = None) -> bytes:
        response = self.s3_client.get_object(Bucket=self.format_bucket_name(bucket), Key=file_name)
        throughput_monitor(client=self, file_size=response['ContentLength'], project_id=project_id)
        return response["Body"].read()

    @space_monitor
    def remove_file(self, bucket: str, file_name: str):
        # self._space_monitor()
        return self.s3_client.delete_object(Bucket=self.format_bucket_name(bucket), Key=file_name)

    def remove_bucket(self, bucket: str):
        # self._space_monitor()
        for file_obj in self.list_files(bucket):
            self.remove_file(bucket, file_obj["name"])

        self.s3_client.delete_bucket(Bucket=self.format_bucket_name(bucket))

    def configure_bucket_lifecycle(self, bucket: str, days: int) -> None:
        self.s3_client.put_bucket_lifecycle_configuration(
            Bucket=self.format_bucket_name(bucket),
            LifecycleConfiguration={
                "Rules": [
                    {
                        "Expiration": {
                            # "NoncurrentVersionExpiration": days,
                            "Days": days
                            # "ExpiredObjectDeleteMarker": True
                        },
                        "NoncurrentVersionExpiration": {
                            'NoncurrentDays': days
                        },
                        "ID": "bucket-retention-policy",
                        'Filter': {'Prefix': ''},
                        "Status": "Enabled"
                    }
                ]
            }
        )

    def get_bucket_lifecycle(self, bucket: str) -> dict:
        return self.s3_client.get_bucket_lifecycle(Bucket=self.format_bucket_name(bucket))

    def get_bucket_size(self, bucket: str) -> int:
        total_size = 0
        for each in self.s3_client.list_objects_v2(
                Bucket=self.format_bucket_name(bucket)
        ).get('Contents', {}):
            total_size += each["Size"]
        return total_size

    def get_file_size(self, bucket: str, filename: str) -> int:
        response = self.s3_client.list_objects_v2(Bucket=self.format_bucket_name(bucket)).get("Contents", {})
        for i in response:
            if str(i["Key"]).lower() == str(filename).lower():
                return i["Size"]
        return 0

    def get_bucket_tags(self, bucket: str) -> dict:
        try:
            return self.s3_client.get_bucket_tagging(Bucket=self.format_bucket_name(bucket))
        except ClientError:
            return {}

    def set_bucket_tags(self, bucket: str, tags: dict) -> None:
        tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        self.s3_client.put_bucket_tagging(
            Bucket=self.format_bucket_name(bucket),
            Tagging={
                'TagSet': tag_set
            },
        )

    def select_object_content(self, bucket: str, file_name: str, expression_addon: str = '') -> list:
        try:
            response = self.s3_client.select_object_content(
                Bucket=self.format_bucket_name(bucket),
                Key=file_name,
                ExpressionType='SQL',
                Expression=f"select * from s3object s{expression_addon}",
                InputSerialization={
                    'CSV': {
                        "FileHeaderInfo": "USE",
                    },
                    'CompressionType': 'GZIP',
                },
                OutputSerialization={'JSON': {}},
            )
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                log.error(f'Cannot find file "{file_name}" in bucket "{bucket}"')
                return []
            else:
                raise
        results = []
        for event in response['Payload']:
            if 'Records' in event:
                payload = event['Records']['Payload'].decode('utf-8')
                for line in payload.split('\n'):
                    try:
                        results.append(loads(line))
                    except Exception:
                        pass
            if 'Stats' in event:
                throughput_monitor(client=self, file_size=event['Stats']['Details']['BytesScanned'])
        return results

    def is_file_exist(self, bucket: str, file_name: str):
        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=file_name,
        )
        for obj in response.get('Contents', []):
            if obj['Key'] == file_name:
                return True
        return False


class S3MinioClientAdmin(MinioClientABC):
    def __init__(self,
                 integration_id: Optional[int] = None,
                 **kwargs):
        self.project = None
        self.integration_id = integration_id
        self.is_local = False
        access_key, secret_access_key, region_name, url = self.extract_access_data(integration_id)
        super().__init__(access_key, secret_access_key, region_name, url)

    @property
    def bucket_prefix(self) -> str:
        return 'p--administration.'


class S3MinioClient(MinioClientABC):
    @classmethod
    def from_project_id(cls, project_id: int,
                        integration_id: Optional[int] = None,
                        is_local: bool = True,
                        rpc_manager=None,
                        **kwargs):
        if not rpc_manager:
            rpc_manager = RpcMixin().rpc
        project = rpc_manager.call.project_get_by_id(project_id=project_id)
        return cls(project, integration_id, is_local)

    def __init__(self, project: dict,
                 integration_id: Optional[int] = None,
                 is_local: bool = True,
                 **kwargs):
        if isinstance(project, dict):
            self.project = project
        else:
            self.project = project.to_json()
        self.integration_id = integration_id
        self.is_local = is_local
        access_key, secret_access_key, region_name, url = self.extract_access_data(integration_id,
                                                                                   is_local)
        super().__init__(
            access_key, secret_access_key, region_name, url,
            integration_id=integration_id,
            is_local=is_local
        )

    @property
    def bucket_prefix(self) -> str:
        return f'p--{self.project["id"]}.'

#
# Select active (compat) client for storage
#

log.info("Using storage engine: %s", c.STORAGE_ENGINE)
#
if c.STORAGE_ENGINE == "s3":
    MinioClient = S3MinioClient
    MinioClientAdmin = S3MinioClientAdmin
else:
    try:
        engine_pkg = importlib.import_module(
            f"plugins.shared.tools.storage_engines.{c.STORAGE_ENGINE}"
        )
        MinioClient = engine_pkg.Engine
        MinioClientAdmin = engine_pkg.AdminEngine
    except:  # pylint: disable=W0702
        log.exception("Failed to set storage engine: %s", c.STORAGE_ENGINE)
        raise
