# pylint: disable=C0116
#
#   Copyright 2024 getcarrier.io
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

""" Storage engine impl """

import os
import sys
import json
import time
import queue
import base64
import datetime

from libcloud.storage.types import Provider  # pylint: disable=E0401
from libcloud.storage.providers import get_driver  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0401,E0611

from tools import context  # pylint: disable=E0401
from tools import config as c  # pylint: disable=E0401

from .. import db
from ..minio_tools import space_monitor, throughput_monitor  # pylint: disable=E0401
from ...models.storage import StorageMeta


class EngineMeta(type):
    """ Engine meta class """

    def __getattr__(cls, name):
        log.info("StorageEngine.cls.__getattr__(%s)", name)


class EngineBase(metaclass=EngineMeta):
    """ Engine base class """

    def purify_bucket_name(self, name: str) -> str:
        return name[len(self.bucket_prefix):] if name.startswith(self.bucket_prefix) else name

    def __getattr__(self, name):
        log.info("StorageEngine.base.__getattr__(%s)", name)

    TASKS_BUCKET = "tasks"

    def __init__(
            self,
            *args,
            storage_libcloud_driver=None,
            storage_libcloud_params=None,
            storage_libcloud_encoder=None,
            **kwargs,
    ):
        _ = args, kwargs
        #
        self.event_manager = context.event_manager
        self.rpc_manager = context.rpc_manager
        #
        self.storage_libcloud_driver = storage_libcloud_driver \
            if storage_libcloud_driver is not None else \
            c.STORAGE_LIBCLOUD_DRIVER
        #
        self.storage_libcloud_params = storage_libcloud_params \
            if storage_libcloud_params is not None else \
            c.STORAGE_LIBCLOUD_PARAMS
        #
        self.storage_libcloud_encoder = storage_libcloud_encoder \
            if storage_libcloud_encoder is not None else \
            c.STORAGE_LIBCLOUD_ENCODER
        #
        driver_cls = get_driver(getattr(Provider, self.storage_libcloud_driver))
        #
        driver_params = self.storage_libcloud_params
        if not isinstance(driver_params, dict):
            driver_params = json.loads(driver_params)
        #
        driver_args = driver_params.get("args", [])
        driver_kwargs = driver_params.get("kwargs", {})
        #
        if self.storage_libcloud_driver == "LOCAL":
            base = driver_kwargs.get("key")
            os.makedirs(base, exist_ok=True)
        #
        self.driver = driver_cls(*driver_args, **driver_kwargs)

    def extract_access_data(self, integration_id=None, is_local=True):
        try:
            rpc_call = self.rpc_manager.timeout(5)
            #
            if self.project:
                settings = rpc_call.integrations_get_s3_settings(
                    self.project["id"], integration_id, is_local,
                )
            else:
                settings = rpc_call.integrations_get_s3_admin_settings(integration_id)
        #
        except queue.Empty:
            settings = None
        #
        if settings:
            return dict(settings)
        #
        return {}

    @property
    def bucket_prefix(self):
        raise NotImplementedError

    def format_bucket_name(self, bucket):
        if bucket.startswith(self.bucket_prefix):
            return bucket
        #
        return f"{self.bucket_prefix}{bucket}"

    def _fs_encode_name(self, name):
        if self.storage_libcloud_encoder == "base64":
            return base64.urlsafe_b64encode(name.encode()).decode()
        #
        if self.storage_libcloud_encoder == "base32":
            return base64.b32encode(name.encode()).decode()
        #
        if self.storage_libcloud_encoder == "base32domain":
            return base64.b32encode(name.encode()).decode().lower().replace("=", "8")
        #
        return name

    def _fs_decode_name(self, name):
        if self.storage_libcloud_encoder == "base64":
            return base64.urlsafe_b64decode(name.encode()).decode()
        #
        if self.storage_libcloud_encoder == "base32":
            return base64.b32decode(name.encode()).decode()
        #
        if self.storage_libcloud_encoder == "base32domain":
            return base64.b32decode(name.replace("8", "=").upper().encode()).decode()
        #
        return name

    def _save_meta(self, bucket, meta):
        bucket_name = self._fs_encode_name(self.format_bucket_name(bucket))
        meta_obj = StorageMeta.query.get(bucket_name)
        if meta_obj is None:
            meta_obj = StorageMeta(id=bucket_name, data=meta)
            meta_obj.insert()
        else:
            meta_obj.data = meta
            meta_obj.commit()

    def _load_meta(self, bucket):
        bucket_name = self._fs_encode_name(self.format_bucket_name(bucket))
        meta_obj = StorageMeta.query.get(bucket_name)
        if meta_obj is None:
            return {}
        return meta_obj.data

    def list_bucket(self):
        result = []
        #
        for item in self.driver.iterate_containers():
            try:
                name = self._fs_decode_name(item.name)
            except:  # pylint: disable=W0702
                continue
            #
            if name.startswith(self.bucket_prefix):
                result.append(name.replace(self.bucket_prefix, "", 1))
        #
        return result

    def create_bucket(self, bucket, bucket_type=None, retention_days=None) -> dict:
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        #
        try:
            self.driver.get_container(bucket_key)
        except:  # pylint: disable=W0702
            self.driver.create_container(bucket_key)
        #
        if bucket_type and bucket_type in ("system", "autogenerated", "local"):
            self.set_bucket_tags(bucket=bucket, tags={"type": bucket_type})
        #
        if retention_days:
            self.configure_bucket_lifecycle(bucket_name, retention_days)
        #
        return {
            "Location": f"/{bucket_name}"
        }

    def list_files(self, bucket, next_continuation_token=None):
        _ = next_continuation_token
        #
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        #
        files = []
        #
        container = self.driver.get_container(bucket_key)
        for entry in self.driver.iterate_container_objects(container):
            modify_time = entry.extra.get("modify_time", time.time())
            #
            files.append({
                "name": self._fs_decode_name(entry.name),
                "size": entry.size,
                "modified": datetime.datetime.fromtimestamp(modify_time).isoformat(),
            })
        #
        return files

    @space_monitor
    def upload_file(self, bucket, file_obj, file_name):
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        file_key = self._fs_encode_name(file_name)
        #
        # TODO: memory-efficient 'any file-data iterator'
        #
        if isinstance(file_obj, bytes):
            data = file_obj
        elif isinstance(file_obj, str):
            data = file_obj.encode()
        else:
            data = file_obj.read()
        #
        container = self.driver.get_container(bucket_key)
        self.driver.upload_object_via_stream(iter([data]), container, file_key)
        #
        throughput_monitor(client=self, file_size=sys.getsizeof(file_obj))
        #
        # NB: No return response data emulated

    def download_file(self, bucket, file_name, project_id=None):
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        file_key = self._fs_encode_name(file_name)
        #
        obj = self.driver.get_object(bucket_key, file_key)
        file_size = obj.size
        throughput_monitor(client=self, file_size=file_size, project_id=project_id)
        #
        return b"".join(self.driver.download_object_as_stream(obj))

    @space_monitor
    def remove_file(self, bucket, file_name):
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        file_key = self._fs_encode_name(file_name)
        #
        obj = self.driver.get_object(bucket_key, file_key)
        self.driver.delete_object(obj)

    def remove_bucket(self, bucket):
        for file_obj in self.list_files(bucket):
            self.remove_file(bucket, file_obj["name"])
        #
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        #
        with db.with_project_schema_session(None) as session:
            meta_obj = session.query(StorageMeta).where(StorageMeta.id == bucket_key).first()
            if meta_obj is not None:
                session.delete(meta_obj)
        #
        container = self.driver.get_container(bucket_key)
        self.driver.delete_container(container)

    def configure_bucket_lifecycle(self, bucket, days):
        meta = self._load_meta(bucket)
        #
        meta["lifecycle"] = days
        #
        self._save_meta(bucket, meta)

    def get_bucket_lifecycle(self, bucket):
        meta = self._load_meta(bucket)
        #
        if not meta or "lifecycle" not in meta:
            return {}
        #
        return {
            "Rules": [{
                "Expiration": {
                    "Days": meta["lifecycle"],
                },
            }],
        }

    def get_bucket_size(self, bucket):
        total_size = 0
        #
        for file_obj in self.list_files(bucket):
            total_size += file_obj["size"]
        #
        return total_size

    def get_file_size(self, bucket, filename):
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        file_key = self._fs_encode_name(filename)
        #
        try:
            obj = self.driver.get_object(bucket_key, file_key)
            return obj.size
        except:  # pylint: disable=W0702
            return 0

    def get_bucket_tags(self, bucket):
        meta = self._load_meta(bucket)
        #
        if not meta or "tags" not in meta:
            return {}
        #
        result = {
            "TagSet": [],
        }
        #
        for key, value in meta["tags"].items():
            result["TagSet"].append({
                "Key": key,
                "Value": value,
            })
        #
        return result

    def set_bucket_tags(self, bucket, tags):
        meta = self._load_meta(bucket)
        #
        if "tags" not in meta:
            meta["tags"] = {}
        #
        meta["tags"].update(tags)
        #
        self._save_meta(bucket, meta)

    def select_object_content(self, bucket, file_name, expression_addon=""):
        # TODO: implement using py-partiql-parser or similiar
        log.info("select_object_content(%s, %s, %s)", bucket, file_name, expression_addon)
        return []

    def is_file_exist(self, bucket, file_name):
        bucket_name = self.format_bucket_name(bucket)
        bucket_key = self._fs_encode_name(bucket_name)
        file_key = self._fs_encode_name(file_name)
        #
        try:
            self.driver.get_object(bucket_key, file_key)
            return True
        except:  # pylint: disable=W0702
            return False


class Engine(EngineBase):
    """ Engine class """

    def __init__(self, project: dict, integration_id=None, is_local=True, **kwargs):
        _ = kwargs
        #
        if isinstance(project, dict):
            self.project = project
        else:
            self.project = project.to_json()
        #
        self.integration_id = integration_id
        self.is_local = is_local
        #
        self.rpc_manager = context.rpc_manager
        #
        integration_settings = self.extract_access_data(integration_id, is_local)
        #
        if integration_settings:
            storage_libcloud_driver=integration_settings["access_key"]
            storage_libcloud_params=integration_settings["secret_access_key"]
            storage_libcloud_encoder=integration_settings["region_name"]
            #
            if integration_settings["integration_id"] != 1:
                if storage_libcloud_driver == "LOCAL":
                    raise RuntimeError("Non-default LOCAL integrations are not curently supported")
                #
                self.integration_id = integration_settings["integration_id"]
                self.is_local = integration_settings["is_local"]
                #
                super().__init__(
                    storage_libcloud_driver=storage_libcloud_driver,
                    storage_libcloud_params=storage_libcloud_params,
                    storage_libcloud_encoder=storage_libcloud_encoder,
                )
            else:
                super().__init__()
        else:
            super().__init__()

    @property
    def bucket_prefix(self):
        return f"p--{self.project['id']}."

    @classmethod
    def from_project_id(
            cls, project_id: int,
            integration_id=None, is_local=True, rpc_manager=None,
            **kwargs
    ):
        _ = kwargs
        #
        if not rpc_manager:
            rpc_manager = context.rpc_manager
        #
        project = rpc_manager.call.project_get_by_id(project_id=project_id)
        return cls(project, integration_id, is_local)


class AdminEngine(EngineBase):
    """ Engine admin class """

    def __init__(self, integration_id=None, **kwargs):
        _ = kwargs
        #
        self.project = None
        self.integration_id = integration_id
        self.is_local = False
        #
        self.rpc_manager = context.rpc_manager
        #
        integration_settings = self.extract_access_data(integration_id)
        #
        if integration_settings:
            storage_libcloud_driver=integration_settings["access_key"]
            storage_libcloud_params=integration_settings["secret_access_key"]
            storage_libcloud_encoder=integration_settings["region_name"]
            #
            if integration_settings["integration_id"] != 1:
                if storage_libcloud_driver == "LOCAL":
                    raise RuntimeError("Non-default LOCAL integrations are not curently supported")
                #
                self.integration_id = integration_settings["integration_id"]
                self.is_local = integration_settings["is_local"]
                #
                super().__init__(
                    storage_libcloud_driver=storage_libcloud_driver,
                    storage_libcloud_params=storage_libcloud_params,
                    storage_libcloud_encoder=storage_libcloud_encoder,
                )
            else:
                super().__init__()
        else:
            super().__init__()

    @property
    def bucket_prefix(self):
        return "p--administration."


# TODO: engine init() and retention watcher thread
