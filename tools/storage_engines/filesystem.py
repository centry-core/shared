# pylint: disable=C0116
#
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

""" Secret engine impl """

import os
import sys
import json
import base64
import datetime

from pylon.core.tools import log  # pylint: disable=E0401

from tools import context  # pylint: disable=E0401
from tools import config as c  # pylint: disable=E0401

from ..minio_tools import space_monitor, throughput_monitor  # pylint: disable=E0401


class EngineMeta(type):
    """ Engine meta class """

    def __getattr__(cls, name):
        log.info("StorageEngine.cls.__getattr__(%s)", name)


class EngineBase(metaclass=EngineMeta):
    """ Engine base class """

    def __getattr__(self, name):
        log.info("StorageEngine.base.__getattr__(%s)", name)

    TASKS_BUCKET = "tasks"

    def __init__(self, *args, **kwargs):
        _ = args, kwargs
        #
        self.event_manager = context.event_manager
        #
        self.bucket_path = os.path.join(c.STORAGE_FILESYSTEM_PATH, "bucket")
        self.meta_path = os.path.join(c.STORAGE_FILESYSTEM_PATH, "meta")
        #
        os.makedirs(self.bucket_path, exist_ok=True)
        os.makedirs(self.meta_path, exist_ok=True)

    def extract_access_data(self, integration_id=None, is_local=True):
        raise RuntimeError("Not supported")

    @property
    def bucket_prefix(self):
        raise NotImplementedError

    def format_bucket_name(self, bucket):
        if bucket.startswith(self.bucket_prefix):
            return bucket
        #
        return f"{self.bucket_prefix}{bucket}"

    @staticmethod
    def _fs_encode_name(name):
        if c.STORAGE_FILESYSTEM_ENCODER == "base64":
            return base64.urlsafe_b64encode(name.encode()).decode()
        #
        if c.STORAGE_FILESYSTEM_ENCODER == "base32":
            return base64.b32encode(name.encode()).decode()
        #
        return name

    @staticmethod
    def _fs_decode_name(name):
        if c.STORAGE_FILESYSTEM_ENCODER == "base64":
            return base64.urlsafe_b64decode(name.encode()).decode()
        #
        if c.STORAGE_FILESYSTEM_ENCODER == "base32":
            return base64.b32decode(name.encode()).decode()
        #
        return name

    def _save_meta(self, bucket, meta):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.meta_path, self._fs_encode_name(bucket_name))
        #
        with open(path, "w", encoding="utf-8") as file:
            json.dump(meta, file)

    def _load_meta(self, bucket):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.meta_path, self._fs_encode_name(bucket_name))
        #
        if os.path.exists(path):
            with open(path, "rb") as file:
                return json.load(file)
        #
        return {}

    def list_bucket(self):
        result = []
        #
        for item in os.listdir(path=self.bucket_path):
            name = self._fs_decode_name(item)
            #
            if name.startswith(self.bucket_prefix):
                result.append(name.replace(self.bucket_prefix, "", 1))
        #
        return result

    def create_bucket(self, bucket, bucket_type=None, retention_days=None):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.bucket_path, self._fs_encode_name(bucket_name))
        #
        os.makedirs(path, exist_ok=True)
        #
        if bucket_type and bucket_type in ("system", "autogenerated", "local"):
            self.set_bucket_tags(bucket=bucket, tags={"type": bucket_type})
        #
        if retention_days:
            self.configure_bucket_lifecycle(bucket_name, retention_days)
        #
        # No return dict data emulated

    def list_files(self, bucket, next_continuation_token=None):
        _ = next_continuation_token
        #
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.bucket_path, self._fs_encode_name(bucket_name))
        #
        files = []
        #
        with os.scandir(path) as it:
            for entry in it:
                stat = entry.stat()
                #
                files.append({
                    "name": self._fs_decode_name(entry.name),
                    "size": stat.st_size,
                    "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
        #
        return files

    @space_monitor
    def upload_file(self, bucket, file_obj, file_name):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(
            self.bucket_path,
            self._fs_encode_name(bucket_name),
            self._fs_encode_name(file_name)
        )
        #
        with open(path, "wb") as file:
            if isinstance(file_obj, bytes):
                data = file_obj
            elif isinstance(file_obj, str):
                data = file_obj.encode()
            else:
                data = file_obj.read()
            #
            file.write(data)
        #
        throughput_monitor(client=self, file_size=sys.getsizeof(file_obj))
        #
        # No return response data emulated

    def download_file(self, bucket, file_name, project_id=None):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(
            self.bucket_path,
            self._fs_encode_name(bucket_name),
            self._fs_encode_name(file_name)
        )
        #
        file_size = self.get_file_size(bucket, file_name)
        throughput_monitor(client=self, file_size=file_size, project_id=project_id)
        #
        with open(path, "rb") as file:
            return file.read()

    @space_monitor
    def remove_file(self, bucket, file_name):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(
            self.bucket_path,
            self._fs_encode_name(bucket_name),
            self._fs_encode_name(file_name)
        )
        #
        if os.path.exists(path):
            os.remove(path)

    def remove_bucket(self, bucket):
        for file_obj in self.list_files(bucket):
            self.remove_file(bucket, file_obj["name"])
        #
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.bucket_path, self._fs_encode_name(bucket_name))
        meta_path = os.path.join(self.meta_path, self._fs_encode_name(bucket_name))
        #
        if os.path.exists(path):
            os.remove(path)
        #
        if os.path.exists(meta_path):
            os.remove(meta_path)

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
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(self.bucket_path, self._fs_encode_name(bucket_name))
        #
        total_size = 0
        #
        with os.scandir(path) as it:
            for entry in it:
                stat = entry.stat()
                #
                total_size += stat.st_size
        #
        return total_size

    def get_file_size(self, bucket, filename):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(
            self.bucket_path,
            self._fs_encode_name(bucket_name),
            self._fs_encode_name(filename)
        )
        #
        try:
            stat = os.stat(path)
            return stat.st_size
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
        log.info("select_object_content(%s, %s, %s)", bucket, file_name, expression_addon)
        return []

    def is_file_exist(self, bucket, file_name):
        bucket_name = self.format_bucket_name(bucket)
        path = os.path.join(
            self.bucket_path,
            self._fs_encode_name(bucket_name),
            self._fs_encode_name(file_name)
        )
        #
        return os.path.exists(path)


class Engine(EngineBase):
    """ Engine class """

    def __init__(self, project, integration_id=None, is_local=True, **kwargs):
        _ = kwargs
        #
        self.project = project
        self.integration_id = integration_id
        self.is_local = is_local
        #
        super().__init__()

    @property
    def bucket_prefix(self):
        return f"p--{self.project.id}."

    @classmethod
    def from_project_id(
            cls, project_id,
            integration_id=None, is_local=True, rpc_manager=None,
            **kwargs
    ):
        _ = kwargs
        #
        if not rpc_manager:
            rpc_manager = context.rpc_manager
        #
        project = rpc_manager.call.project_get_or_404(project_id=project_id)
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
        super().__init__()

    @property
    def bucket_prefix(self):
        return "p--administration."