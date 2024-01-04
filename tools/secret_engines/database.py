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

import json

from cryptography.fernet import Fernet  # pylint: disable=E0401

from tools import context  # pylint: disable=E0401
from tools import config as c  # pylint: disable=E0401

from . import EngineBase
from ...models.secrets import SecretsKey, SecretsData


class Engine(EngineBase):  # pylint: disable=R0902
    """ Engine class """
    # TODO: use project and system schemas here too

    @staticmethod
    def get_project_creds(project):
        db_key = "unknown"
        #
        if project is None:
            db_key = "admin"
        elif isinstance(project, (int, str)):
            project = context.rpc_manager.call.project_get_or_404(project_id=project)
            db_key = f'project-{project.id}'
        elif isinstance(project, dict):
            db_key = f'project-{project["id"]}'
        elif project is not None:
            db_key = f'project-{project.id}'
        #
        return db_key

    def __init__(
            self, project=None,
            fix_project_auth=False, track_used_secrets=False,
            **kwargs
    ):
        super().__init__(project, fix_project_auth, track_used_secrets, **kwargs)
        #
        self.db_key = self.get_project_creds(project)
        #
        self.master_key = None
        if c.SECRETS_MASTER_KEY is not None:
            self.master_key = c.SECRETS_MASTER_KEY.encode()

    def _write_key(self, key):
        if self.master_key is None:
            data = key
        else:
            data = Fernet(self.master_key).encrypt(key)
        #
        key_obj = SecretsKey.query.get(self.db_key)
        if key_obj is None:
            key_obj = SecretsKey(id=self.db_key, data=data)
            key_obj.insert()
        else:
            key_obj.data = data
            key_obj.commit()

    def _read_key(self):
        data = SecretsKey.query.get(self.db_key).data
        #
        if self.master_key is not None:
            return Fernet(self.master_key).decrypt(data)
        #
        return data

    def _write(self, data):
        key = self._read_key()
        data = Fernet(key).encrypt(json.dumps(data).encode())
        #
        data_obj = SecretsData.query.get(self.db_key)
        if data_obj is None:
            data_obj = SecretsData(id=self.db_key, data=data)
            data_obj.insert()
        else:
            data_obj.data = data
            data_obj.commit()

    def _read(self):
        key = self._read_key()
        data = SecretsData.query.get(self.db_key).data
        return json.loads(Fernet(key).decrypt(data).decode())

    def create_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        key_obj = SecretsKey.query.get(self.db_key)
        if key_obj is None:
            key = Fernet.generate_key()
            self._write_key(key)
        #
        data_obj = SecretsData.query.get(self.db_key)
        if data_obj is None:
            self._write({
                "secrets": {},
                "hidden_secrets": {},
            })
        #
        return super().create_project_space(*args, **kwargs)

    def remove_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        key_obj = SecretsKey.query.get(self.db_key)
        if key_obj is not None:
            key_obj.delete()
        #
        data_obj = SecretsData.query.get(self.db_key)
        if data_obj is not None:
            data_obj.delete()
