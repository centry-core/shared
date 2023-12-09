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
import json

from functools import wraps

from pylon.core.tools import log  # pylint: disable=E0401
from pylon.core.tools.context import Context as Holder  # pylint: disable=E0401

from jinja2 import Template, Environment, nodes  # pylint: disable=E0401

from tools import context  # pylint: disable=E0401
from tools import config as c  # pylint: disable=E0401


class MockMeta(type):
    """ Client meta class """

    def __getattr__(cls, name):
        log.info("cls.__getattr__(%s)", name)


class Engine(metaclass=MockMeta):  # pylint: disable=R0902
    """ Client mock / debug class """

    def __getattr__(self, name):
        log.info("__getattr__(%s)", name)

    template_node_name = "secret"

    @staticmethod
    def get_project_creds(project):
        project_config = None  # actually not used, for compat only
        file_name = "unknown.json"
        #
        if isinstance(project, (int, str)):
            project = context.rpc_manager.call.project_get_or_404(project_id=project)
            project_config = project.secrets_json
            file_name = f'project-{project.id}.json'
        elif isinstance(project, dict):
            project_config = project
            file_name = f'project-{project["id"]}.json'
        elif project is not None:
            project_config = project.secrets_json
            file_name = f'project-{project.id}.json'
        #
        return project_config, file_name

    @classmethod
    def from_project(cls, project, **kwargs):
        return cls(project=project, **kwargs)

    def __init__(
            self, project=None,
            fix_project_auth=False, track_used_secrets=False,
            **kwargs
    ):
        _ = fix_project_auth, kwargs
        #
        self.track_used_secrets = track_used_secrets
        self.used_secrets = set()
        #
        if project is None:
            self.storage = os.path.join(c.MOCK_SECRETS_PATH, "admin.json")
        else:
            _, file_name = self.get_project_creds(project)
            self.storage = os.path.join(c.MOCK_SECRETS_PATH, file_name)
        #
        self._cache = {
            "secrets": {},
            "hidden_secrets": {},
            "shared_secrets": {},
        }
        #
        self.set_project_secrets = self.set_secrets
        self.set_project_hidden_secrets = self.set_hidden_secrets
        self.get_project_secrets = self.get_secrets
        self.get_project_hidden_secrets = self.get_hidden_secrets

    def _write(self, data):
        with open(self.storage, "w", encoding="utf-8") as file:
            json.dump(data, file)

    def _read(self):
        with open(self.storage, "rb") as file:
            return json.load(file)

    @property
    def db_data(self):
        raise RuntimeError("Not supported")

    @property
    def client(self):
        raise RuntimeError("Not supported")

    @staticmethod
    def init_vault(*args, **kwargs):
        _ = args, kwargs
        #
        os.makedirs(c.MOCK_SECRETS_PATH, exist_ok=True)

    def with_admin_token(func):  # pylint: disable=E0213
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)  # pylint: disable=E1102
        return wrapper

    def create_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        if not os.path.exists(self.storage):
            self._write({
                "secrets": {},
                "hidden_secrets": {},
            })
        #
        result = Holder()
        result.dict = lambda *args, **kwargs: {}
        return result

    def remove_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        if os.path.exists(self.storage):
            os.remove(self.storage)

    def set_secrets(self, secrets, **kwargs):
        _ = kwargs
        #
        data = self._read()
        data["secrets"] = secrets
        self._write(data)
        #
        self._cache["secrets"] = secrets

    def set_hidden_secrets(self, secrets, **kwargs):
        _ = kwargs
        #
        data = self._read()
        data["hidden_secrets"] = secrets
        self._write(data)
        #
        self._cache["hidden_secrets"] = secrets

    def get_secrets(self, *args, **kwargs):
        _ = args, kwargs
        #
        if not self._cache["secrets"]:
            data = self._read()
            self._cache["secrets"] = data["secrets"]
        #
        return self._cache["secrets"].copy()

    def get_hidden_secrets(self, *args, **kwargs):
        _ = args, kwargs
        #
        if not self._cache["hidden_secrets"]:
            data = self._read()
            self._cache["hidden_secrets"] = data["hidden_secrets"]
        #
        return self._cache["hidden_secrets"].copy()

    def get_all_secrets(self, *args, **kwargs):
        _ = args, kwargs
        #
        if not self._cache["shared_secrets"]:
            self._cache["shared_secrets"] = self.__class__().get_secrets()
        #
        all_secrets = self._cache["shared_secrets"].copy()
        all_secrets.update(self.get_hidden_secrets())
        all_secrets.update(self.get_secrets())
        #
        return all_secrets

    def _unsecret_list(self, array, secrets, **kwargs):
        for i in range(len(array)):  # pylint: disable=C0200
            array[i] = self.unsecret(array[i], secrets, **kwargs)
        #
        return array

    def _unsecret_json(self, json_data, secrets, **kwargs):
        for key in json_data.keys():
            json_data[key] = self.unsecret(json_data[key], secrets, **kwargs)
        #
        return json_data

    def __unsecret_string(self, value, secrets):
        if self.track_used_secrets:
            env = Environment()
            ast = env.parse(value)
            for i in ast.find_all(nodes.Getattr):
                n = i.find(nodes.Name)
                if n.name == self.template_node_name:
                    secret_value = secrets.get(i.attr)
                    if secret_value:
                        self.used_secrets.add(secret_value)
        #
        template = Template(value)
        return template.render(secret=secrets)

    def unsecret(self, value, secrets=None, **kwargs):
        _ = kwargs
        #
        if not secrets:
            secrets = self.get_all_secrets()
        #
        if isinstance(value, str):
            return self.__unsecret_string(value, secrets)
        #
        if isinstance(value, list):
            return self._unsecret_list(value, secrets)
        #
        if isinstance(value, dict):
            return self._unsecret_json(value, secrets)
        #
        return value
