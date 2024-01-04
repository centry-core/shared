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

from tools import context  # pylint: disable=E0401

from . import EngineBase


class Engine(EngineBase):  # pylint: disable=R0902
    """ Engine class """

    storage = {}

    @staticmethod
    def get_project_creds(project):
        secrets_key = "unknown"
        #
        if project is None:
            secrets_key = "admin"
        elif isinstance(project, (int, str)):
            project = context.rpc_manager.call.project_get_or_404(project_id=project)
            secrets_key = f'project-{project.id}'
        elif isinstance(project, dict):
            secrets_key = f'project-{project["id"]}'
        elif project is not None:
            secrets_key = f'project-{project.id}'
        #
        return secrets_key

    def __init__(
            self, project=None,
            fix_project_auth=False, track_used_secrets=False,
            **kwargs
    ):
        super().__init__(project, fix_project_auth, track_used_secrets, **kwargs)
        #
        self.secrets_key = self.get_project_creds(project)

    def _write(self, data):
        Engine.storage[self.secrets_key] = data

    def _read(self):
        return Engine.storage[self.secrets_key]

    def create_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        if self.secrets_key not in Engine.storage:
            self._write({
                "secrets": {},
                "hidden_secrets": {},
            })
        #
        return super().create_project_space(*args, **kwargs)

    def remove_project_space(self, *args, **kwargs):
        _ = args, kwargs
        #
        Engine.storage.pop(self.secrets_key, None)
