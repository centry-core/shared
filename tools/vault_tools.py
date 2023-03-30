#!/usr/bin/python
# coding=utf-8

#     Copyright 2020 getcarrier.io
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

""" Vault tools """
from typing import Optional, Any, Union

import hvac  # pylint: disable=E0401
import requests
from requests.exceptions import ConnectionError

from pydantic import BaseModel

from pylon.core.tools import log
from jinja2 import Template

from . import constants as c
from ..models.vault import Vault


class VaultAuth(BaseModel):
    role_id: str
    secret_id: str

    class Config:
        fields = {
            'role_id': 'vault_auth_role_id',
            'secret_id': 'vault_auth_secret_id'
        }


class VaultClient:
    @classmethod
    def from_project(cls, project: Union[int, dict, Any]):
        assert project is not None
        auth = None
        vault_name = None
        if isinstance(project, int):
            from .rpc_tools import RpcMixin
            project = RpcMixin().rpc.call.project_get_or_404(project_id=project)
            auth = project.secrets_json
            vault_name = project.id
        elif isinstance(project, dict):
            auth = project
            vault_name = project['id']
        elif project is not None:
            auth = project.secrets_json
            vault_name = project.id
        return cls(vault_auth=auth, vault_name=vault_name)

    def __init__(self,
                 vault_auth: Optional[dict] = None,
                 vault_name: Union[int, str, None] = c.VAULT_ADMINISTRATION_NAME):
        self.auth = None
        if vault_auth:
            self.auth = VaultAuth.parse_obj(vault_auth)
        self.vault_name = vault_name
        self._client = None
        self._cache = {
            'secrets': None,
            'hidden_secrets': None,
            'all_secrets': None
        }

    @property
    def is_administration(self):
        return self.vault_name == c.VAULT_ADMINISTRATION_NAME

    @property
    def client(self):
        if not self._client:
            """ Get "root" Vault client instance """
            # Get Vault client
            client = hvac.Client(url=c.VAULT_URL)
            VaultClient._unseal(client)
            # Get root token from DB
            vault = Vault.query.get(c.VAULT_DB_PK)
            # Add auth info to client
            client.token = vault.unseal_json["root_token"]

            if self.auth:
                # Auth to Vault
                client.auth_approle(
                    self.auth.role_id,
                    self.auth.secret_id,
                    mount_point="carrier-approle",
                )
            self._client = client
        return self._client

    @staticmethod
    def init_vault():
        """ Initialize Vault """
        # Get Vault client
        try:
            client = hvac.Client(url=c.VAULT_URL)
            # Initialize it if needed
            if not client.sys.is_initialized():
                vault = Vault.query.get(c.VAULT_DB_PK)
                # Remove stale DB keys
                if vault is not None:
                    Vault.apply_full_delete_by_pk(pk=c.VAULT_DB_PK)
                # Initialize Vault
                vault_data = client.sys.initialize()
                # Save keys to DB
                vault = Vault(id=c.VAULT_DB_PK, unseal_json=vault_data)
                vault.insert()
            # Unseal if needed
            VaultClient._unseal(client)
            # Enable AppRole auth method if needed
            client = VaultClient().client
            auth_methods = client.sys.list_auth_methods()
            if "carrier-approle/" not in auth_methods["data"].keys():
                client.sys.enable_auth_method(
                    method_type="approle",
                    path="carrier-approle",
                )
        except ConnectionError:
            return 0

    @staticmethod
    def _unseal(client: hvac.Client):
        if client.sys.is_sealed():
            try:
                vault = Vault.query.get(c.VAULT_DB_PK)
                client.sys.submit_unseal_keys(keys=vault.unseal_json["keys"])
            except AttributeError:
                VaultClient.init_vault()

    def __add_hidden_kv(self) -> None:
        # Create hidden secrets KV
        try:
            self.client.sys.enable_secrets_engine(
                backend_type="kv",
                path=f"kv-for-hidden-{self.vault_name}",
                options={"version": "2"},
            )
            self.client.secrets.kv.v2.create_or_update_secret(
                path="project-secrets",
                mount_point=f"kv-for-hidden-{self.vault_name}",
                secret=dict(),
            )
        except hvac.exceptions.InvalidRequest:
            pass

    def __set_hidden_kv_permissions(self) -> None:
        policy = """
            # Login with AppRole
            path "auth/approle/login" {
              capabilities = [ "create", "read" ]
            }
            # Read/write secrets
            path "kv-for-{vault_name}/*" {
              capabilities = ["create", "read", "update", "delete", "list"]
            }
            # Read/write hidden secrets
            path "kv-for-hidden-{vault_name}/*" {
              capabilities = ["create", "read", "update", "delete", "list"]
            }
        """.replace("{vault_name}", str(self.vault_name))

        self.client.sys.create_or_update_policy(
            name=f"policy-for-{self.vault_name}",
            policy=policy
        )

    @property
    def vault_base_url(self) -> str:
        return f'{c.VAULT_URL}/v1/auth/carrier-approle/role'

    def init_project_space(self) -> dict:
        """ Create project approle, policy and KV """
        log.info('Initializing Vault space for [%s]', self.vault_name)
        # Create policy for project
        self.__set_hidden_kv_permissions()
        # Create secrets KV
        self.client.sys.enable_secrets_engine(
            backend_type="kv",
            path=f"kv-for-{self.vault_name}",
            options={"version": "2"},
        )
        self.client.secrets.kv.v2.create_or_update_secret(
            path="project-secrets",
            mount_point=f"kv-for-{self.vault_name}",
            secret=dict(),
        )
        # Create hidden secrets KV
        self.__add_hidden_kv()
        # Create AppRole
        approle_name = f"role-for-{self.vault_name}"
        requests.post(
            f"{self.vault_base_url}/{approle_name}",
            headers={"X-Vault-Token": self.client.token},
            json={"policies": [f"policy-for-{self.vault_name}"]}
        )
        approle_role_id = requests.get(
            f"{self.vault_base_url}/{approle_name}/role-id",
            headers={"X-Vault-Token": self.client.token},
        ).json()["data"]["role_id"]
        approle_secret_id = requests.post(
            f"{self.vault_base_url}/{approle_name}/secret-id",
            headers={"X-Vault-Token": self.client.token},
        ).json()["data"]["secret_id"]
        # Done
        return {
            "auth_role_id": approle_role_id,
            "auth_secret_id": approle_secret_id
        }

    def remove_project_space(self) -> None:
        """ Remove project-specific data from Vault """
        # Remove AppRole
        requests.delete(
            f"{self.vault_base_url}/role-for-{self.vault_name}",
            headers={"X-Vault-Token": self.client.token},
        )
        # Remove secrets KV
        self.client.sys.disable_secrets_engine(
            path=f"kv-for-{self.vault_name}",
        )
        # Remove hidden secrets KV
        self.client.sys.disable_secrets_engine(
            path=f"kv-for-hidden-{self.vault_name}",
        )
        # Remove policy
        self.client.sys.delete_policy(
            name=f"policy-for-{self.vault_name}",
        )

    def set_project_secrets(self, secrets: dict) -> None:
        """ Set project secrets """
        self.client.secrets.kv.v2.create_or_update_secret(
            path="project-secrets",
            mount_point=f"kv-for-{self.vault_name}",
            secret=secrets,
        )
        self._cache['secrets'] = secrets

    def set_project_hidden_secrets(self, secrets: dict) -> None:
        """ Set project hidden secrets """
        if self.is_administration:
            self.set_project_secrets(secrets)
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path="project-secrets",
                mount_point=f"kv-for-hidden-{self.vault_name}",
                secret=secrets,
            )
            self._cache['hidden_secrets'] = secrets
        except (hvac.exceptions.Forbidden, hvac.exceptions.InvalidPath):
            log.error("Exception Forbidden in set_project_hidden_secret")
            self.__set_hidden_kv_permissions()
            self.set_project_secrets(secrets)

    def _get_vault_data(self, mount_point: str) -> dict:
        return self.client.secrets.kv.v2.read_secret_version(
            path="project-secrets",
            mount_point=mount_point,
        ).get("data", {}).get("data", {})

    def get_project_secrets(self) -> dict:
        """ Get project secrets """
        if not self._cache['secrets']:
            self._cache['secrets'] = self._get_vault_data(f"kv-for-{self.vault_name}")
        return self._cache['secrets']

    def get_project_hidden_secrets(self) -> dict:
        """ Get project hidden secrets """
        if self.is_administration:
            return self.get_project_secrets()
        try:
            if not self._cache['hidden_secrets']:
                self._cache['hidden_secrets'] = self._get_vault_data(f"kv-for-hidden-{self.vault_name}")
            return self._cache['hidden_secrets']
        except (hvac.exceptions.Forbidden, hvac.exceptions.InvalidPath):
            log.error("Exception Forbidden in get_project_hidden_secret")
            self.__set_hidden_kv_permissions()
            return {}


    def get_all_secrets(self) -> dict:
        if self.is_administration:
            return self.get_project_secrets()
        if not self._cache['all_secrets']:
            all_secrets = self.__class__().get_all_secrets()
            all_secrets.update(self.get_project_hidden_secrets())
            all_secrets.update(self.get_project_secrets())
            self._cache['all_secrets'] = all_secrets
        return self._cache['all_secrets']

    def _unsecret_list(self, array: list, secrets: dict) -> list:
        for i in range(len(array)):
            array[i] = self.unsecret(i, secrets)
        return array

    def _unsecret_json(self, json: dict, secrets: dict) -> dict:
        for key in json.keys():
            json[key] = self.unsecret(json[key], secrets)
        return json

    def unsecret(self, value: Any, secrets: Optional[dict] = None) -> Any:
        if not secrets:
            secrets = self.get_all_secrets()
            # secrets = self.get_project_secrets()
            # hidden_secrets = self.get_project_hidden_secrets()
            # for key, _value in hidden_secrets.items():
            #     if key not in list(secrets.keys()):
            #         secrets[key] = _value
        if isinstance(value, str):
            template = Template(value)
            return template.render(secret=secrets)
        elif isinstance(value, list):
            return self._unsecret_list(value, secrets)
        elif isinstance(value, dict):
            return self._unsecret_json(value, secrets)
        else:
            return value
