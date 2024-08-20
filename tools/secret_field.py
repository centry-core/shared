import re
from uuid import uuid4
from typing import Optional

from pydantic import SecretStr
from pydantic.validators import str_validator

from ..tools.vault_tools import VaultClient


class SecretString(SecretStr):
    _secret_pattern = re.compile(r'^{{secret\.([A-Za-z0-9_]+)}}$')

    def __bool__(self):
       return bool(self._secret_value or self._secret_repr)

    def __len__(self) -> int:
        if self._secret_value is None:
            return 0
        return len(self._secret_value)

    @classmethod
    def validate(cls, value: str | dict) -> 'SecretString':
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            value['value'] = str_validator(value['value'])
        else:
            value = str_validator(value)
        return cls(value)

    def __init__(self, value: str | dict, project_id: Optional[int] = None):
        self._project_id = project_id
        self._vault_client = None
        self._secret_repr = None
        self._secret_value = None
        self._is_secret = False

        if isinstance(value, dict):
            # assume that this is old-style secret field
            self._is_secret = value['from_secrets']
            if self._is_secret:
                self._secret_repr = value['value']
            else:
                self._secret_value = value['value']
        else:
            self._is_secret = re.fullmatch(self._secret_pattern, value) is not None
            if self._is_secret:
                self._secret_repr = value
            else:
                self._secret_value = value

        super().__init__(self._secret_value)

    @property
    def _secret_name(self) -> Optional[str]:
        if self._secret_repr:
            try:
                return re.fullmatch(self._secret_pattern, self._secret_repr).group(1)
            except IndexError:
                ...

    def __str__(self) -> str:
        return self._secret_repr if self._is_secret else self._secret_value

    @property
    def vault_client(self):
        if self._vault_client is None:
            self._vault_client = VaultClient(project=self.project_id)
        return self._vault_client

    @vault_client.setter
    def vault_client(self, value: VaultClient):
        self._vault_client = value
        self._project_id = value.project_id

    def get_secret_value(self) -> str:
        if self._is_secret:
            self._secret_value = self.vault_client.unsecret(self._secret_repr)
        return self._secret_value

    def store_secret(self, force_set_secret: bool = False) -> str:
        if not self._secret_repr:
            self._secret_repr = '{{secret.%s}}' % str(uuid4()).replace("-", "")
        if self._secret_value is None and not force_set_secret:
            raise ValueError(
                'Secret value was not set. If you are still sure to change secret, pass force_set_secret=True'
            )

        # from pylon.core.tools import log
        # hs = self.vault_client.get_secrets()
        # hs[self._secret_name] = self._secret_value
        # self.vault_client.set_secrets(hs)

        hs = self.vault_client.get_hidden_secrets()
        hs[self._secret_name] = self._secret_value
        self.vault_client.set_hidden_secrets(hs)

        self._is_secret = True
        return self._secret_repr

    def set_value(self, value: str) -> None:
        self._secret_value = value

    def unsecret(self, project_id: Optional[int] = None) -> str:
        self._project_id = project_id
        return self.get_secret_value()

    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    def project_id(self, value):
        if self._project_id != value:
            self._vault_client = None
        self._project_id = value


def store_secrets(model_dict: dict, project_id: int) -> None:
    vault_client = None
    for field_name, field_value in model_dict.items():
        if isinstance(field_value, SecretString):
            if not field_value._is_secret:
                if vault_client is None:
                    vault_client = VaultClient(project_id)
                field_value.vault_client = vault_client
                field_value.store_secret()
        elif isinstance(field_value, dict):
            store_secrets(field_value, project_id)
