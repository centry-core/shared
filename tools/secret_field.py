import re
from uuid import uuid4
from typing import Any, Optional, Dict

from pydantic import SecretStr
from pydantic.validators import str_validator

from ..tools.vault_tools import VaultClient


class SecretString(SecretStr):
    _secret_pattern = re.compile(r'^{{secret\.([A-Za-z0-9_]+)}}$')

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        from pydantic.utils import update_not_none
        update_not_none(
            field_schema,
            type='string',
            writeOnly=True,
            format='password',
            minLength=cls.min_length,
            maxLength=cls.max_length,
        )

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

    def get_secret_value(self) -> str:
        if self._is_secret:
            client = VaultClient(project=self._project_id)
            self._secret_value = client.unsecret(self._secret_repr)
        return self._secret_value

    def store_secret(self) -> str:
        if not self._secret_repr:
            self._secret_repr = '{{secret.%s}}' % str(uuid4()).replace("-", "")
        client = VaultClient(self._project_id)

        hs = client.get_hidden_secrets()
        hs[self._secret_name] = self._secret_value
        client.set_hidden_secrets(hs)

        # hs = client.get_secrets()
        # hs[self._secret_name] = self._secret_value
        # client.set_secrets(hs)

        self._is_secret = True
        return self._secret_repr

    def set_value(self, value: str) -> None:
        self._secret_value = value

    def unsecret(self, project_id: Optional[int] = None) -> str:
        self._project_id = project_id
        return self.get_secret_value()


def store_secrets(model_dict: dict, project_id: int):
    for field_name, field_value in model_dict.items():
        if isinstance(field_value, SecretStr):
            field_value._project_id = project_id
            field_value.store_secret()
        elif isinstance(field_value, dict):
            store_secrets(field_value, project_id)
