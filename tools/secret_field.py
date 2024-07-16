import json

from uuid import uuid4

from typing import Any, Optional, Generator, Dict

import re

from pydantic import SecretField, BaseModel, SecretStr
from pydantic.validators import str_validator


try:
    from ..tools.vault_tools import VaultClient
except ImportError:
    secrets = {
        'auth_token': 'secret-password123!#',
        'other_secret': 'yaayayay'
    }
    class VaultClient:
        def __init__(self, *args, **kwargs):
            ...

        def unsecret(self, value):
            return self.__unsecret_string(value, secrets)

        def __unsecret_string(self, value: str, secrets: dict) -> str:
            from jinja2 import Template
            template = Template(value)
            return template.render(secret=secrets)

        def get_hidden_secrets(self):
            return secrets

        def set_hidden_secrets(self, v):
            global secrets
            secrets = v







# class SecretString(str, SecretField):
class SecretString(SecretStr, str):
    # try __new__ or metaclass
    _secret_pattern = re.compile(r'^{{secret\.([A-Za-z0-9_-]+)}}$')

    # @classmethod
    # def __get_validators__(cls) -> 'Generator':
    #     yield cls.validate

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
        if not self._secret_value and self._is_secret:
            client = VaultClient(project=self._project_id)
            self._secret_value = client.unsecret(self._secret_repr)
        return self._secret_value

    def store_secret(self) -> str:
        if not self._secret_repr:
            self._secret_repr = '{{secret.%s}}' % uuid4()
        client = VaultClient(project=self._project_id)
        hs = client.get_hidden_secrets()
        hs[self._secret_name] = self._secret_value
        client.set_hidden_secrets(hs)
        self._is_secret = True
        return self._secret_repr

    def set_value(self, value: str) -> None:
        self._secret_value = value

    def unsecret(self, project_id: Optional[int] = None) -> str:
        self._project_id = project_id
        return self.get_secret_value()

psw1 = 'secret-password123!#'
psw2 = '{{secret.auth_token}}'
psw3 = {'from_secrets': True, 'value': psw2}
psw4 = {'from_secrets': False, 'value': psw1}

p1 = SecretString(psw1)
p2 = SecretString(psw2)
p3 = SecretString(psw3)
p4 = SecretString(psw4)


print(p1.get_secret_value())
print(p2.get_secret_value())
print(p3.get_secret_value())
print(p4.get_secret_value())

# print(p1.__dict__)
# print(p2.__dict__)
# print(p3.__dict__)
# print(p4.__dict__)
#
# print(p1.get_secret_value())
# print(p2.get_secret_value())
# print(p3.get_secret_value())
# print(p4.get_secret_value())
#
# print(p1.__dict__)
# print(p2.__dict__)
# print(p3.__dict__)
# print(p4.__dict__)
#
# p1.set_value('new_valuie')
# print(p1)
# print(p1._secret_name)
# p1.store_secret()
# print(p1)
# print(secrets)
# print(p1._secret_name)
# print(SecretString.__get_validators__())

class Qqq(BaseModel):
    username: str
    psw: SecretString


q = Qqq(username='qwe', psw=psw2)
# print('value', q.psw.get_secret_value())
# print(q)
# print(q.json())
# print(q.dict())
print(json.dumps(q.dict()))
