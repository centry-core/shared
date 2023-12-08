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

from pylon.core.tools import log  # pylint: disable=E0401
from pylon.core.tools.context import Context as Holder  # pylint: disable=E0401


class MockMeta(type):
    """ Client meta class """

    def __getattr__(cls, name):
        log.info("cls.__getattr__(%s)", name)

    def __setattr__(cls, name, value):
        log.info("cls.__setattr__(%s)", name)

    def __delattr__(cls, name):
        log.info("cls.__delattr__(%s)", name)


class MockEngine(metaclass=MockMeta):
    """ Client mock / debug class """

    def __init__(self, *args, **kwargs):
        log.info("__init__(%s, %s)", args, kwargs)

    def __getattr__(self, name):
        log.info("__getattr__(%s)", name)

    def __setattr__(self, name, value):
        log.info("__setattr__(%s)", name)

    def __delattr__(self, name):
        log.info("__delattr__(%s)", name)

    @staticmethod
    def init_vault(*args, **kwargs):
        log.info("init_vault(%s, %s)", args, kwargs)

    def create_project_space(self, *args, **kwargs):
        log.info("create_project_space(%s, %s)", args, kwargs)
        result = Holder()
        result.dict = lambda *args, **kwargs: {}
        return result

    def get_all_secrets(self, *args, **kwargs):
        log.info("get_all_secrets(%s, %s)", args, kwargs)
        return {}

    def set_secrets(self, *args, **kwargs):
        log.info("set_secrets(%s, %s)", args, kwargs)

    def get_secrets(self, *args, **kwargs):
        log.info("get_secrets(%s, %s)", args, kwargs)
        return {}

    def set_hidden_secrets(self, *args, **kwargs):
        log.info("set_hidden_secrets(%s, %s)", args, kwargs)

    def get_hidden_secrets(self, *args, **kwargs):
        log.info("get_hidden_secrets(%s, %s)", args, kwargs)
        return {}

    def unsecret(self, value, secrets=None, *args, **kwargs):
        return value

    @classmethod
    def from_project(cls, project, **kwargs):
        return cls(project=project, **kwargs)
