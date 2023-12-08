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

    def list_bucket(self, *args, **kwargs):
        log.info("list_bucket(%s, %s)", args, kwargs)
        return []

    def create_bucket(self, *args, **kwargs):
        log.info("create_bucket(%s, %s)", args, kwargs)

    def upload_file(self, bucket, file_obj, file_name):
        log.info("upload_file(%s, %s)", bucket, file_name)

    def list_files(self, bucket, next_continuation_token=None):
        log.info("list_files(%s, %s)", bucket, next_continuation_token)
        return []


class MockAdminEngine(metaclass=MockMeta):
    """ Client mock / debug class """

    def __init__(self, *args, **kwargs):
        log.info("admin.__init__(%s, %s)", args, kwargs)

    def __getattr__(self, name):
        log.info("admin.__getattr__(%s)", name)

    def __setattr__(self, name, value):
        log.info("admin.__setattr__(%s)", name)

    def __delattr__(self, name):
        log.info("admin.__delattr__(%s)", name)

    def list_bucket(self, *args, **kwargs):
        log.info("admin.list_bucket(%s, %s)", args, kwargs)
        return []

    def create_bucket(self, *args, **kwargs):
        log.info("admin.create_bucket(%s, %s)", args, kwargs)

    def upload_file(self, bucket, file_obj, file_name):
        log.info("admin.upload_file(%s, %s)", bucket, file_name)

    def list_files(self, bucket, next_continuation_token=None):
        log.info("admin.list_files(%s, %s)", bucket, next_continuation_token)
        return []
