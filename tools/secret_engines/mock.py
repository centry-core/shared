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

from pylon.core.tools import log


class MockEngine:
    """ Client mock / debug class """

    def __init__(self):
        log.info("__init__()")

    def __get__(self, instance, owner=None):
        log.info("__get__(%s)", instance)

    def __set__(self, instance, value):
        log.info("__set__(%s)", instance)

    def __delete__(self, instance):
        log.info("__delete__(%s)", instance)

    def __getattr__(self, name):
        log.info("__getattr__(%s)", name)

    def __setattr__(self, name, value):
        log.info("__setattr__(%s)", name)

    def __delattr__(self, name):
        log.info("__delattr__(%s)", name)
