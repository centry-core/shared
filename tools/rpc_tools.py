#!/usr/bin/python3
# coding=utf-8

#   Copyright 2022 getcarrier.io
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

""" RPC tools """

import functools
import traceback
from flask import current_app
from pylon.core.tools.rpc import RpcManager


def wrap_exceptions(target_exception):
    """ Wrap exceptions into generic exception (for RPC transport) """
    #
    def _decorator(func):
        _target_exception = target_exception
        #
        @functools.wraps(func)
        def _decorated(*_args, **_kvargs):
            try:
                return func(*_args, **_kvargs)
            except BaseException as exception_data:
                if isinstance(exception_data, _target_exception):
                    raise exception_data
                raise _target_exception(traceback.format_exc())
        #
        return _decorated
    #
    return _decorator


class RpcMixin:
    _rpc = None

    @classmethod
    def set_rpc_manager(cls, rpc_manager: RpcManager):
        cls._rpc = rpc_manager

    @property
    def rpc(self):
        if not self._rpc:
            self.set_rpc_manager(current_app.config['CONTEXT'].rpc_manager)
        return self._rpc

    @rpc.setter
    def rpc(self, rpc_manager: RpcManager):
        self.set_rpc_manager(rpc_manager)
