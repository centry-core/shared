#   Copyright 2021 getcarrier.io
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

""" Module """
from datetime import datetime

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import module  # pylint: disable=E0611,E0401

from .config import Config
from .connectors.vault import init_vault
from .db_manager import db_session
from .init_db import init_db


class Module(module.ModuleModel):
    """ Galloper module """

    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor

    def init(self):
        """ Init module """
        log.info("Initializing module Shared")

        from .tools import rpc_tools, db_tools, db_migrations
        self.descriptor.register_tool('rpc_tools', rpc_tools)
        self.descriptor.register_tool('db_tools', db_tools)
        self.descriptor.register_tool('db_migrations', db_migrations)






        self.context.app.config.from_object(Config())
        init_db()
        init_vault()  # won't do anything if vault is not available

        self.init_filters()

        self.descriptor.register_tool('shared', self)

        @self.context.app.teardown_appcontext
        def shutdown_session(exception=None):
            db_session.remove()



    def deinit(self):  # pylint: disable=R0201
        """ De-init module """
        log.info("De-initializing module Shared")

    def init_filters(self):
        from .filters import tag_format, extract_tags, list_pd_to_json, convert_time as ctime, return_zero as is_zero
        # Register custom Jinja filters
        self.context.app.template_filter()(tag_format)
        self.context.app.template_filter()(extract_tags)
        self.context.app.template_filter()(list_pd_to_json)
        self.context.app.template_filter()(ctime)
        self.context.app.template_filter()(is_zero)
