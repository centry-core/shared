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

import pymongo  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from pylon.core.tools import module  # pylint: disable=E0611,E0401
from pylon.core.tools.context import Context as Holder  # pylint: disable=E0611,E0401
from .tools.jinja_filters import humanize_timestamp, format_datetime

# from .db_manager import db_session


class Module(module.ModuleModel):
    """ Galloper module """

    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor
        self.db = None
        self.mongo = None
        self.job_type_rpcs = set()

    def init(self):
        """ Init module """
        log.info("Initializing module Shared")

        from .tools.rpc_tools import RpcMixin, EventManagerMixin
        RpcMixin.set_rpc_manager(self.context.rpc_manager)
        EventManagerMixin.set_manager(self.context.event_manager)

        from .tools import constants
        self.descriptor.register_tool('constants', constants)

        from .tools.config import Config
        self.descriptor.register_tool('config', Config())

        from .tools import rpc_tools, api_tools
        self.descriptor.register_tool('rpc_tools', rpc_tools)
        self.descriptor.register_tool('api_tools', api_tools)

        from .tools import db
        self.db = db
        self.descriptor.register_tool('db', db)

        from .tools import db_tools, db_migrations
        self.descriptor.register_tool('db_tools', db_tools)
        self.descriptor.register_tool('db_migrations', db_migrations)

        # self.context.app.config.from_object(self.config)
        from .init_db import init_db
        init_db()

        self.mongo = Holder()
        self.mongo.url = self.descriptor.config.get("mongo_connection", None)
        self.mongo.options = self.descriptor.config.get("mongo_options", dict())
        self.mongo.db_name = self.descriptor.config.get("mongo_db", None)
        self.mongo.client = pymongo.MongoClient(
            self.mongo.url, **self.mongo.options
        )
        self.mongo.db = self.mongo.client[self.mongo.db_name]
        self.descriptor.register_tool("mongo", self.mongo)

        from .tools.minio_client import MinioClient
        self.descriptor.register_tool('MinioClient', MinioClient)

        from .tools import vault_tools
        self.descriptor.register_tool('vault_tools', vault_tools)
        vault_tools.init_vault()  # won't do anything if vault is not available

        from .tools import data_tools
        self.descriptor.register_tool('data_tools', data_tools)

        self.init_filters()

        self.descriptor.register_tool('shared', self)

        self.descriptor.init_api()

        self.context.app.jinja_env.filters['humanize_timestamp'] = humanize_timestamp
        self.context.app.jinja_env.filters['format_datetime'] = format_datetime


    #     self.context.app.teardown_appcontext(self.shutdown_session)
    #
    # def shutdown_session(self, exception=None):
    #     self.db.session.remove()

    def deinit(self):  # pylint: disable=R0201
        """ De-init module """
        log.info("De-initializing module Shared")

    def init_filters(self):
        from .filters import tag_format, extract_tags, list_pd_to_json, ctime, is_zero
        # Register custom Jinja filters
        self.context.app.template_filter()(tag_format)
        self.context.app.template_filter()(extract_tags)
        self.context.app.template_filter()(list_pd_to_json)
        self.context.app.template_filter()(ctime)
        self.context.app.template_filter()(is_zero)
