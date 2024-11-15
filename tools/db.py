import traceback

from contextlib import closing
from flask_sqlalchemy import BaseQuery
from sqlalchemy import create_engine, MetaData
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

from pylon.core.tools import log
from pylon.core.tools import db_support

from tools import config as c, context


# DB: local
def schema_mapper(schema):
    if schema in [..., None, c.POSTGRES_SCHEMA]:
        return ...
    #
    from tools import project_constants as pc  # pylint: disable=E0401,C0415
    return pc["PROJECT_SCHEMA_TEMPLATE"].format(schema)


# DB: local
context.db.schema_mapper = schema_mapper


# DB: transitional config injector
if c.FORCE_INJECT_DB or context.db.url == "sqlite://":
    log.info("Injecting DB config")
    #
    db_support.close_local_session()
    context.db.engine.dispose()
    #
    context.db.config = {
        "engine_url": c.DATABASE_URI,
        "engine_kwargs": c.DATABASE_ENGINE_OPTIONS.copy(),
        # "default_schema": c.POSTGRES_SCHEMA,
        "default_source_schema": c.POSTGRES_TENANT_SCHEMA,
    }
    #
    context.db.url = db_support.get_db_url(context.db)
    context.db.engine = db_support.make_engine(context.db)
    context.db.make_session = db_support.make_session_fn(context.db)
    #
    db_support.create_local_session()


# DB: proxy
engine = context.db.engine


# DB: transitional for 'public' schemas on present deployments
with engine.connect() as connection:
    connection.execute(CreateSchema(c.POSTGRES_SCHEMA, if_not_exists=True))
    connection.commit()


# DB: local
# def get_schema_translate_map(project_id: int | None) -> dict | None:
#     if project_id:
#         from tools import project_constants as pc
#         template = pc['PROJECT_SCHEMA_TEMPLATE']
#         return {
#             c.POSTGRES_TENANT_SCHEMA: template.format(project_id),
#             # c.POSTGRES_SCHEMA: c.POSTGRES_SCHEMA
#         }
#     return None


# DB: local
# def get_project_schema_sessionmaker(project_id: int | None):
#     schema_translate_map = get_schema_translate_map(project_id)
#     connectable = engine.execution_options(schema_translate_map=schema_translate_map)
#     return sessionmaker(
#         bind=connectable,
#         expire_on_commit=False,
#     )


# DB: used
def get_project_schema_session(project_id: int | None):
    return context.db.make_session(project_id)


# DB: obsolete
# class SessionMeta(DeclarativeMeta):
#     def __init__(cls, name, bases, attrs):
#         super().__init__(name, bases, attrs)
#         cls.session = get_project_schema_session(None)
#         cls.query = cls.session.query_property(query_cls=BaseQuery)


# DB: proxy
class SessionProxyMeta(type):
    """ Proxy meta class """

    def __getattr__(cls, name):
        log.info("SessionProxy.cls.__getattr__(%s)", name)


# DB: proxy
class SessionProxy(metaclass=SessionProxyMeta):  # pylint: disable=R0902,R0903
    """ Proxy class """

    def __getattr__(self, name):
        db_support.check_local_entities()
        #
        if context.local.db_session is None:
            log.warning("Creating new local session")
            #
            log.debug("Local session stack:")
            for stack_line in traceback.format_stack():
                log.debug("%s", stack_line.strip())
            #
            db_support.create_local_session()
        #
        return getattr(context.local.db_session, name)


# DB: proxy
session = SessionProxy()


# DB: proxy
class QueryProxyMeta(type):
    """ Proxy meta class """

    def __getattr__(cls, name):
        log.info("QueryProxy.cls.__getattr__(%s)", name)


# DB: proxy
class QueryProxy(metaclass=QueryProxyMeta):  # pylint: disable=R0902
    """ Proxy base """

    def __get__(self, instance, owner):
        return BaseQuery(owner, session=session)


# DB: legacy
Base = declarative_base()
Base.query = QueryProxy()


# DB: used
def get_all_metadata():
    meta = MetaData()
    for table in Base.metadata.tables.values():
        table.tometadata(meta)
    return meta


# DB: used
def get_shared_metadata():
    meta = MetaData()
    for table in Base.metadata.tables.values():
        if table.schema != c.POSTGRES_TENANT_SCHEMA:
            table.tometadata(meta)
    return meta


# DB: used (by flows)
def get_tenant_specific_metadata():
    meta = MetaData(schema=c.POSTGRES_TENANT_SCHEMA)
    for table in Base.metadata.tables.values():
        if table.schema == c.POSTGRES_TENANT_SCHEMA:
            table.tometadata(meta)
    return meta


# DB: used
def get_session(project_id: int | None = None):
    return closing(get_project_schema_session(project_id))


# DB: used
with_project_schema_session = get_session
