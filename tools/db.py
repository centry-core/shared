from contextlib import closing
from flask_sqlalchemy import BaseQuery
from sqlalchemy import create_engine, MetaData
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

from tools import config as c

engine = create_engine(c.DATABASE_URI, **c.DATABASE_ENGINE_OPTIONS)
with engine.connect() as conn:
    conn.execute(CreateSchema(c.POSTGRES_SCHEMA, if_not_exists=True))
    conn.commit()


def get_schema_translate_map(project_id: int | None) -> dict | None:
    if project_id:
        from tools import project_constants as pc
        template = pc['PROJECT_SCHEMA_TEMPLATE']
        return {
            c.POSTGRES_TENANT_SCHEMA: template.format(project_id),
            # c.POSTGRES_SCHEMA: c.POSTGRES_SCHEMA
        }
    return None


def get_project_schema_sessionmaker(project_id: int | None):
    schema_translate_map = get_schema_translate_map(project_id)
    connectable = engine.execution_options(schema_translate_map=schema_translate_map)
    return sessionmaker(
        bind=connectable,
        expire_on_commit=False,
    )


def get_project_schema_session(project_id: int | None):
    project_sessionmaker = get_project_schema_sessionmaker(project_id)
    return project_sessionmaker()


# class SessionMeta(DeclarativeMeta):
#     def __init__(cls, name, bases, attrs):
#         super().__init__(name, bases, attrs)
#         cls.session = get_project_schema_session(None)
#         cls.query = cls.session.query_property(query_cls=BaseQuery)


session = scoped_session(get_project_schema_sessionmaker(None))
Base = declarative_base()
Base.query = session.query_property(query_cls=BaseQuery)


def get_all_metadata():
    meta = MetaData()
    for table in Base.metadata.tables.values():
        table.tometadata(meta)
    return meta


def get_shared_metadata():
    meta = MetaData()
    for table in Base.metadata.tables.values():
        if table.schema != c.POSTGRES_TENANT_SCHEMA:
            table.tometadata(meta)
    return meta


def get_tenant_specific_metadata():
    meta = MetaData(schema=c.POSTGRES_TENANT_SCHEMA)
    for table in Base.metadata.tables.values():
        if table.schema == c.POSTGRES_TENANT_SCHEMA:
            table.tometadata(meta)
    return meta


def get_session(project_id: int | None = None):
    return closing(get_project_schema_session(project_id))

with_project_schema_session = get_session
