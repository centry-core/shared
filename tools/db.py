from contextlib import contextmanager
from flask_sqlalchemy import BaseQuery
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base

from tools import config as c

engine = create_engine(c.DATABASE_URI, **c.DATABASE_ENGINE_OPTIONS)
if not engine.dialect.has_schema(engine, c.POSTGRES_SCHEMA):  # TODO: allow to use other DBs, not only postgres
    engine.execute(sqlalchemy.schema.CreateSchema(c.POSTGRES_SCHEMA))
#
session = scoped_session(sessionmaker(bind=engine))
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


def get_schema_translate_map(project_id: int | None) -> dict | None:
    if project_id:
        from tools import project_constants as pc
        template = pc['PROJECT_SCHEMA_TEMPLATE']
        return {
            c.POSTGRES_TENANT_SCHEMA: template.format(project_id),
            # c.POSTGRES_SCHEMA: c.POSTGRES_SCHEMA
        }
    return None


def get_project_schema_session(project_id: int | None):
    schema_translate_map = get_schema_translate_map(project_id)
    connectable = engine.execution_options(schema_translate_map=schema_translate_map)
    return scoped_session(sessionmaker(bind=connectable))


@contextmanager
def with_project_schema_session(project_id: int | None):
    # schema_translate_map = get_schema_translate_map(project_id)
    # connectable = engine.execution_options(schema_translate_map=schema_translate_map)
    db = None
    try:
        db = get_project_schema_session(project_id)
        # db = scoped_session(sessionmaker(bind=connectable))
        yield db
    finally:
        if db:
            db.close()
