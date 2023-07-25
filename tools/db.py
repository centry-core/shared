from contextlib import contextmanager
from flask_sqlalchemy import BaseQuery
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from tools import config as c


engine = create_engine(c.DATABASE_URI, **c.DATABASE_ENGINE_OPTIONS)
session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = session.query_property(query_cls=BaseQuery)


def get_shared_metadata():
    meta = MetaData()
    for table in Base.metadata.tables.values():
        if table.schema != "tenant":
            table.tometadata(meta)
    return meta


def get_tenant_specific_metadata():
    meta = MetaData(schema="tenant")
    for table in Base.metadata.tables.values():
        if table.schema == "tenant":
            table.tometadata(meta)
    return meta


def get_project_schema_session(project_id: int | None):
    if project_id:
        from tools import project_constants as pc
        template = pc['PROJECT_SCHEMA_TEMPLATE']
        schema_translate_map = dict(tenant=template.format(project_id))
    else:
        schema_translate_map = None

    connectable = engine.execution_options(schema_translate_map=schema_translate_map)
    return scoped_session(sessionmaker(bind=connectable))


@contextmanager
def with_project_schema_session(project_id: int | None):
    if project_id:
        from tools import project_constants as pc
        template = pc['PROJECT_SCHEMA_TEMPLATE']
        schema_translate_map = dict(tenant=template.format(project_id))
    else:
        schema_translate_map = None

    connectable = engine.execution_options(schema_translate_map=schema_translate_map)
    db = None
    try:
        db = scoped_session(sessionmaker(bind=connectable))
        yield db
    finally:
        if db:
            db.close()
