from flask_sqlalchemy import BaseQuery
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from tools import config


url = config.DATABASE_URI
options = config.db_engine_config
engine = create_engine(url, **options)
session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = session.query_property(query_cls=BaseQuery)
