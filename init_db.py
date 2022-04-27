# from ..shared.db_manager import Base, engine
from tools import db


def init_db():
    from .models import vault
    db.Base.metadata.create_all(bind=db.engine)

