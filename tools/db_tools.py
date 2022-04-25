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

""" DB tools """
import json

from tools import config, db


def sqlalchemy_mapping_to_dict(obj):
    """ Make dict from sqlalchemy mappings().one() object """
    return {str(key):value for key, value in dict(obj).items()}


class AbstractBaseMixin:
    __table__ = None
    __table_args__ = {"schema": config.DATABASE_SCHEMA} if config.DATABASE_SCHEMA else None

    def __repr__(self) -> str:
        return json.dumps(self.to_json(), indent=2)

    def to_json(self, exclude_fields: tuple = ()) -> dict:
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns if column.name not in exclude_fields
        }

    @staticmethod
    def commit() -> None:
        db.session.commit()

    def add(self) -> None:
        db.session.add(self)

    def insert(self) -> None:
        self.add()
        self.commit()

    def delete(self, commit: bool = True) -> None:
        db.session.delete(self)
        if commit:
            self.commit()

