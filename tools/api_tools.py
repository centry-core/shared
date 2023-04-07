#     Copyright 2020 getcarrier.io
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
import datetime
import operator
from abc import ABC, abstractmethod
from functools import reduce
from json import loads
from typing import Union, Optional, Callable, Tuple, Any

from pylon.core.tools import log
from sqlalchemy import and_
from flask_restful import Api, Resource, reqparse, Resource, abort
# from werkzeug.exceptions import Forbidden

from .minio_client import MinioClient, MinioClientAdmin
from .rpc_tools import RpcMixin


# def str2bool(v):
#     if v.lower() in ('yes', 'true', 't', 'y', '1'):
#         return True
#     elif v.lower() in ('no', 'false', 'f', 'n', '0'):
#         return False
#     return False


# def build_req_parser(rules: tuple, location=("json", "values")) -> reqparse.RequestParser:
#     request_parser = reqparse.RequestParser()
#     for rule in rules:
#         if isinstance(rule, dict):
#             kwargs = rule.copy()
#             name = kwargs["name"]
#             del kwargs["name"]
#             if "location" not in kwargs:
#                 # Use global location unless it"s specified by the rule.
#                 kwargs["location"] = location
#             request_parser.add_argument(name, **kwargs)
#
#         elif isinstance(rule, (list, tuple)):
#             name, _type, required, default = rule
#             kwargs = {
#                 "type": _type,
#                 "location": location,
#                 "required": required
#             }
#             if default is not None:
#                 kwargs["default"] = default
#             request_parser.add_argument(name, **kwargs)
#
#     return request_parser


def get(project_id: Optional[int], args: dict, data_model,
        additional_filters: Optional[list] = None,
        rpc_manager: Optional[Callable] = None,
        mode: str = 'default'
        ) -> Tuple[int, list]:
    def _calculate_limit(limit: Union[str, int], total: int):
        return total if limit == 'All' or limit == 0 else limit

    limit_ = args.get("limit")
    offset_ = args.get("offset")
    if args.get("sort"):
        sort_rule = getattr(getattr(data_model, args["sort"]), args["order"])()
    else:
        sort_rule = data_model.id.desc()

    filter_ = []
    try:
        filter_.append(operator.eq(data_model.mode, mode))
    except AttributeError:
        ...

    if mode == 'default':
        if not rpc_manager:
            rpc_manager = RpcMixin().rpc
        project = rpc_manager.call.project_get_or_404(project_id=project_id)
        filter_.append(operator.eq(data_model.project_id, project.id))

    if additional_filters:
        filter_.extend(additional_filters)
        # for key, value in additional_filter.items():
        #     filter_.append(operator.eq(getattr(data_model, key), value))

    if args.get('filter'):
        for key, value in loads(args.get('filter')).items():
            filter_.append(operator.eq(getattr(data_model, key), value))

    filter_ = and_(*filter_)
    total = data_model.query.order_by(sort_rule).filter(filter_).count()
    res = data_model.query.filter(filter_).order_by(sort_rule).limit(
        _calculate_limit(limit_, total)).offset(offset_).all()

    return total, res


def upload_file_base(bucket: str, data: bytes, file_name: str, client, create_if_not_exists: bool = True) -> None:
    if create_if_not_exists:
        if bucket not in client.list_bucket():
            bucket_type = 'system' if bucket in ('tasks', 'tests') else 'local'
            client.create_bucket(bucket=bucket, bucket_type=bucket_type)
    client.upload_file(bucket, data, file_name)


def upload_file(bucket: str, f, project, create_if_not_exists: bool = True, **kwargs) -> None:
    upload_file_base(
        bucket=bucket,
        data=f.read(),
        file_name=f.filename,
        client=MinioClient(project=project),
        create_if_not_exists=create_if_not_exists
    )
    try:
        f.remove()
    except:
        pass


def upload_file_admin(bucket: str, f, create_if_not_exists: bool = True, **kwargs) -> None:
    upload_file_base(
        bucket=bucket,
        data=f.read(),
        file_name=f.filename,
        client=MinioClientAdmin(),
        create_if_not_exists=create_if_not_exists
    )
    try:
        f.remove()
    except:
        pass


class APIBase(Resource):
    mode_handlers = dict()
    url_params = list()

    def proxy_method(self, method: str, mode: str = 'default', **kwargs):
        log.info(
            'Calling proxy method: [%s] mode: [%s] | %s',
            method, mode, kwargs
        )
        try:
            return getattr(self.mode_handlers[mode](self, mode), method)(**kwargs)
        except KeyError:
            abort(404)

    def __init__(self, module):
        self.module = module
        log.info('APIBase INIT %s | %s', self.mode_handlers, self.url_params)

    def get(self, **kwargs):
        return self.proxy_method('get', **kwargs)

    def post(self, **kwargs):
        return self.proxy_method('post', **kwargs)

    def put(self, **kwargs):
        return self.proxy_method('put', **kwargs)

    def delete(self, **kwargs):
        return self.proxy_method('delete', **kwargs)

    def patch(self, **kwargs):
        return self.proxy_method('patch', **kwargs)


class APIModeHandler:
    def __init__(self, api: Resource, mode: str = 'default'):
        self._api = api
        self.mode = mode

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            if item in ['get', 'post', 'put', 'delete', 'head', 'options', 'patch']:
                abort(404)
            return getattr(self._api, item)


def build_api_url(
        plugin: str, file_name: str, mode: str = 'default',
        api_version: int = 1, trailing_slash: bool = False
):
    return f"/api/v{api_version}/{plugin}/{file_name}/{mode}{'/' if trailing_slash else ''}"
