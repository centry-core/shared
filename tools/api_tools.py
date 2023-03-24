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
from json import loads

from pylon.core.tools import log
from sqlalchemy import and_
from flask_restful import Api, Resource, reqparse, Resource, abort
# from werkzeug.exceptions import Forbidden

from .minio_client import MinioClient, MinioClientAdmin
from .rpc_tools import RpcMixin


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    return False


def build_req_parser(rules: tuple, location=("json", "values")) -> reqparse.RequestParser:
    request_parser = reqparse.RequestParser()
    for rule in rules:
        if isinstance(rule, dict):
            kwargs = rule.copy()
            name = kwargs["name"]
            del kwargs["name"]
            if "location" not in kwargs:
                # Use global location unless it"s specified by the rule.
                kwargs["location"] = location
            request_parser.add_argument(name, **kwargs)

        elif isinstance(rule, (list, tuple)):
            name, _type, required, default = rule
            kwargs = {
                "type": _type,
                "location": location,
                "required": required
            }
            if default is not None:
                kwargs["default"] = default
            request_parser.add_argument(name, **kwargs)

    return request_parser


def add_resource_to_api(api: Api, resource: Resource, *urls, **kwargs) -> None:
    # This /api/v1 thing is made here to be able to register auth endpoints for local development
    urls = (*(f"/api/v1{url}" for url in urls), *(f"/api/v1{url}/" for url in urls))
    api.add_resource(resource, *urls, **kwargs)


def _calcualte_limit(limit, total):
    return total if limit == 'All' or limit == 0 else limit


def get(project_id: int, args: dict, data_model, additional_filter: dict = None):
    rpc = RpcMixin().rpc
    project = rpc.call.project_get_or_404(project_id=project_id)
    limit_ = args.get("limit")
    offset_ = args.get("offset")
    if args.get("sort"):
        sort_rule = getattr(getattr(data_model, args["sort"]), args["order"])()
    else:
        sort_rule = data_model.id.desc()
    filter_ = list()
    filter_.append(operator.eq(data_model.project_id, project.id))
    if additional_filter:
        for key, value in additional_filter.items():
            filter_.append(operator.eq(getattr(data_model, key), value))
    if args.get('filter'):
        for key, value in loads(args.get('filter')).items():
            filter_.append(operator.eq(getattr(data_model, key), value))
    filter_ = and_(*tuple(filter_))
    total = data_model.query.order_by(sort_rule).filter(filter_).count()
    res = data_model.query.filter(filter_).order_by(sort_rule).limit(
        _calcualte_limit(limit_, total)).offset(offset_).all()
    return total, res


def upload_file_base(bucket: str, f, client, create_if_not_exists: bool = True):
    name = f.filename
    content = f.read()
    f.seek(0, 2)
    # file_size = f.tell()
    try:
        f.remove()
    except:
        pass
    if create_if_not_exists:
        if bucket not in client.list_bucket():
            bucket_type = 'system' if bucket in ('tasks', 'tests') else 'local'
            client.create_bucket(bucket=bucket, bucket_type=bucket_type)
    client.upload_file(bucket, content, name)


def upload_file(bucket, f, project, create_if_not_exists=True):
    client = MinioClient(project=project)
    upload_file_base(bucket, f, client, create_if_not_exists)


def upload_file_admin(bucket: str, f, create_if_not_exists: bool = True):
    client = MinioClientAdmin()
    upload_file_base(bucket, f, client, create_if_not_exists)


def format_date(date_object: datetime.datetime) -> str:
    date_format = '%d.%m.%Y %H:%M'
    return date_object.strftime(date_format)


class APIBase(Resource):
    mode_handlers = dict()
    url_params = list()

    def proxy_method(self, method: str, mode: str = 'default', **kwargs):
        log.info('Calling proxy method: [%s] mode: [%s] | %s', method, mode, kwargs)
        log.info('Proxy: [%s] ', method, mode, kwargs)
        try:
            return getattr(self.mode_handlers[mode](self), method)(**kwargs)
        except KeyError:
            abort(404)

    def __init__(self, module):
        self.module = module
        # if we have mode handlers then check if url params accept mode
        log.info('APIBase INIT %s | %s', self.mode_handlers, self.url_params)
        # if self.mode_handlers.keys():
        #     unaware_url_patterns = lambda: filter(lambda i: not i.startswith('<string:mode>'), self.url_params)
        #     mode_aware_patterns = list(map(lambda i: f'<string:mode>/{i}', unaware_url_patterns()))
        #     self.url_params = [*list(unaware_url_patterns()), *list(mode_aware_patterns)]
        # log.info('APIBase after check %s | %s', self.mode_handlers, self.url_params)

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
    def __init__(self, api: Resource):
        self._api = api

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
