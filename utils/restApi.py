from flask_restful import Resource
from pylon.core.tools import log

from ..tools.rpc_tools import RpcMixin


class RestResource(Resource):
    def __init__(self):
        self.logger = log
        self.rpc_manager = RpcMixin()
        self.rpc = self.rpc_manager.rpc.call
