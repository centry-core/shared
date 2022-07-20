from queue import Empty

from flask_restful import Resource
from pylon.core.tools import log


class API(Resource):
    url_params = [
        '<int:project_id>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int):
        try:
            # project_id = self.module.context.rpc_manager.call.project_get_id()
            public_regions = self.module.context.rpc_manager.call.get_rabbit_queues("carrier")
            project_regions = self.module.context.rpc_manager.call.get_rabbit_queues(f"project_{project_id}_vhost")
        except Empty:
            log.warning('Cannot fetch project_id via RPC')
            return {'public_regions': [], 'project_regions': []}, 400
        public_regions.remove("__internal")
        return {'public_regions': public_regions, 'project_regions': project_regions}, 200

