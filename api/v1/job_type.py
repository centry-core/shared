from flask_restful import Resource


class API(Resource):
    url_params = [
        '<int:project_id>/<string:test_uid>',
    ]

    def __init__(self, module):
        self.module = module

    def get(self, project_id: int, test_uid: str):
        self.module.context.rpc_manager.call.project_get_or_404(project_id=project_id)
        PLUGINS_TO_SEARCH = ['security_dast', 'backend_performance']

        for plugin in PLUGINS_TO_SEARCH:
            job_type = self.module.context.rpc_manager.call_function_with_timeout(
                func=f'{plugin}_job_type_by_uid',
                timeout=5,
                project_id=project_id,
                test_uid=test_uid
            )
            if job_type:
                return {'job_type': job_type}, 200
        return {'job_type': 'not_found'}, 200  # intentionally not 404
