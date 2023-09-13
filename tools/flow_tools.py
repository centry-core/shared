from typing import Optional
from pylon.core.tools import log


class FlowNodes:
    _registry = {}

    def __init__(self, module):
        self.module = module

    @staticmethod
    def get_rpc_name(uid: str) -> str:
        return f'flows_node_{uid}'

    def register(
            self,
            uid: str,
            display_name: Optional[str] = None,
            tooltip: str = '',
            icon_url: Optional[str] = None,
            inputs: int = 1,
            outputs: int = 1,
            validation_rpc: Optional = None,
            **kwargs):

        def wrapper(func, *args, **kwargs):
            self.module.context.rpc_manager.register_function(self.get_rpc_name(uid), func)
            return func

        structure = {
            'uid': uid,
            'display_name': display_name or uid,
            'tooltip': tooltip,
            'icon_url': icon_url,
            'inputs': inputs,
            'outputs': outputs,
            'validation_rpc': validation_rpc
        }
        structure.update(kwargs)

        # todo: remove that
        structure['rpc_name_tmp'] = self.get_rpc_name(uid)
        log.info('registering flow %s', structure)

        self._registry[uid] = structure
        return wrapper
