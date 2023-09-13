from typing import Optional
from pylon.core.tools import log


class FlowNodes:
    _registry = {}

    def __init__(self, module):
        self.module = module

    @staticmethod
    def get_rpc_name(uid: str) -> str:
        return f'flows_node_{uid}'

    @staticmethod
    def get_validator_rpc_name(uid: str) -> str:
        return f'flows_node_validator_{uid}'

    def register(
            self,
            uid: str,
            display_name: Optional[str] = None,
            tooltip: str = '',
            icon_url: Optional[str] = None,
            icon_fa: Optional[str] = None,
            inputs: int = 1,
            outputs: int = 1,
            validation_rpc: Optional[str] = None,
            **kwargs
    ) -> None:
        structure = {
            'uid': uid,
            'display_name': display_name or uid,
            'tooltip': tooltip,
            'icon_url': icon_url,
            'icon_fa': icon_fa,
            'inputs': inputs,
            'outputs': outputs,
            'validation_rpc': validation_rpc
        }
        structure.update(kwargs)

        # todo: remove that
        structure['rpc_name_tmp'] = self.get_rpc_name(uid)
        log.info('Registering flow %s', structure)

        self._registry[uid] = structure

    def register_validator(self, flow_uid: str) -> None:
        log.info('Registering flow validator for flow %s', flow_uid)
        try:
            if self._registry[flow_uid]['validation_rpc']:
                log.critical(
                    'Validator already exists for flow %s : %s', flow_uid,
                    self._registry[flow_uid]['validation_rpc']
                )
                raise ValueError(
                    f'Validator already exists for flow {flow_uid} : {self._registry[flow_uid]["validation_rpc"]}'
                )
            self._registry[flow_uid]['validation_rpc'] = self.get_validator_rpc_name(flow_uid)
        except KeyError:
            log.critical(f'Flow {flow_uid} does not exist or is not registered')
            raise KeyError(f'Flow {flow_uid} does not exist or is not registered')

    def flow(
            self,
            uid: str,
            display_name: Optional[str] = None,
            tooltip: str = '',
            icon_url: Optional[str] = None,
            icon_fa: Optional[str] = None,
            inputs: int = 1,
            outputs: int = 1,
            validation_rpc: Optional[str] = None,
            **kwargs
    ):
        self.register(
            uid=uid,
            display_name=display_name,
            tooltip=tooltip,
            icon_url=icon_url,
            icon_fa=icon_fa,
            inputs=inputs,
            outputs=outputs,
            validation_rpc=validation_rpc,
            **kwargs
        )

        def wrapper(func):
            self.module.context.rpc_manager.register_function(self.get_rpc_name(uid), func)
            return func

        return wrapper

    def validator(self, flow_uid: str, **kwargs):
        self.register_validator(flow_uid)

        def wrapper(func):
            self.module.context.rpc_manager.register_function(self.get_validator_rpc_name(flow_uid), func)
            return func

        return wrapper
