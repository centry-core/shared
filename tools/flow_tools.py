import re
from functools import wraps
from typing import Optional
from pylon.core.tools import log

from pydantic import ValidationError


def handle_exceptions(fn):
    def _is_special_value(value):
        if not isinstance(value, str):
            return False

        variable_pattern = r"([a-zA-Z0-9_]+)"
        variables_pattern = r"{{variables\." + variable_pattern + r"}}"
        prev_pattern = r"{{nodes\['" + variable_pattern + r"'\]\.?" + variable_pattern + r"?}}"

        if re.fullmatch(variables_pattern, value) or \
                re.fullmatch(prev_pattern, value):
            return True

        return False

    @wraps(fn)
    def decorated(**kwargs):
        try:
            fn(**kwargs)
            return {"ok": True}
        except ValidationError as e:
            valid_erros = []
            for error in e.errors():
                log.info(f"Flow validation error: {error}")
                if error['type'] == "value_error.missing" or "__root__" in error['loc']:
                    valid_erros.append(error)
                    continue

                invalid_value = {**kwargs}
                for loc in error["loc"]:
                    invalid_value = invalid_value[loc]

                # check for special values
                if not _is_special_value(invalid_value):
                    valid_erros.append(error)

            if valid_erros:
                return {"ok": False, "errors": valid_erros}
            return {"ok": True}
        except Exception as e:
            # log.error(e)
            return {"ok": False, "error": str(e)}

    return decorated


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

        # structure['rpc_name'] = self.get_rpc_name(uid)
        log.info('Registering flow <%s>', structure)

        self._registry[uid] = structure

    def register_validator(self, flow_uid: str) -> None:
        log.info('Registering flow validator: <%s> for flow <%s>', self.get_validator_rpc_name(flow_uid), flow_uid)
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
            self.module.context.rpc_manager.register_function(
                func,
                name=self.get_rpc_name(uid)
            )
            return func

        return wrapper

    def validator(self, flow_uid: str, **kwargs):
        self.register_validator(flow_uid)

        def wrapper(func):
            self.module.context.rpc_manager.register_function(
                handle_exceptions(func),
                name=self.get_validator_rpc_name(flow_uid)
            )
            return func

        return wrapper
