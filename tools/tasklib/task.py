import re
from typing import Optional
from types import FunctionType
from collections import namedtuple
from pylon.core.tools import log

from pydantic import ValidationError, BaseModel


_data_tuple = namedtuple("Data", ("obj", "meta"))
_tasks_registry: dict = {}
_reverse_task_registy: dict = {}


class TaskMeta(BaseModel):
    uid: str
    tooltip: Optional[str]
    icon_url: Optional[str]


def register_task(rpc_name: str, func_obj: FunctionType, meta: dict):
    if rpc_name in _tasks_registry:
        log.error(f"Task with {rpc_name} name already exists")
        return False
    
    if not isinstance(func_obj, FunctionType):
        log.error("Function object is not passed")
        return False
    
    front_name_id = meta['uid']

    _tasks_registry[rpc_name] = _data_tuple(func_obj, meta)
    _reverse_task_registy[front_name_id] = rpc_name
    log.info(f"Registering function with name {rpc_name}")
    return True


def list_tasks_meta():
    return {
        entry.meta for entry in _tasks_registry.values() 
    }

def get_tasks_to_rpc_mappings():
    return _reverse_task_registy


### Decorators
def task(rpc_name:str, meta_payload):
    def _decorator(obj: FunctionType):
        try:
            meta = TaskMeta.validate(meta_payload)
        except ValidationError as e:
            log.error(f'Falid to register flowy task\nReason: {e}')
            return obj
        
        register_task(rpc_name, obj, meta.dict())
        return obj
    return _decorator



def handle_exceptions(fn):

    def _is_special_value(value):

        if not isinstance(value, str):
            return False
        
        variable_pattern = r"([a-zA-Z0-9_]+)"
        variables_pattern = r"{{variables\." + variable_pattern + r"}}"
        prev_pattern = r"{{nodes\['"+ variable_pattern + r"'\]\.?" + variable_pattern + r"?}}"

        if re.fullmatch(variables_pattern, value) or \
            re.fullmatch(prev_pattern, value):
            return True

        return False

    def decorated(self, **kwargs):
        try:
            fn(self, **kwargs)
            return {"ok": True}
        except ValidationError as e:
            valid_erros = []
            for error in e.errors():
                log.info(f"ERROR: {error}")
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
            return {"ok": False, "error": str(e)}
        
    return decorated


# from typing import Optional
# from types import FunctionType
# from ..rpc_tools import EventManagerMixin
# from pylon.core.tools import log
# from pydantic import ValidationError, BaseModel


# class TaskMeta(BaseModel):
#     uid: str
#     tooltip: Optional[str]
#     icon_url: Optional[str]


# ### Decorators
# def task(rpc_name:str, meta_payload):
#     def _decorator(obj: FunctionType):
#         try:
#             meta = TaskMeta.validate(meta_payload)
#         except ValidationError as e:
#             log.error(f'Falid to register flowy task\nReason: {e}')
#             return obj

#         event_manager = EventManagerMixin().event_manager
#         payload = {
#             'rpc_name': rpc_name,
#             'meta': meta.dict(),
#         }
#         log.info(f"BEFORE EVENT : {payload}")
#         event_manager.fire_event("register_task", payload)
#         return obj
#     return _decorator
