from typing import Dict, Type
from types import FunctionType
from collections import namedtuple
from pylon.core.tools import log


_data_tuple = namedtuple("Data", ("obj", "params", "display_name"))
_tasks_registry = dict()
#Dict[str, NamedTuple[Union[FunctionType, Type]]]


def register_task(name: str, func_obj: FunctionType, parameters: Dict, display_name:str=None):
    if name in _tasks_registry:
        log.error(f"Task with {name} name already exists")
        return False
    
    if not isinstance(func_obj, FunctionType):
        log.error("Function object is not passed")
        return False
    
    if display_name is None:
        display_name = ' '.join(tuple(map(lambda x:x.capitalize(), name.split("_")[1:])))

    _tasks_registry[name] = _data_tuple(func_obj, parameters, display_name)
    log.info(f"Registering function with name {name}")
    return True


def list_tasks_meta():
    return {name:(entry.params, entry.display_name) for name, entry in _tasks_registry.items()}


def get_task_meta(name:str):
    entry = _tasks_registry.get(name)
    if not entry:
        log.error(f"Task with {name} not found")
        return 
    return entry.params, entry.display_name 


def get_task_object(name:str):
    entry = _tasks_registry.get(name)
    if not entry:
        log.error(f"Task with {name} not found")
        return
    return entry.obj, entry.display_name


### Decorators
def task(name:str, params:Dict[str, Type]):
    def _decorator(obj: FunctionType):
        register_task(name, obj, params)
        return obj
    return _decorator
