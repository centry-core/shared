from datetime import datetime

from typing import Any

import json


def formatted_value(obj: Any):
    if isinstance(obj, datetime):
        return obj.isoformat(timespec='seconds') + 'Z'
    else:
        return str(obj)


def serialize(obj: Any, with_attrs: list | None = None) -> Any:
    if isinstance(obj, (list, set, tuple)):
        return [serialize(i, with_attrs=with_attrs) for i in obj]
    try:
        j = obj.model_dump_json()
    except AttributeError:
        try:
            j = obj.json()
        except AttributeError:
            try:
                result = {i.name: getattr(obj, i.name) for i in obj.__table__.columns}
                if with_attrs:
                    for i in with_attrs:
                        result[i] = getattr(obj, i)
                j = json.dumps(result, ensure_ascii=False, default=lambda o: serialize(o))
            except AttributeError:
                j = json.dumps(obj, ensure_ascii=False, default=formatted_value)
    return json.loads(j)
