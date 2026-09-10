"""`lane`, with the msgpack serializer — the suite tests it separately."""

from .lane import *  # noqa: F401,F403
from .lane import CACHES as _BASE

CACHES = {
    name: {**spec, "OPTIONS": {**spec["OPTIONS"], "SERIALIZER": "django_redis.serializers.msgpack.MSGPackSerializer"}}
    for name, spec in _BASE.items()
}
