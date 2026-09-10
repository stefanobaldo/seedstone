"""`lane`, with the JSON serializer — the suite tests it separately."""

from .lane import *  # noqa: F401,F403
from .lane import CACHES as _BASE

CACHES = {
    name: {**spec, "OPTIONS": {**spec["OPTIONS"], "SERIALIZER": "django_redis.serializers.json.JSONSerializer"}}
    for name, spec in _BASE.items()
}
