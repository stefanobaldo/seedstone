"""Cache settings for the compatibility lane.

Every alias points at one seedstone on db 0. The upstream suite's own settings
use db 1, which makes the client send SELECT on connect; this server has no
SELECT and needs none, because the database index is the suite's choice and not
the client library's. `doesnotexist` points at a closed port on purpose: the
suite uses it to exercise connection failure.

`default` and `sample` list the same server twice, which is the backend's
master/replica shape. Both entries resolve to the same instance, which
exercises the read/write client selection at no cost to this server.

The address comes from the environment because the runner decides it: on Linux
the container shares the host's network namespace and the server stays on the
loopback, while a containerised daemon on any other platform is reached across
a bridge. `doesnotexist` keeps its own closed port, since the point of that
alias is that nothing answers.
"""

import os

_HOST = os.environ.get("SEEDSTONE_HOST", "127.0.0.1")
_PORT = os.environ.get("SEEDSTONE_PORT", "6390")
_SERVER = "redis://%s:%s?db=0" % (_HOST, _PORT)

# Not a secret. Django refuses to configure without one, and this settings
# module exists only to point a test suite at a server on loopback: nothing
# here signs a cookie, a session or a token that outlives the container. A
# secret scanner will flag the line anyway, which is why it says so here.
SECRET_KEY = "seedstone-compat-lane"
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": [_SERVER, _SERVER],
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    },
    "doesnotexist": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:56379?db=0",
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    },
    "sample": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "%s,%s" % (_SERVER, _SERVER),
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    },
    "with_prefix": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": _SERVER,
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
        "KEY_PREFIX": "test-prefix",
    },
}
INSTALLED_APPS = ["django.contrib.sessions"]
USE_TZ = True
