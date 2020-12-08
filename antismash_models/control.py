"""antiSMASH worker control abstraction"""
from functools import wraps
from .base import BaseMapper, async_mixin, sync_mixin


CONTROL_TIMEOUT = 300


class BaseControl(BaseMapper):
    """Dispatcher management object"""

    ATTRIBUTES = (
        'max_jobs',
        'name',
        'running',
        'running_jobs',
        'status',
        'stop_scheduled',
        'version',
    )

    INTERNAL = (
        '_db',
        '_key',
    )

    # Meh, needs to be repeated if we want to allow subclasses to have restricted attributes
    __slots__ = ATTRIBUTES + INTERNAL

    BOOL_ARGS = {
        'running',
        'stop_scheduled'
    }

    INT_ARGS = {
        'max_jobs',
        'running_jobs',
    }

    def __init__(self, db, name, max_jobs, version="unknown"):
        super(BaseControl, self).__init__(db, "control:{}".format(name))
        self.name = name
        self.stop_scheduled = False
        self.running = True
        self.status = 'running'
        self.max_jobs = max_jobs
        self.running_jobs = 0
        self.version = version


def expiring_async_mixin(klass):
    """Override async_mixin to expire the object"""
    def commit_expire(fn):
        @wraps(fn)
        async def wrapper(self):
            ret = await fn(self)
            await self._db.expire(self._key, CONTROL_TIMEOUT)
            return ret
        return wrapper

    async def alive(self):
        return await self._db.expire(self._key, CONTROL_TIMEOUT)

    klass = async_mixin(klass)
    klass.commit = commit_expire(klass.commit)
    klass.alive = alive

    return klass


def expiring_sync_mixin(klass):
    """Override the sync_mixin to expire the object"""
    def commit_expire(fn):
        @wraps(fn)
        def wrapper(self):
            ret = fn(self)
            self._db.expire(self._key, CONTROL_TIMEOUT)
            return ret
        return wrapper

    def alive(self):
        return self._db.expire(self._key, CONTROL_TIMEOUT)

    klass = sync_mixin(klass)
    klass.commit = commit_expire(klass.commit)
    klass.alive = alive

    return klass


@expiring_async_mixin
class AsyncControl(BaseControl):
    """Control object using fetch/commit co-routines"""
    __slots__ = ()


@expiring_sync_mixin
class SyncControl(BaseControl):
    """Control object using sync fetch/commit functions"""
    __slots__ = ()
