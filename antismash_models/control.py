"""antiSMASH worker control abstraction"""
from .base import BaseMapper, async_mixin, sync_mixin


class BaseControl(BaseMapper):
    """Dispatcher management object"""

    ATTRIBUTES = (
        'max_jobs',
        'name',
        'running',
        'running_jobs',
        'status',
        'stop_scheduled',
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

    def __init__(self, db, name, max_jobs):
        super(BaseControl, self).__init__(db, "control:{}".format(name))
        self.name = name
        self.stop_scheduled = False
        self.running = True
        self.status = 'running'
        self.max_jobs = max_jobs
        self.running_jobs = 0


@async_mixin
class AsyncControl(BaseControl):
    """Control object using fetch/commit co-routines"""
    __slots__ = ()


@sync_mixin
class SyncControl(BaseControl):
    """Control object using sync fetch/commit functions"""
    __slots__ = ()
