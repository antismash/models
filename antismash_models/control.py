"""antiSMASH worker control abstraction"""
from .base import BaseMapper


class Control(BaseMapper):
    """Dispatcher management object"""

    ATTRIBUTES = (
        'name',
        'running',
        'stop_scheduled',
        'status',
        'max_jobs',
    )

    INTERNAL = (
        '_db',
        '_key',
    )

    BOOL_ARGS = {
        'running',
        'stop_scheduled'
    }

    INT_ARGS = {
        'max_jobs',
    }

    def __init__(self, db, name, max_jobs):
        super(Control, self).__init__(db, "control:{}".format(name))
        self.name = name
        self.stop_scheduled = False
        self.running = True
        self.status = 'running'
        self.max_jobs = max_jobs
