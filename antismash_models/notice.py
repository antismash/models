"""antiSMASH notice abstraction"""
from datetime import datetime, timedelta
from functools import wraps
from .base import BaseMapper, async_mixin, sync_mixin


class BaseNotice(BaseMapper):
    """Notice object"""

    PROPERTIES = (
        'category',
    )

    ATTRIBUTES = (
        'added',
        'show_from',
        'show_until',
        'teaser',
        'text',
    )

    INTERNAL = (
        '_db',
        '_key',
    )

    # Meh, needs to be repeated if we want to allow subclasses to have restricted attributes
    __slots__ = ATTRIBUTES + INTERNAL + tuple(['_%s' % p for p in PROPERTIES])

    DATE_ARGS = {
        'show_from',
        'show_until'
    }

    VALID_CATEGORIES = {
        'error',
        'info',
        'warning',
    }

    def __init__(self, db, notice_id):
        super(BaseNotice, self).__init__(db, "notice:{}".format(notice_id))
        self._category = 'info'

        # by default, show new notices immediately
        self.show_from = datetime.utcnow()
        # by default, show notices for a week
        self.show_until = self.show_from + timedelta(days=7)

        self.teaser = None
        self.text = None

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, val):
        if val not in self.VALID_CATEGORIES:
            raise ValueError("Invalid category {!r}".format(val))
        self._category = val


def expiring_async_mixin(klass):
    """Override async_mixin to expire the object"""
    def commit_expire(fn):
        @wraps(fn)
        async def wrapper(self):
            ret = await fn(self)
            # aio-redis expireat can't deal with datetime objects, but converting datetimes to timestamps sucks,
            # so use expire on the difference instead
            td = self.show_until - datetime.utcnow()
            await self._db.expire(self._key, int(td.total_seconds()))
            return ret
        return wrapper

    klass = async_mixin(klass)
    klass.commit = commit_expire(klass.commit)

    return klass


def expiring_sync_mixin(klass):
    """Override the sync_mixin to expire the object"""
    def commit_expire(fn):
        @wraps(fn)
        def wrapper(self):
            ret = fn(self)
            # regular redis doesn't deal with datetime objects anymore either, so also use expire on the difference
            td = self.show_until - datetime.utcnow()
            self._db.expire(self._key, int(td.total_seconds()))
            return ret
        return wrapper

    klass = sync_mixin(klass)
    klass.commit = commit_expire(klass.commit)

    return klass


@expiring_async_mixin
class AsyncNotice(BaseNotice):
    """Notice object using fetch/commit co-routines"""
    __slots__ = ()


@expiring_sync_mixin
class SyncNotice(BaseNotice):
    """Notice object using sync fetch/commit functions"""
    __slots__ = ()
