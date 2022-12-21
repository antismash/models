"""antiSMASH notice abstraction"""
from __future__ import annotations
from datetime import datetime, timedelta
from functools import wraps
from typing import TypeVar, Union

from .base import BaseMapper, DataBase, async_mixin, sync_mixin

TNotice = TypeVar("TNotice", bound="BaseNotice")


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
        '_id',
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

    def __init__(self, db: DataBase, notice_id: str, *, category: str = "info",
                 teaser: str = "placeholder", text: str = "placeholder",
                 show_from: Union[datetime, None] = None,
                 show_until: Union[datetime, None] = None):
        super(BaseNotice, self).__init__(db, "notice:{}".format(notice_id))
        self._id: str = notice_id
        self.category = category

        # by default, show new notices immediately
        self.show_from: datetime = show_from if show_from else datetime.utcnow()
        # by default, show notices for a week
        self.show_until: datetime = show_until if show_until else self.show_from + timedelta(days=7)

        self.teaser: str = teaser
        self.text: str = text

    @property
    def notice_id(self) -> str:
        return self._id

    @property
    def category(self) -> str:
        return self._category

    @category.setter
    def category(self, val: str) -> None:
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
