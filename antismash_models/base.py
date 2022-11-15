"""Base Redis<->Python object mapper"""
from datetime import datetime
import json


class BaseMapper:
    """Base object mapper class"""

    ATTRIBUTES = ()
    PROPERTIES = ()
    INTERNAL = ('_db', '_key')

    __slots__ = ATTRIBUTES + INTERNAL + tuple(['_%s' % p for p in PROPERTIES])

    BOOL_ARGS = {}
    INT_ARGS = {}
    FLOAT_ARGS = {}
    DATE_ARGS = {}
    LIST_ARGS = {}

    def __init__(self, db, key):
        self._db = db
        self._key = key

        for attribute in self.ATTRIBUTES:
            setattr(self, attribute, None)

    def to_dict(self):
        ret = {}

        args = self.PROPERTIES + self.ATTRIBUTES

        for arg in args:
            if getattr(self, arg) is not None:
                arg_val = getattr(self, arg)

                # redis can't handle bool or datetime types, int and float are fine
                if arg in self.BOOL_ARGS:
                    arg_val = str(arg_val)
                elif arg in self.DATE_ARGS:
                    arg_val = arg_val.strftime("%Y-%m-%d %H:%M:%S.%f")
                elif arg in self.LIST_ARGS:
                    arg_val = json.dumps(arg_val)

                ret[arg] = arg_val

        return ret

    def _parse(self, args, values):
        for i, arg in enumerate(args):
            val = values[i]

            # Don't touch values that are unset
            if val is None and getattr(self, arg) is None:
                continue

            if val is None:
                # avoid type conversion for None values
                # this allows 'unsetting' values that used to be set
                # only create an empty list for list args
                if arg in self.LIST_ARGS:
                    val = []
            elif arg in self.BOOL_ARGS:
                val = (val != 'False')
            elif arg in self.INT_ARGS:
                val = int(val)
            elif arg in self.FLOAT_ARGS:
                val = float(val)
            elif arg in self.DATE_ARGS:
                # We're not totally fixated on sub-second resolution
                try:
                    val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            elif arg in self.LIST_ARGS:
                val = json.loads(val)

            setattr(self, arg, val)

    @classmethod
    def fromExisting(cls, new_id, existing):
        """"Create a copy from an existing object, with a new ID

        :param new_id: New key to use
        :param existing: Existing object to copy values from
        :return: New mapper object
        """
        if not isinstance(existing, cls):
            raise ValueError("Can't copy from type {!r}".format(existing.__class__))
        args, values = zip(*existing.to_dict().items())
        new = cls(existing._db, new_id)
        new._parse(args, values)
        return new


def async_mixin(klass):
    """Mixin for using aioredis for database connectivity"""
    async def fetch(self):
        args = self.PROPERTIES + self.ATTRIBUTES

        exists = await self._db.exists(self._key)
        if exists == 0:
            raise ValueError("No {} with ID {} in database, can't fetch".
                             format(self.__class__.__name__, self._key))

        values = await self._db.hmget(self._key, *args)

        self._parse(args, values)
        return self

    async def commit(self):
        return await self._db.hset(self._key, mapping=self.to_dict())

    async def delete(self):
        return await self._db.delete(self._key)

    klass.fetch = fetch
    klass.commit = commit
    klass.delete = delete

    return klass


def sync_mixin(klass):
    """Mixin for using redis for database connectivity"""
    def fetch(self):
        args = self.PROPERTIES + self.ATTRIBUTES

        exists = self._db.exists(self._key)
        if exists == 0:
            raise ValueError("No {} with ID {} in database, can't fetch".
                             format(self.__class__.__name__, self._key))

        values = self._db.hmget(self._key, *args)

        self._parse(args, values)
        return self

    def commit(self):
        return self._db.hset(self._key, mapping=self.to_dict())

    def delete(self):
        return self._db.delete(self._key)

    klass.fetch = fetch
    klass.commit = commit
    klass.delete = delete

    return klass
