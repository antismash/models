"""Base Redis<->Python object mapper"""
from datetime import datetime


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

                # (aio)redis can't handle bool or datetime types, int and float are fine
                if arg in self.BOOL_ARGS:
                    arg_val = str(arg_val)
                elif arg in self.DATE_ARGS:
                    arg_val = arg_val.strftime("%Y-%m-%d %H:%M:%S.%f")

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
                pass
            elif arg in self.BOOL_ARGS:
                val = (val != 'False')
            elif arg in self.INT_ARGS:
                val = int(val)
            elif arg in self.FLOAT_ARGS:
                val = float(val)
            elif arg in self.DATE_ARGS:
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")

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

        values = await self._db.hmget(self._key, *args, encoding='utf-8')

        self._parse(args, values)

    async def commit(self):
        return await self._db.hmset_dict(self._key, self.to_dict())

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

    def commit(self):
        # sync redis 'hmset' is the same as aioredis 'hmset_dict'. Go figure
        return self._db.hmset(self._key, self.to_dict())

    def delete(self):
        return self._db.delete(self._key)

    klass.fetch = fetch
    klass.commit = commit
    klass.delete = delete

    return klass

