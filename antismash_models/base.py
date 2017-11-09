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

                # aioredis can't handle bool or datetime types, int and float are fine
                if arg in self.BOOL_ARGS:
                    arg_val = str(arg_val)
                elif arg in self.DATE_ARGS:
                    arg_val = arg_val.strftime("%Y-%m-%d %H:%M:%S.%f")

                ret[arg] = arg_val

        return ret

    async def fetch(self):
        args = self.PROPERTIES + self.ATTRIBUTES

        exists = await self._db.exists(self._key)
        if exists == 0:
            raise ValueError("No {} with ID {} in database, can't fetch".\
                             format(self.__class__.__name__,  self._key))

        values = await self._db.hmget(self._key, *args, encoding='utf-8')

        for i, arg in enumerate(args):
            val = values[i]

            if val is None:
                continue

            if arg in self.BOOL_ARGS:
                val = (val != 'False')
            elif arg in self.INT_ARGS:
                val = int(val)
            elif arg in self.FLOAT_ARGS:
                val = float(val)
            elif arg in self.DATE_ARGS:
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")

            setattr(self, arg, val)

    async def commit(self):
        return await self._db.hmset_dict(self._key, self.to_dict())