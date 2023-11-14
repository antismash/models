"""Utility functions used in multiple modules"""

from datetime import datetime

try:  # pragma: no cover
    from datetime import UTC  # type: ignore  # 3.10 and older don't have this

    def now():
        return datetime.now(UTC)
except ImportError:  # pragma: no cover

    def now():
        return datetime.utcnow()
