"""Tests for the Notice objects"""
import pytest

from antismash_models.notice import AsyncNotice, SyncNotice


def test_init_sync(sync_db):
    notice = SyncNotice(sync_db, 'fake')
    assert notice._key == 'notice:fake'
    assert notice.category == 'info'
    assert notice.text is None
    notice.category = 'warning'
    assert notice.category == 'warning'

    with pytest.raises(ValueError):
        notice.category = 'bob'


def test_sync_expire(sync_db):
    notice = SyncNotice(sync_db, 'fake')
    notice.commit()
    # default expiration is 1 week == 604800 seconds
    assert sync_db.ttl(notice._key) <= 604800


@pytest.mark.asyncio
async def test_async_expire(async_db):
    notice = AsyncNotice(async_db, 'fake')
    await notice.commit()
    # default expiration is 1 week == 604800 seconds
    assert await async_db.ttl(notice._key) <= 604800
