"""Tests for the Notice objects"""
import pytest

from antismash_models.notice import AsyncNotice, SyncNotice


def test_init_sync(sync_db):
    notice = SyncNotice(sync_db, 'fake')
    assert notice._key == 'notice:fake'
    assert notice.category == 'info'
    assert notice.text == "placeholder"
    assert notice.teaser == "placeholder"
    notice.category = 'warning'
    assert notice.category == 'warning'
    assert notice.notice_id == "fake"

    with pytest.raises(ValueError):
        notice.category = 'bob'


def test_sync_expire(sync_db):
    notice = SyncNotice(sync_db, 'fake')
    notice.commit()
    # default expiration is 1 week == 604800 seconds
    assert sync_db.ttl(notice._key) <= 604800


def test_low_res_date(sync_db):
    notice = SyncNotice(sync_db, 'fake')
    notice.commit()

    date_str = sync_db.hget(notice._key, 'show_from')
    assert date_str == notice.show_from.strftime("%Y-%m-%d %H:%M:%S.%f")
    sync_db.hset(notice._key, 'show_from', date_str[:date_str.find('.')])

    notice.fetch()


@pytest.mark.asyncio
async def test_async_expire(async_db):
    notice = AsyncNotice(async_db, 'fake')
    await notice.commit()
    # default expiration is 1 week == 604800 seconds
    assert await async_db.ttl(notice._key) <= 604800
