import asyncio
import pytest
import time

from antismash_models.control import AsyncControl, SyncControl


def test_init(sync_db):
    control = SyncControl(sync_db, 'name', 42, "1ced7ea")

    assert control.name == 'name'
    assert control.stop_scheduled is False
    assert control.running is True
    assert control.status == 'running'
    assert control.max_jobs == 42
    assert control.running_jobs == 0
    assert control.version == '1ced7ea'


def test_async_init(async_db):
    control = AsyncControl(async_db, 'name', 42)

    assert control.name == 'name'
    assert control.stop_scheduled is False
    assert control.running is True
    assert control.status == 'running'
    assert control.max_jobs == 42
    assert control.running_jobs == 0
    assert control.version == 'unknown'


def test_async_set_invalid(async_db):
    control = AsyncControl(async_db, 'name', 42)
    with pytest.raises(AttributeError):
        control.nope = 'foo'


def test_sync_set_invalid(sync_db):
    control = SyncControl(sync_db, 'name', 42)
    with pytest.raises(AttributeError):
        control.nope = 'foo'


@pytest.mark.asyncio
async def test_async_delete(async_db):
    control = AsyncControl(async_db, 'name', 42)
    await control.commit()
    assert 1 == await async_db.exists('control:name')

    await control.delete()
    assert 0 == await async_db.exists('control:name')


def test_sync_delete(sync_db):
    control = SyncControl(sync_db, 'name', 42)
    control.commit()
    assert 1 == sync_db.exists('control:name')

    control.delete()
    assert 0 == sync_db.exists('control:name')


@pytest.mark.asyncio
async def test_async_alive(async_db):
    control = AsyncControl(async_db, 'name', 42)
    await control.commit()

    # TODO: Use persist instead once available
    await asyncio.sleep(1)
    old_ttl = await async_db.ttl('control:name')
    assert old_ttl > -1

    await control.alive()
    new_ttl = await async_db.ttl('control:name')
    assert -1 < old_ttl <= new_ttl


def test_sync_alive(sync_db):
    control = SyncControl(sync_db, 'name', 42)
    control.commit()

    old_ttl = sync_db.ttl('control:name')
    assert old_ttl > -1

    # TODO: Use persist instead once available
    time.sleep(1)

    control.alive()
    new_ttl = sync_db.ttl('control:name')
    assert -1 < old_ttl <= new_ttl
