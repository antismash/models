import fakeredis
import fakeredis.aioredis as fakeaioredis

import pytest


@pytest.fixture
def async_db():
    return fakeaioredis.FakeRedis(encoding='utf-8', decode_responses=True)


@pytest.fixture
def sync_db():
    return fakeredis.FakeRedis(encoding='utf-8', decode_responses=True)
