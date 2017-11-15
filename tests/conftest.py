import mockaioredis
import mockredis

import pytest


@pytest.fixture
def async_db():
    return mockaioredis.MockRedis()


@pytest.fixture
def sync_db():
    return mockredis.MockRedis(encoding='utf-8', decode_responses=True)
