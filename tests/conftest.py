import mockaioredis

import pytest


@pytest.fixture
def db():
    return mockaioredis.MockRedis()