from datetime import datetime
import pytest
from antismash_models.job import BaseJob, AsyncJob, SyncJob


def test_init(sync_db):
    fake_id = 'taxon-fake'
    job = BaseJob(sync_db, fake_id)

    assert job.job_id == fake_id
    assert job.taxon == 'taxon'

    assert job.state == 'created'
    with pytest.raises(ValueError):
        job.state = 'bob'
    job.state = 'downloading'
    assert job.state == 'downloading'

    assert job.molecule_type == 'nucl'
    with pytest.raises(ValueError):
        job.molecule_type = 'bob'
    job.molecule_type = 'prot'
    assert job.molecule_type == 'prot'

    assert job.genefinder == 'none'
    with pytest.raises(ValueError):
        job.genefinder = 'bob'
    job.genefinder = 'prodigal'
    assert job.genefinder == 'prodigal'

    assert job.genefinding == 'prodigal'
    with pytest.raises(ValueError):
        job.genefinding = 'bob'
    job.genefinding = 'none'
    assert job.genefinding == 'none'
    assert job.genefinder == job.genefinding


def test_init_legacy(sync_db):
    fake_id = 'a7db5650-ec0d-4ca8-b3b2-c5de27a8cdf3'
    job = BaseJob(sync_db, fake_id)
    assert job._legacy
    assert job.taxon == 'bacteria'

    assert job.state == 'created'
    job.status = "done: All finished"
    assert job.state == 'done'

    job.genefinder = 'prodigal_m'
    assert job.genefinder == 'prodigal-m'

    job.molecule_type = None
    assert job.molecule_type == 'nucl'

    job.status = b'failed: Invalid utf-8 \x21'
    assert job.state == 'failed'


def test_legacy_from_state(sync_db):
    fake_id = 'bacteria-fake'
    job = BaseJob(sync_db, fake_id)
    assert not job._legacy

    job.state = None

    assert job._legacy


def test_fromExisting(sync_db):
    first_id = 'bacteria-first'
    second_id = 'bacteria-second'
    first_job = BaseJob(sync_db, first_id)
    first_job.download = 'FAKE1234'

    second_job = BaseJob.fromExisting(second_id, first_job)
    assert first_job.job_id != second_job.job_id
    assert first_job.download == second_job.download
    assert second_job.original_id == first_job.job_id

    with pytest.raises(ValueError):
        SyncJob.fromExisting('bactaria-broken', first_job)


def test_is_valid_taxon():
    assert BaseJob.is_valid_taxon('bacteria')
    assert BaseJob.is_valid_taxon('fungi')
    assert BaseJob.is_valid_taxon('bob') is False


def test_to_dict(sync_db):
    fake_id = 'taxon-fake'
    job = BaseJob(sync_db, fake_id)
    job.tta = True
    now = datetime.utcnow()
    job.added = now
    job.last_changed = now

    expected = {
        'genefinder': 'none',
        'job_id': fake_id,
        'taxon': 'taxon',
        'molecule_type': 'nucl',
        'state': 'created',
        'status': 'pending',
        'tta': 'True',
        'added': now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        'last_changed': now.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }
    ret = job.to_dict(True)
    assert ret == expected


def test_str(sync_db):
    fake_id = 'taxon-fake'
    job = BaseJob(sync_db, fake_id)

    assert str(job) == "Job(id: {}, state: created)".format(fake_id)


@pytest.mark.asyncio
async def test_async_commit(async_db):
    fake_id = 'taxon-fake'
    job = AsyncJob(async_db, fake_id)
    job.tta = True
    now = datetime.utcnow()
    job.added = now
    await job.commit()

    assert await async_db.exists('job:taxon-fake')


def test_sync_commit(sync_db):
    fake_id = 'taxon-fake'
    job = SyncJob(sync_db, fake_id)
    job.tta = True
    now = datetime.utcnow()
    job.added = now
    job.commit()

    assert sync_db.exists('job:taxon-fake')


@pytest.mark.asyncio
async def test_async_fetch(async_db):
    now = datetime.utcnow()
    job_dict = {
        'added': now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        'cf_threshold': 0.5,
        'genefinder': 'none',
        'molecule_type': 'nucl',
        'seed': 42,
        'state': 'queued',
        'status': 'waiting for job to start',
        'tta': 'True',
    }

    await async_db.hmset_dict('job:taxon-fake', job_dict)
    job = AsyncJob(async_db, 'taxon-fake')
    await job.fetch()

    assert job.tta
    assert job.seed == 42
    assert job.cf_threshold == 0.5


@pytest.mark.asyncio
async def test_async_fetch_invalid(async_db):
    job = AsyncJob(async_db, 'taxon-fake')
    with pytest.raises(ValueError):
        await job.fetch()


def test_sync_fetch(sync_db):
    now = datetime.utcnow()
    job_dict = {
        'added': now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        'cf_threshold': 0.5,
        'genefinder': 'none',
        'molecule_type': 'nucl',
        'seed': 42,
        'state': 'queued',
        'status': 'waiting for job to start',
        'tta': 'True',
    }

    sync_db.hmset('job:taxon-fake', job_dict)
    job = SyncJob(sync_db, 'taxon-fake')
    job.fetch()

    assert job.tta
    assert job.seed == 42
    assert job.cf_threshold == 0.5


def test_sync_fetch_invalid(sync_db):
    job = SyncJob(sync_db, 'taxon-fake')
    with pytest.raises(ValueError):
        job.fetch()


def test_async_set_invalid(async_db):
    job = AsyncJob(async_db, 'taxon-fake')
    with pytest.raises(AttributeError):
        job.nope = 'foo'


def test_sync_set_invalid(sync_db):
    job = SyncJob(sync_db, 'taxon-fake')
    with pytest.raises(AttributeError):
        job.nope = 'foo'
