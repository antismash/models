from datetime import datetime
import pytest
from antismash_models.job import Job


def test_init(db):
    fake_id = 'taxon-fake'
    job = Job(db, fake_id)

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


def test_is_valid_taxon():
    assert Job.is_valid_taxon('bacterial')
    assert Job.is_valid_taxon('bob') is False


def test_to_dict(db):
    fake_id = 'taxon-fake'
    job = Job(db, fake_id)
    job.tta = True
    now = datetime.utcnow()
    job.added = now

    expected = {
        'genefinder': 'none',
        'job_id': fake_id,
        'taxon': 'taxon',
        'molecule_type': 'nucl',
        'state': 'created',
        'status': 'pending',
        'tta': 'True',
        'added': now.strftime("%Y-%m-%d %H:%M:%S.%f")
    }
    ret = job.to_dict(True)
    assert ret == expected


def test_str(db):
    fake_id = 'taxon-fake'
    job = Job(db, fake_id)

    assert str(job) == "Job(id: {}, state: created)".format(fake_id)


@pytest.mark.asyncio
async def test_commit(db):
    fake_id = 'taxon-fake'
    job = Job(db, fake_id)
    job.tta = True
    now = datetime.utcnow()
    job.added = now
    await job.commit()

    assert await db.exists('job:taxon-fake')


@pytest.mark.asyncio
async def test_fetch(db):
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

    await db.hmset_dict('job:taxon-fake', job_dict)
    job = Job(db, 'taxon-fake')
    await job.fetch()

    assert job.tta
    assert job.seed == 42
    assert job.cf_threshold == 0.5


@pytest.mark.asyncio
async def test_fetch_invalid(db):
    job = Job(db, 'taxon-fake')
    with pytest.raises(ValueError):
        await job.fetch()
