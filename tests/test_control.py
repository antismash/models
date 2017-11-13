from antismash_models.control import Control


def test_init(db):
    control = Control(db, 'name', 42)

    assert control.name == 'name'
    assert control.stop_scheduled is False
    assert control.running is True
    assert control.status == 'running'
    assert control.max_jobs == 42
    assert control.running_jobs == 0
