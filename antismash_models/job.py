"""antiSMASH job abstraction"""
from datetime import datetime
from .base import BaseMapper


class Job(BaseMapper):
    """An antiSMASH job as represented in the Redis DB"""
    VALID_TAXA = {'bacterial', 'fungal', 'plant'}

    PROPERTIES = (
        'genefinder',
        'molecule_type',
        'state',
    )

    INTERNAL = (
        '_db',
        '_id',
        '_key',
        '_taxon',
    )

    ATTRIBUTES = (
        'added',
        'all_orfs',
        'asf',
        'borderpredict',
        'cassis',
        'cf_cdsnr',
        'cf_npfams',
        'cf_threshold',
        'clusterblast',
        'clusterfinder',
        'dispatcher',
        'download',
        'email',
        'filename',
        'from_pos',
        'full_hmmer',
        'gff3',
        'inclusive',
        'jobtype',
        'knownclusterblast',
        'last_changed',
        'minimal',
        'seed',
        'smcogs',
        'status',
        'subclusterblast',
        'to_pos',
        'transatpks_da',
        'tta',
    )

    BOOL_ARGS = {
        'all_orfs',
        'asf',
        'borderpredict',
        'cassis',
        'full_hmmer',
        'clusterblast',
        'clusterfinder',
        'inclusive',
        'knownclusterblast',
        'minimal',
        'smcogs',
        'subclusterblast',
        'transatpks_da',
        'tta',
    }

    INT_ARGS = {
        'cf_cdsnr',
        'cf_npfams',
        'from_pos',
        'seed',
        'to_pos',
    }

    FLOAT_ARGS = {
        'cf_threshold',
    }

    DATE_ARGS = {
        'added',
        'last_changed',
    }

    VALID_STATES = {
        'created',
        'downloading',
        'validating',
        'queued',
        'running',
        'done',
        'failed'
    }

    def __init__(self, db, job_id):
        super(Job, self).__init__(db, 'job:{}'.format(job_id))
        self._id = job_id

        # taxon is the first element of the ID
        self._taxon = self._id.split('-')[0]

        # storage for properties
        self._state = 'created'
        self._molecule_type = 'nucl'
        self._genefinder = 'none'

        # Regular attributes that differ from None
        self.status = 'pending'

    # Not really async, but follow the same API as the other properties
    @property
    def job_id(self):
        return self._id

    # No setter, job_id is a read-only property

    # Not really async, but follow same API as the other properties
    @property
    def taxon(self):
        return self._taxon

    # No setter, taxon is a read-only property

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        if value not in self.VALID_STATES:
            raise ValueError('Invalid state {}'.format(value))

        self._state = value
        self.changed()

    @property
    def molecule_type(self):
        return self._molecule_type

    @molecule_type.setter
    def molecule_type(self, value):
        if value not in {'nucl', 'prot'}:
            raise ValueError('Invalid molecule_type {}'.format(value))

        self._molecule_type = value

    @property
    def genefinder(self):
        return self._genefinder

    @genefinder.setter
    def genefinder(self, value):
        if value not in {'prodigal', 'prodigal-m', 'none'}:
            raise ValueError('Invalid genefinding method {}'.format(value))
        self._genefinder = value

    @staticmethod
    def is_valid_taxon(taxon: str) -> bool:
        """
        Check if taxon string is one of 'bacterial', 'fungal' or 'plant'
        """
        if taxon not in Job.VALID_TAXA:
            return False

        return True

    def changed(self):
        """Update the job's last changed timestamp"""
        self.last_changed = datetime.utcnow()

    def to_dict(self, extra_info=False):
        ret = super(Job, self).to_dict()

        if extra_info:
            ret['job_id'] = self.job_id
            ret['taxon'] = self.taxon

        return ret

    def __str__(self):
        return "Job(id: {}, state: {})".format(self._id, self.state)
