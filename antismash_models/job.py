"""antiSMASH job abstraction"""
from datetime import datetime
from .base import BaseMapper, async_mixin, sync_mixin


class BaseJob(BaseMapper):
    """An antiSMASH job as represented in the Redis DB"""
    VALID_TAXA = {'bacteria', 'fungi', 'plant'}

    PROPERTIES = (
        'genefinder',
        'molecule_type',
        'state',
        'status',
    )

    INTERNAL = (
        '_db',
        '_id',
        '_key',
        '_taxon',
        '_legacy',
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
        'clusterhmmer',
        'dispatcher',
        'download',
        'email',
        'filename',
        'from_pos',
        'full_hmmer',
        'gff3',
        'inclusive',
        'ip_addr',
        'jobtype',
        'knownclusterblast',
        'last_changed',
        'minimal',
        'needs_download',
        'original_id',
        'pfam2go',
        'seed',
        'smcogs',
        'subclusterblast',
        'to_pos',
        'transatpks_da',
        'tta',
    )

    # Meh, needs to be repeated if we want to allow subclasses to have restricted attributes
    __slots__ = ATTRIBUTES + INTERNAL + tuple(['_%s' % p for p in PROPERTIES])

    BOOL_ARGS = {
        'all_orfs',
        'asf',
        'borderpredict',
        'cassis',
        'full_hmmer',
        'clusterblast',
        'clusterfinder',
        'clusterhmmer',
        'inclusive',
        'knownclusterblast',
        'minimal',
        'needs_download',
        'pfam2go',
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
        'waiting',
        'queued',
        'running',
        'done',
        'failed',
        'removed',
    }

    def __init__(self, db, job_id):
        super(BaseJob, self).__init__(db, 'job:{}'.format(job_id))
        self._id = job_id

        # taxon is the first element of the ID
        self._taxon = self._id.split('-')[0]
        self._legacy = False

        # unless this is a legacy job id
        if not self.is_valid_taxon(self._taxon) and job_id.count('-') == 4:
            self._taxon = 'bacteria'
            self._legacy = True


        # storage for properties
        self._state = 'created'
        self._molecule_type = 'nucl'
        self._genefinder = 'none'
        self.status = 'pending'

        # Regular attributes that differ from None
        self.added = datetime.utcnow()
        self.last_changed = datetime.utcnow()

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
        if self._legacy:
            state = self.status.split(' ')[0].rstrip(':')
            if state in self.VALID_STATES:
                return state
        return self._state

    @state.setter
    def state(self, value):
        if value is None:
            self._legacy = True
            return

        if value not in self.VALID_STATES:
            raise ValueError('Invalid state {}'.format(value))

        self._state = value
        self.changed()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if type(value) == bytes:
            value = value.decode('utf-8', 'ignore')
        self._status = value

    @property
    def molecule_type(self):
        return self._molecule_type

    @molecule_type.setter
    def molecule_type(self, value):
        if value is None:
            value = 'nucl'

        if value == 'nucleotide':
            value = 'nucl'

        if value not in {'nucl', 'prot'}:
            raise ValueError('Invalid molecule_type {}'.format(value))

        self._molecule_type = value

    @property
    def genefinder(self):
        return self._genefinder

    @genefinder.setter
    def genefinder(self, value):
        if value not in {'prodigal', 'prodigal-m', 'glimmerhmm', 'error', 'none'}:
            if self._legacy and value in {'glimmer', 'prodigal_m'}:
                if value == 'prodigal_m':
                    value = 'prodigal-m'
            else:
                raise ValueError('Invalid genefinding method {}'.format(value))
        self._genefinder = value

    # We want to migrate from "genefinder" to "genefinding" eventually
    @property
    def genefinding(self):
        return self._genefinder

    @genefinding.setter
    def genefinding(self, value):
        self.genefinder = value

    @staticmethod
    def is_valid_taxon(taxon: str) -> bool:
        """
        Check if taxon string is one of 'bacterial', 'fungal' or 'plant'
        """
        if taxon not in BaseJob.VALID_TAXA:
            return False

        return True

    def changed(self):
        """Update the job's last changed timestamp"""
        self.last_changed = datetime.utcnow()

    def to_dict(self, extra_info=False):
        ret = super(BaseJob, self).to_dict()

        if extra_info:
            ret['job_id'] = self.job_id
            ret['taxon'] = self.taxon

        return ret

    def __str__(self):
        return "Job(id: {}, state: {})".format(self._id, self.state)

    @classmethod
    def fromExisting(cls, new_id, existing):
        """"Create a copy from an existing job, with a new ID

        :param new_id: New key to use
        :param existing: Existing job to copy values from
        :return: New job object
        """
        new = super(BaseJob, cls).fromExisting(new_id, existing)
        new.original_id = existing.job_id
        return new


@async_mixin
class AsyncJob(BaseJob):
    """Job using fetch/commit as co-routines"""
    __slots__ = ()


@sync_mixin
class SyncJob(BaseJob):
    """Job using sync fetch/commit functions"""
    __slots__ = ()
