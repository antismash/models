"""antiSMASH job abstraction"""
from __future__ import annotations
from datetime import datetime
import string
from typing import Any, Optional, Type, TypeVar, Union
from warnings import warn

from .base import BaseMapper, DataBase, async_mixin, sync_mixin
from .utils import now

TJob = TypeVar("TJob", bound="BaseJob")


class BaseJob(BaseMapper):
    """An antiSMASH job as represented in the Redis DB"""
    VALID_TAXA = {'bacteria', 'fungi', 'plant'}

    PROPERTIES = (
        'genefinder',
        'hmmdetection_strictness',
        'molecule_type',
        'sideload_simple',
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
        'cc_mibig',
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
        'ncbi_context',
        'needs_download',
        'original_id',
        'pfam2go',
        'rre',
        'rre_minlength',
        'rre_cutoff',
        'seed',
        'sideloads',
        'smcogs',
        'smcog_trees',
        'subclusterblast',
        'target_queues',
        'tfbs',
        'tigrfam',
        'to_pos',
        'trace',
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
        'cc_mibig',
        'clusterblast',
        'clusterfinder',
        'clusterhmmer',
        'inclusive',
        'knownclusterblast',
        'minimal',
        'ncbi_context',
        'needs_download',
        'pfam2go',
        'rre',
        'smcogs',
        'smcog_trees',
        'subclusterblast',
        'tfbs',
        'tigrfam',
        'transatpks_da',
        'tta',
    }

    INT_ARGS = {
        'cf_cdsnr',
        'cf_npfams',
        'from_pos',
        'rre_minlength',
        'seed',
        'to_pos',
    }

    FLOAT_ARGS = {
        'cf_threshold',
        'rre_cutoff',
    }

    DATE_ARGS = {
        'added',
        'last_changed',
    }

    LIST_ARGS = {
        'sideloads',
        'target_queues',
        'trace',
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

    STRICTNESS_LEVELS = {
        'strict',
        'relaxed',
        'loose',
        None,
    }

    SAFE_ACCESSION_CHARS = string.ascii_letters + string.digits + "._-"

    def __init__(self, db: DataBase, job_id: str) -> None:
        super(BaseJob, self).__init__(db, 'job:{}'.format(job_id))
        self._id: str = job_id

        # taxon is the first element of the ID
        self._taxon: str = self._id.split('-')[0]
        self._legacy: bool = False

        # unless this is a legacy job id
        if not self.is_valid_taxon(self._taxon) and job_id.count('-') == 4:
            self._taxon = 'bacteria'
            self._legacy = True

        # storage for properties
        self._state: str = 'created'
        self._molecule_type: str = 'nucl'
        self._genefinder: str = 'none'
        self._hmmdetection_strictness: Union[str, None] = None
        self.sideloads: list[str] = []
        self._sideload_simple: Union[str, None] = None
        self.status: str = 'pending'
        self.original_id: Union[str, None] = None

        # Regular attributes that differ from None
        self.added: datetime = now()
        self.last_changed: datetime = now()

        # Initialise list type attributes to [] instead of None
        for attr in self.LIST_ARGS:
            setattr(self, attr, [])

    # Not really async, but follow the same API as the other properties
    @property
    def job_id(self) -> str:
        return self._id

    # No setter, job_id is a read-only property

    # Not really async, but follow same API as the other properties
    @property
    def taxon(self) -> str:
        return self._taxon

    # No setter, taxon is a read-only property

    @property
    def state(self) -> str:
        if self._legacy:
            state = self.status.split(' ')[0].rstrip(':')
            if state in self.VALID_STATES:
                return state
            return "created"
        return self._state

    @state.setter
    def state(self, value: Union[str, None]) -> None:
        if value is None:
            self._legacy = True
            return

        if value not in self.VALID_STATES:
            raise ValueError('Invalid state {}'.format(value))

        self._state = value
        self.changed()

    @property
    def status(self) -> str:
        return self._status

    @status.setter
    def status(self, value: str) -> None:
        self._status = value

    @property
    def molecule_type(self) -> str:
        return self._molecule_type

    @molecule_type.setter
    def molecule_type(self, value: str) -> None:
        if value is None:
            value = 'nucl'

        if value == 'nucleotide':
            value = 'nucl'

        if value not in {'nucl', 'prot'}:
            raise ValueError('Invalid molecule_type {}'.format(value))

        self._molecule_type = value

    @property
    def genefinder(self) -> str:
        return self._genefinder

    @genefinder.setter
    def genefinder(self, value: str) -> None:
        if value not in {'prodigal', 'prodigal-m', 'glimmerhmm', 'error', 'none'}:
            if self._legacy and value in {'glimmer', 'prodigal_m'}:
                if value == 'prodigal_m':
                    value = 'prodigal-m'
            else:
                raise ValueError('Invalid genefinding method {}'.format(value))
        self._genefinder = value

    # We want to migrate from "genefinder" to "genefinding" eventually
    @property
    def genefinding(self) -> str:
        return self._genefinder

    @genefinding.setter
    def genefinding(self, value) -> None:
        self.genefinder = value

    @property
    def hmmdetection_strictness(self) -> Union[str, None]:
        return self._hmmdetection_strictness

    @hmmdetection_strictness.setter
    def hmmdetection_strictness(self, value: str) -> None:
        if value not in BaseJob.STRICTNESS_LEVELS:
            raise ValueError('Invalid strictnes level {}'.format(value))

        self._hmmdetection_strictness = value

    @property
    def sideload(self) -> Optional[str]:
        warn("Job.sideload deprecated, please use Job.sideloads list instead", DeprecationWarning)
        if self.sideloads:
            return self.sideloads[0]
        return None

    @sideload.setter
    def sideload(self, value: str) -> None:
        warn("Job.sideload deprecated, please use Job.sideloads list instead", DeprecationWarning)
        if self.sideloads:
            self.sideloads[0] = value
        else:
            self.sideloads.append(value)

    @property
    def sideload_simple(self) -> Union[str, None]:
        return self._sideload_simple

    @sideload_simple.setter
    def sideload_simple(self, value: str) -> None:
        # validate format is correct: ACCESSION:START-END
        parts = value.split(":")
        if len(parts) != 2:
            raise ValueError("Invalid sideload_simple value {}".format(value))

        acc, coords = parts
        for ch in acc:
            if ch not in self.SAFE_ACCESSION_CHARS:
                raise ValueError("Invalid sideload_simple accession {}".format(acc))

        if acc[0] == "-":
            raise ValueError("Accession can't start with '-' for sideload_simple")

        parts = coords.split("-")
        if len(parts) != 2:
            raise ValueError("Invalid sideload_simple coordinates {}".format(coords))

        for p in parts:
            for ch in p:
                if ch not in string.digits:
                    raise ValueError("Invalid sideload_simple coordinates {}".format(coords))

        self._sideload_simple = value

    @staticmethod
    def is_valid_taxon(taxon: str) -> bool:
        """
        Check if taxon string is one of 'bacterial', 'fungal' or 'plant'
        """
        if taxon not in BaseJob.VALID_TAXA:
            return False

        return True

    def changed(self) -> None:
        """Update the job's last changed timestamp"""
        self.last_changed = now()

    def to_dict(self, extra_info=False) -> dict[str, Any]:
        ret: dict[str, Any] = super(BaseJob, self).to_dict()

        if extra_info:
            ret['job_id'] = self.job_id
            ret['taxon'] = self.taxon

        return ret

    def __str__(self) -> str:
        return "Job(id: {}, state: {})".format(self._id, self.state)

    @classmethod
    def fromExisting(cls: Type[TJob], new_id: str, existing: TJob) -> TJob:
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
