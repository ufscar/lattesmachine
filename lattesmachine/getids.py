import more_itertools
import logging
import rocksdb
import json
from multiprocessing import Pool
from .extract import extract
from .jsonwalk import jsoniterkeys
from . import settings


logger = logging.getLogger(__name__)


def cv_ids(db):
    it = db.iterkeys()
    it.seek_to_first()
    for cv_id in it:
        yield cv_id.decode('utf-8')


def _proc_cv(cv):
    cv = json.loads(cv)
    cv_ref_ids = set()
    for unused, value in jsoniterkeys(cv, {'@NRO-ID-CNPQ', }):
        if value:
            cv_ref_ids.add(value)
    return cv_ref_ids


def unresolved_ids(db):
    resolved_ids = set(cv_ids(db))
    ref_ids = set()
    with Pool(processes=settings.processing_jobs) as p:
        it = db.itervalues()
        it.seek_to_first()
        for batch in more_itertools.chunked(it, settings.cv_batch_size):
            for cv_ref_ids in p.map(_proc_cv, batch):
                ref_ids.update(cv_ref_ids - resolved_ids)
    return ref_ids


def getids(db, kind):
    if kind == 'resolved':
        f = cv_ids
    elif kind == 'unresolved':
        f = unresolved_ids
    else:
        raise ValueError(f'Unknown kind: {kind}')
    for cv in f(db):
        print(cv)


def getids_cmd(db_path, kind):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    getids(db, kind)
    db.close()
