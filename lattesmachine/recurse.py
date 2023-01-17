import more_itertools
import logging
import rocksdb
import json
from multiprocessing import Pool
from .extract import extract
from .jsonwalk import jsoniterkeys
from . import settings


logger = logging.getLogger(__name__)


def _proc_cv(cv):
    cv = json.loads(cv)
    cv_id = cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR']
    cv_ref_ids = set()
    for unused, value in jsoniterkeys(cv, {'@NRO-ID-CNPQ', }):
        if value:
            cv_ref_ids.add(value)
    return cv_id, cv_ref_ids


def unresolved_ids(db):
    cv_ids = set()
    ref_ids = set()
    with Pool() as p:
        it = db.itervalues()
        it.seek_to_first()
        for batch in more_itertools.chunked(it, settings.cv_batch_size):
            for cv_id, cv_ref_ids in p.map(_proc_cv, batch):
                cv_ids.add(cv_id)
                ref_ids.update(cv_ref_ids)
    return ref_ids - cv_ids


def recurse(db):
    already_tried = set()
    while True:
        missing_cvs = unresolved_ids(db) - already_tried
        logger.info('%d CVs faltantes', len(missing_cvs))
        if len(missing_cvs) == 0:
            break
        extract(db, [{'idcnpq': idcnpq} for idcnpq in missing_cvs])
        already_tried.update(missing_cvs)


def recurse_cmd(db_path):
    db = rocksdb.DB(db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    recurse(db)
    db.close()
