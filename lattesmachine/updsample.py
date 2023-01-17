import more_itertools
import logging
import rocksdb
import random
import json
from multiprocessing import Pool
from .extract import extract
from .jsonwalk import jsoniterkeys
from . import settings


logger = logging.getLogger(__name__)


def _proc_cv(cv):
    cv = json.loads(cv)
    return cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR']


def cv_ids(db):
    ids = []
    with Pool() as p:
        it = db.itervalues()
        it.seek_to_first()
        for batch in more_itertools.chunked(it, settings.cv_batch_size):
            ids.extend(p.map(_proc_cv, batch))
    return ids


def updsample(db, probability):
    ids = cv_ids(db)
    sample = random.sample(ids, probability*len(ids))
    logger.info('Atualizando %d CVs', len(sample))
    extract(db, ({'idcnpq': idcnpq} for idcnpq in sample))


def updsample_cmd(db_path, probability):
    db = rocksdb.DB(db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    updsample(db, probability)
    db.close()
