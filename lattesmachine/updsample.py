import logging
import rocksdb
import random
from .extract import extract


logger = logging.getLogger(__name__)


def cv_ids(db):
    it = db.iterkeys()
    it.seek_to_first()
    return list(it)


def updsample(db, probability):
    ids = cv_ids(db)
    sample = random.sample(ids, int(probability*len(ids)))
    logger.info('Atualizando %d CVs', len(sample))
    extract(db, [{'idcnpq': idcnpq} for idcnpq in sample])


def updsample_cmd(db_path, probability):
    db = rocksdb.DB(db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    updsample(db, probability)
    db.close()
