import logging
import rocksdb
import random
from .extract import extract


logger = logging.getLogger(__name__)


def cv_sample(db_path, probability):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    it = db.iterkeys()
    it.seek_to_first()
    ids = list(it)
    db.close()
    return random.sample(ids, int(probability*len(ids)))


def updsample_cmd(db_path, probability):
    sample = cv_sample(db_path, probability)
    logger.info('Atualizando %d CVs', len(sample))

    db = rocksdb.DB(db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    extract(db, [{'idcnpq': idcnpq} for idcnpq in sample])
    db.close()
