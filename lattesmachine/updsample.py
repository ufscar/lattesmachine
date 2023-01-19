import logging
import rocksdb
import random
from .extract import extract


logger = logging.getLogger(__name__)


def cv_ids(db_path):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    it = db.iterkeys()
    it.seek_to_first()
    res = list(it)
    db.close()
    return res


def updsample_cmd(db_path, probability):
    ids = cv_ids(db_path)
    sample = random.sample(ids, int(probability*len(ids)))
    logger.info('Atualizando %d CVs', len(sample))

    db = rocksdb.DB(db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))
    extract(db, [{'idcnpq': idcnpq} for idcnpq in sample])
    db.close()
