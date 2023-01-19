import logging
import rocksdb
import random
from .extract import extract


logger = logging.getLogger(__name__)


def samplekeys(db, probability):
    it = db.iterkeys()
    it.seek_to_first()
    ids = list(it)
    return random.sample(ids, int(probability*len(ids)))


def samplekeys_cmd(db_path, probability):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    for k in samplekeys(db, probability):
        print(k.decode('utf-8'))
    db.close()
