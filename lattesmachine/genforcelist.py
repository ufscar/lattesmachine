# Este utilitário gera um conjunto a ser passado para a opção
# force_list do xmltodict, fazendo com que chaves que contenham
# uma lista em pelo menos um dos CVs sejam forçados a conter
# uma lista em todos eles, uniformizando assim o JSON dos CVs.

import more_itertools
import rocksdb
import json
import sys
from multiprocessing import Pool
from .jsonwalk import *
from . import settings


def get_keys_containing_list(cv):
    cv = json.loads(cv)
    has_list = set()
    for path, value in jsoniter(cv):
        for i, p in enumerate(path[:-1]):
            if isinstance(path[i + 1], int):
                has_list.add(p)
    return has_list


def genforcelist(db, report_status=True):
    with Pool(processes=settings.processing_jobs) as p:
        keys_containing_list = set()
        it = db.itervalues()
        it.seek_to_first()
        for batch in more_itertools.chunked(it, settings.cv_batch_size):
            for has_list in p.map(get_keys_containing_list, batch):
                keys_containing_list.update(has_list)
            if report_status:
                sys.stderr.write('#')
                sys.stderr.flush()
        if report_status:
            sys.stderr.write('\n')
        return keys_containing_list


def genforcelist_cmd(db_path):
    db = rocksdb.DB(db_path, rocksdb.Options(), read_only=True)
    force_list = genforcelist(db)
    print('force_list = {%s}' % ', '.join(repr(x) for x in sorted(force_list)))
    db.close()
