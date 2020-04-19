import more_itertools
import simstring
import logging
import plyvel
import json
import sys
import os
import re
from tempfile import TemporaryDirectory
from multiprocessing import Pool
from .norm import *
from . import settings


logger = logging.getLogger(__name__)


def write_item_to_temp_db(wb, cdb, kind, item_key, item):
    item = json.loads(item)

    titulo = item['DADOS-BASICOS'].get('@TITULO')
    if titulo:
        titulo = no_accents(titulo)
        cdb.insert(titulo)
        wb.put(b'title/' + titulo.encode('utf-8'), item_key)

    identifiers = {
        b'doi/': item['DADOS-BASICOS'].get('@DOI'),

        # ISBN de trabalhos em eventos, capítulos, etc. não é um identificador
        # pois pode haver mais de um item em um mesmo livro
        b'isbn/': kind == 'LIVRO-PUBLICADO-OU-ORGANIZADO' and item.get('DETALHAMENTO', {}).get('@ISBN'),

        b'reg/': item.get('DETALHAMENTO', {}).get('REGISTRO-OU-PATENTE', {}).get('@CODIGO-DO-REGISTRO-OU-PATENTE'),
    }

    for id_prefix, id_value in identifiers.items():
        if id_value:
            wb.put(id_prefix + id_value.encode('utf-8'), item_key)


def find_item_dups(wb, ldb, cdb, kind, item_key, item):
    ldb.get(b'doi/')


def first_each_piece(iterable, pred):
    """Similar ao more_itertools.split_when, mas devolve
    somente o primeiro elemento de cada pedaço.

    >>> list(first_each_piece([1, 2, 3, 3, 2, 5, 2, 4, 2], lambda x, y: x > y))
    [1, 2, 2, 2]
    """
    it = iter(iterable)
    try:
        cur_item = next(it)
    except StopIteration:
        return
    yield cur_item
    for next_item in it:
        if pred(cur_item, next_item):
            yield next_item
        cur_item = next_item


def piece_key(k: bytes) -> bytes:
    # tipo/ano
    return re.search(br'^[^/]+/[^/]+', k).group(0)


def piece_kind(k: bytes) -> str:
    # tipo (sem subtipo)
    return re.search(br'^[^/:]+', k).group(0).decode('utf-8')


def dedup(items_db, report_status=True):
    # Coleta primeiro elemento de cada grupo (por tipo/ano) de itens
    logger.info('Determinando grupos para desduplicação')
    delim = list(first_each_piece((k for k, unused in items_db),
                                  lambda k1, k2: piece_key(k1) != piece_key(k2)))
    delim.append(None)  # o último grupo deve ir até o final do db

    # Percorre os grupos de itens
    for group_no, (start, stop) in enumerate(more_itertools.windowed(delim, 2)):
        kind = piece_kind(start)
        logger.info('(%d/%d) %s', group_no + 1, len(delim) - 1,
                    piece_key(start).decode('utf-8'))

        with TemporaryDirectory() as tmpdir:
            ldb_path = os.path.join(tmpdir, 'ldb')
            cdb_path = os.path.join(tmpdir, 'cdb')

            # Passo 1: montagem dos dbs temporários
            batch_no = 1
            ldb = plyvel.DB(ldb_path, create_if_missing=True)
            cdb = simstring.writer(cdb_path, n=settings.title_ngram, be=settings.title_be)
            for batch in more_itertools.chunked(items_db.iterator(start=start, stop=stop), settings.item_batch_size):
                with ldb.write_batch() as wb:
                    for item_key, item in batch:
                        write_item_to_temp_db(wb, cdb, kind, item_key, item)
                if report_status:
                    sys.stderr.write('\r' + batch_no * '#')
                    sys.stderr.flush()
                    batch_no += 1
            if report_status:
                sys.stderr.write('\n')
            cdb.close()

            # Passo 2: identificação das duplicatas
            batch_no = 1
            cdb = simstring.reader(cdb_path)
            cdb.measure = settings.title_measure
            cdb.threshold = settings.title_threshold
            for batch in more_itertools.chunked(items_db.iterator(start=start, stop=stop), settings.item_batch_size):
                with ldb.write_batch() as wb:
                    for item_key, item in batch:
                        find_item_dups(wb, ldb, cdb, kind, item_key, item)
                if report_status:
                    sys.stderr.write('\r' + batch_no * '#')
                    sys.stderr.flush()
                    batch_no += 1
            if report_status:
                sys.stderr.write('\n')
            cdb.close()
            ldb.close()


def dedup_cmd(items_db_path):
    items_db = plyvel.DB(items_db_path, create_if_missing=True)
    dedup(items_db)
    items_db.close()

