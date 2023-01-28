import more_itertools
import simstring
import itertools
import logging
import rocksdb
import json
import sys
import os
import re
import gc
from multiprocessing import Pool
from stringdist import levenshtein_norm
from collections import defaultdict
from tempfile import TemporaryDirectory
from disjoint_set import DisjointSet
from .authority_mix import authority_mix
from .nametools import AuthorSet
from .score import score_item
from .norm import *
from . import settings


logger = logging.getLogger(__name__)
_title_norm = no_accents


def get_item_identifiers(kind, item):
    return {
        'doi': item['DADOS-BASICOS'].get('@DOI'),

        # ISBN de trabalhos em eventos, capítulos, etc. não é um identificador
        # pois pode haver mais de um item em um mesmo livro
        'isbn': kind == 'LIVRO-PUBLICADO-OU-ORGANIZADO' and item.get('DETALHAMENTO', {}).get('@ISBN'),

        'reg': item.get('DETALHAMENTO', {}).get('REGISTRO-OU-PATENTE', {}).get('@CODIGO-DO-REGISTRO-OU-PATENTE'),
    }


def tabulate_item(tbl, cdb, kind, item_key, item):
    item = json.loads(item)

    for id_namespace, id_value in get_item_identifiers(kind, item).items():
        if id_value:
            tbl[id_namespace][id_value].add(item_key)

    title = item['DADOS-BASICOS'].get('@TITULO')
    if title:
        title = _title_norm(title)
        cdb.insert(title)
        tbl['title'][title].add(item_key)


# Variáveis globais dos workers da Pool
_tbl_title: dict = None
_items_db: rocksdb.DB = None
_cdb: simstring.reader = None
_kind: str = None


def _init_pool(paralellize, tbl_title, items_db_or_path, cdb_path, kind):
    global _tbl_title, _items_db, _cdb, _kind
    _tbl_title = tbl_title
    if paralellize:
        _items_db = rocksdb.DB(items_db_or_path, rocksdb.Options(), read_only=True)
    else:
        # Workaround para bug do python-rocksdb: não abrir mais de um db por processo
        _items_db = items_db_or_path
    _cdb = simstring.reader(cdb_path)
    _cdb.measure = settings.title_measure
    _cdb.threshold = settings.title_threshold
    _kind = kind


def _cleanup_globals():
    global _tbl_title, _items_db, _cdb, _kind
    if _cdb:
        _cdb.close()
    _tbl_title = _items_db = _cdb = _kind = None


def find_item_approx_dups(item_key, item):
    global _tbl_title, _items_db, _cdb, _kind
    item = json.loads(item)

    title = item['DADOS-BASICOS'].get('@TITULO')
    if not title:
        return

    title = _title_norm(title)
    similar_titles = {similar for similar in _cdb.retrieve(title)
                      # Remove publicações que pertençam à mesma série, mas que
                      # tenham uma numeração diferente no final do título
                      if not differs_only_by_appended_num(similar, title)}
    # Remove o título exatamente igual ao está sendo buscado,
    # pois já foi previamente agrupado em tbl
    similar_titles.remove(title)

    if len(similar_titles) == 0:
        # se não tiver nenhum outro, nada a fazer
        return

    # Rankeia os títulos similares encontrados de acordo com a distância de Levenshtein
    similar_titles = sorted((levenshtein_norm(title, similar), similar) for similar in similar_titles)

    # Poda similares pelo limiar configurado de distância de Levenshtein
    similar_titles = [(dist, similar_title) for dist, similar_title in similar_titles if dist <= settings.title_threshold_levnorm]

    if len(similar_titles) == 0:
        # Nada a fazer se não houver similares
        return

    # Identificadores únicos do item atual
    item_ids = get_item_identifiers(_kind, item)

    # CVs visitados
    visited_cvs = {item_idcnpq(item_key), }

    # Candidatos a duplicatas (depois ainda precisa verificar a lista de autores)
    candidates = []

    for dist, similar_title in similar_titles:
        clash = False
        new_candidates = []
        for similar_key in _tbl_title[similar_title]:
            # Evita mesclar duas publicações de um mesmo CV
            similar_idcnpq = item_idcnpq(similar_key)
            if similar_idcnpq in visited_cvs:
                clash = True
                break
            visited_cvs.add(similar_idcnpq)
            # Nenhum dos itens com o título similar pode ter um identificar único
            # (e.g. DOI) diferente do identificador deste item
            similar_item = json.loads(_items_db.get(similar_key))
            new_candidates.append((similar_key, similar_item))
            similar_ids = get_item_identifiers(_kind, similar_item)
            for id_namespace, id_value in item_ids.items():
                similar_id_value = similar_ids[id_namespace]
                if id_value and similar_id_value and id_value != similar_id_value:
                    clash = True
                    break
            if clash:
                break
        if clash:
            # Se houver um conflito, analisar itens com título ainda mais
            # distante geraria ambiguidade (venceria o último item a
            # executar esta função)
            break
        else:
            candidates.extend(new_candidates)

    # Verifica se o conjunto de autores dos candidatos possui similaridade
    # o suficiente para considerar como o mesmo item
    author_set = AuthorSet.to_author_set(item['AUTORES'])
    for similar_key, similar_item in candidates:
        similar_authors = AuthorSet.to_author_set(similar_item['AUTORES'])
        if author_set.compare(similar_authors) <= settings.author_threshold:
            # Considera como duplicata
            yield (item_key, similar_key)


def merge_items(items_db, item_key_sets):
    wb = rocksdb.WriteBatch()
    for item_key_set in item_key_sets:
        if len(item_key_set) == 1:
            # se só tem um item, nada a fazer
            continue
        # Obtém items do db
        items = [(item_key, json.loads(items_db.get(item_key))) for item_key in item_key_set]
        # Pontua items
        items = [(item_key, item, score_item(item)) for item_key, item in items]
        best_item_key, best_item, unused = max(items, key=lambda t: t[2])
        # Insere chaves indicando duplicatas
        best_item['@@dups'] = [item_key.decode('utf-8') for item_key, unused, unused in items if item_key != best_item_key]
        for item_key, item, unused in items:
            if item_key != best_item_key:
                item['@@dup_of'] = best_item_key.decode('utf-8')
        # Junta id de autoridade de autores dos outros itens
        authority_mix(best_item, ((item_key, item) for item_key, item, unused in items if item_key != best_item_key))
        # Salva modificações no db
        for item_key, item, unused in items:
            wb.put(item_key, json.dumps(item, ensure_ascii=False).encode('utf-8'))
    items_db.write(wb)


def differs_only_by_appended_num(a, b):
    """Retorna True se os títulos `a` e `b` diferirem apenas de um numeral no final

    Exemplos:
    - 'Como casar strings de títulos - 1.' e 'Como casar strings de títulos 2'
    - 'Numerais romanos devem ser lembrados; I' e 'Numerais romanos devem ser lembrados, II.'
    """
    def preproc(s): return re.split(r'\s+', no_punct(s).strip())

    a, b = preproc(a), preproc(b)

    # http://stackoverflow.com/a/267405
    def is_num(s): return s and (s.isdigit() or re.match(r'^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$', s.upper()))

    return a[:-1] == b[:-1] and a[-1] != b[-1] and is_num(a[-1]) and is_num(b[-1])


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


def piece_key_ignore_year(k: bytes) -> bytes:
    # tipo
    return re.search(br'^[^/]+', k).group(0)


def item_kind(k: bytes) -> str:
    # tipo (sem subtipo)
    return re.search(br'^[^/:]+', k).group(0).decode('utf-8')


def item_idcnpq(k: bytes) -> str:
    return k.split(b'/', 3)[2]


def dedup_cmd(items_db_path, ignore_year=False, report_status=True):
    items_db = rocksdb.DB(items_db_path, rocksdb.Options(compression=rocksdb.CompressionType.lz4_compression))

    # Coleta primeiro elemento de cada grupo (por tipo/ano) de itens
    logger.info('Determinando grupos para desduplicação')
    it = items_db.iterkeys()
    it.seek_to_first()
    if ignore_year:
        delim = list(first_each_piece(it, lambda k1, k2: piece_key_ignore_year(k1) != piece_key_ignore_year(k2)))
    else:
        delim = list(first_each_piece(it, lambda k1, k2: piece_key(k1) != piece_key(k2)))
    delim.append(None)  # o último grupo deve ir até o final do db

    tbl = defaultdict(lambda: defaultdict(lambda: set()))  # tbl[namespace][lookup_value] = {item_keys}

    # Percorre os grupos de itens
    for group_no, (start, stop) in enumerate(more_itertools.windowed(delim, 2)):
        kind = item_kind(start)
        dups = DisjointSet()
        logger.info('(%d/%d) %s', group_no + 1, len(delim) - 1,
                    piece_key(start).decode('utf-8'))

        if group_no % 64 == 0:
            # Mesmo usando context manager, o Pool não libera todos os recursos
            # adequadamente. É necessário forçar o GC de tempos em tempos para
            # não cair no limite de arquivos abertos.
            gc.collect()

        with TemporaryDirectory() as tmpdir:
            cdb_path = os.path.join(tmpdir, 'cdb')

            # Passo 1: montagem das tabelas de identificadores
            tbl.clear()
            batch_no = 1
            cdb = simstring.writer(cdb_path, n=settings.title_ngram, be=settings.title_be)
            it = items_db.iteritems()
            it.seek(start)
            for batch in more_itertools.chunked(itertools.takewhile(lambda args: args[0] != stop, it), settings.item_batch_size):
                for item_key, item in batch:
                    tabulate_item(tbl, cdb, kind, item_key, item)
                if report_status:
                    sys.stderr.write('#')
                    sys.stderr.flush()
                batch_no += 1
            if report_status:
                sys.stderr.write('\n')
            cdb.close()

            for id_namespace in sorted(tbl.keys()):  # sorted() para mostrar stats sempre na mesma ordem
                num_unions = 0
                for dup_keys in tbl[id_namespace].values():
                    if len(dup_keys) >= 2:
                        for a, b in more_itertools.windowed(dup_keys, 2):
                            num_unions += dups.find(a) != dups.find(b)
                            dups.union(a, b)
                if num_unions > 0:
                    logger.info('%s: %d uniões', id_namespace, num_unions)

            # Só paraleliza a próxima etapa se compensar o overhead de subir os novos processos
            parallelize = batch_no > settings.parallel_batch_threshold

            # Passo 2: identificação de duplicatas aproximadas
            batch_no = 1
            num_unions = 0
            if parallelize:
                p = Pool(processes=settings.processing_jobs, initializer=_init_pool, initargs=(parallelize, tbl['title'], items_db_path, cdb_path, kind))
                map_func = p.map
            else:
                _init_pool(parallelize, tbl['title'], items_db, cdb_path, kind)
                map_func = map
            it = items_db.iteritems()
            it.seek(start)
            for batch in more_itertools.chunked(itertools.takewhile(lambda args: args[0] != stop, it), settings.item_batch_size):
                for dup_keys in map_func(lambda args: list(find_item_approx_dups(*args)), batch):
                    for item_key, dup_key in dup_keys:
                        num_unions += dups.find(item_key) != dups.find(dup_key)
                        dups.union(item_key, dup_key)
                if report_status:
                    sys.stderr.write('#')
                    sys.stderr.flush()
                batch_no += 1
            if parallelize:
                p.close()
            else:
                _cleanup_globals()
            if report_status:
                sys.stderr.write('\n')
            if num_unions > 0:
                logger.info('simstring: %d uniões', num_unions)

        batch_no = 1
        for batch in more_itertools.chunked(dups.itersets(), settings.item_batch_size):
            merge_items(items_db, batch)
            if report_status:
                sys.stderr.write('#')
                sys.stderr.flush()
            batch_no += 1
        if report_status:
            sys.stderr.write('\n')

    items_db.close()
