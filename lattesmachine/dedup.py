import more_itertools
import simstring
import logging
import plyvel
import json
import sys
import os
import re
from stringdist import levenshtein
from collections import defaultdict
from tempfile import TemporaryDirectory
from disjoint_set import DisjointSet
from .nametools import AuthorSet
from .norm import *
from . import settings


logger = logging.getLogger(__name__)


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
        title = no_accents(title)
        cdb.insert(title)
        tbl['title'][title].add(item_key)


def find_item_approx_dups(tbl, items_db, cdb, kind, item_key, item):
    item = json.loads(item)

    title = item['DADOS-BASICOS'].get('@TITULO')
    if not title:
        return

    title = no_accents(title)
    similar_titles = {similar for similar in cdb.retrieve(title)
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
    similar_titles = sorted((levenshtein(title, similar), similar) for similar in similar_titles)

    # Identificadores únicos do item atual
    item_ids = get_item_identifiers(kind, item)

    # Candidatos a duplicatas (depois ainda precisa verificar a lista de autores)
    candidates = []

    for dist, similar_title in similar_titles:
        clash = False
        new_candidates = []
        # Nenhum dos itens com o título similar pode ter um identificar único
        # (e.g. DOI) diferente do identificador deste item
        for similar_key in tbl['title'][similar_title]:
            similar_item = json.loads(items_db.get(similar_key))
            new_candidates.append((similar_key, similar_item))
            similar_ids = get_item_identifiers(kind, similar_item)
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
            yield similar_key


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


def piece_kind(k: bytes) -> str:
    # tipo (sem subtipo)
    return re.search(br'^[^/:]+', k).group(0).decode('utf-8')


def dedup(items_db, report_status=True):
    # Coleta primeiro elemento de cada grupo (por tipo/ano) de itens
    logger.info('Determinando grupos para desduplicação')
    delim = list(first_each_piece((k for k, unused in items_db),
                                  lambda k1, k2: piece_key(k1) != piece_key(k2)))
    delim.append(None)  # o último grupo deve ir até o final do db

    tbl = defaultdict(lambda: defaultdict(lambda: set()))  # tbl[namespace][lookup_value] = {item_keys}
    dups = DisjointSet()

    # Percorre os grupos de itens
    for group_no, (start, stop) in enumerate(more_itertools.windowed(delim, 2)):
        kind = piece_kind(start)
        logger.info('(%d/%d) %s', group_no + 1, len(delim) - 1,
                    piece_key(start).decode('utf-8'))

        with TemporaryDirectory() as tmpdir:
            cdb_path = os.path.join(tmpdir, 'cdb')

            # Passo 1: montagem das tabelas de identificadores
            tbl.clear()
            batch_no = 1
            cdb = simstring.writer(cdb_path, n=settings.title_ngram, be=settings.title_be)
            for batch in more_itertools.chunked(items_db.iterator(start=start, stop=stop), settings.item_batch_size):
                for item_key, item in batch:
                    tabulate_item(tbl, cdb, kind, item_key, item)
                if report_status:
                    sys.stderr.write('\r' + batch_no * '#')
                    sys.stderr.flush()
                batch_no += 1
            if report_status:
                sys.stderr.write('\n')
            cdb.close()

            for id_namespace, subtbl in tbl.items():
                num_unions = 0
                for dup_keys in subtbl.values():
                    if len(dup_keys) >= 2:
                        for a, b in more_itertools.windowed(dup_keys, 2):
                            num_unions += dups.find(a) != dups.find(b)
                            dups.union(a, b)
                if num_unions > 0:
                    logger.info('%s: %d uniões', id_namespace, num_unions)

            # Passo 2: identificação de duplicatas aproximadas
            batch_no = 1
            num_unions = 0
            cdb = simstring.reader(cdb_path)
            cdb.measure = settings.title_measure
            cdb.threshold = settings.title_threshold
            for batch in more_itertools.chunked(items_db.iterator(start=start, stop=stop), settings.item_batch_size):
                for item_key, item in batch:
                    for dup_key in find_item_approx_dups(tbl, items_db, cdb, kind, item_key, item):
                        num_unions += dups.find(item_key) != dups.find(dup_key)
                        dups.union(item_key, dup_key)
                if report_status:
                    sys.stderr.write('\r' + batch_no * '#')
                    sys.stderr.flush()
                batch_no += 1
            if report_status:
                sys.stderr.write('\n')
            if num_unions > 0:
                logger.info('simstring: %d uniões', num_unions)
            cdb.close()


def dedup_cmd(items_db_path):
    items_db = plyvel.DB(items_db_path, create_if_missing=True)
    dedup(items_db)
    items_db.close()

