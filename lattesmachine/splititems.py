import multiprocessing
import more_itertools
import logging
import pydecor
import plyvel
import json
import sys
from .jsonwalk import *
from .schema.item_keys import item_keys
from .schema.subkind import subkind_getter
from .schema.author_list import author_list_pattern
from . import settings


logger = logging.getLogger(__name__)


def fix_multiple_citation_names(authors):
    pass


def ensure_author_in_item(idcnpq, authors):
    return True


def fix_doi_url(item):
    pass


@pydecor.intercept(ValueError)
def process_item(from_year, to_year, idcnpq, kind, item):
    dados_basicos = keymatches('DADOS-BASICOS.*', item)

    # Verifica aceitabilidade do item e normaliza inconsistências comuns do Lattes

    ano = keymatches('@ANO.*', dados_basicos)
    if not ano or int(ano) < from_year or int(ano) > to_year:
        return

    authors = keymatches(author_list_pattern, item)
    fix_multiple_citation_names(authors)
    if not ensure_author_in_item(idcnpq, authors):
        return

    fix_doi_url(item)

    # Produz chave e retorna item

    seqno = keymatches('@SEQUENCIA.*', item)

    key = kind
    getter = subkind_getter.get(kind)
    subkind = getter and getter(dados_basicos)
    if subkind:
        key += ':' + subkind
    key += '/' + ano + '/' + idcnpq + '/' + seqno

    return key.encode('utf-8'), json.dumps(item).encode('utf-8')


def items_from_cv(from_year, to_year, cv):
    res = []
    cv = json.loads(cv)
    idcnpq = cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR']
    for path, items in jsoniterkeys(cv, item_keys):
        kind = path[-1]
        for item in items:
            key_item = process_item(from_year, to_year, idcnpq, kind, item)
            if key_item:
                res.append(key_item)
    return res


def splititems(cv_db, items_db, from_year, to_year, report_status=True):
    p = multiprocessing.Pool()
    for batch in more_itertools.chunked((cv for unused, cv in cv_db), settings.cv_batch_size):
        with items_db.write_batch() as wb:
            for cv_items in p.map(lambda cv: items_from_cv(from_year, to_year, cv), batch):
                for key, item in cv_items:
                    wb.put(key, item)
        if report_status:
            sys.stderr.write('#')
            sys.stderr.flush()
    if report_status:
        sys.stderr.write('\n')


def splititems_cmd(cv_db_path, items_db_path, from_year, to_year):
    if from_year <= 1900:
        logger.warn('O ano de 1900 costumava ser utilizado '
                    'como padrão quando o campo de ano não era '
                    'preenchido na Plataforma. Ele não deveria '
                    'estar incluído no intervalo escolhido (%d a %d).',
                    from_year, to_year)

    cv_db = plyvel.DB(cv_db_path)
    items_db = plyvel.DB(items_db_path, create_if_missing=True)
    splititems(cv_db, items_db, from_year, to_year)
    items_db.close()
    cv_db.close()
