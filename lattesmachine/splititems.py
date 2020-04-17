import multiprocessing
import more_itertools
import logging
import plyvel
import json
from .jsonwalk import *
from .schema.item_keys import item_keys
from . import settings


logger = logging.getLogger(__name__)


def process_item(idcnpq, key, item, from_year, to_year):
    item['_origem'] = idcnpq
    item['_tipo'] = key
    return item


def items_from_cv(cv, from_year, to_year):
    res = []
    cv = json.loads(cv)
    idcnpq = cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR']
    for path, items in jsoniterkeys(cv, item_keys):
        key = path[-1]
        for item in items:
            if process_item(idcnpq, key, item, from_year, to_year):
                res.append(item)
    return res


def splititems(cv_db, items_db, from_year, to_year, report_status=True):
    p = multiprocessing.Pool()
    for batch in more_itertools.chunked((cv for unused, cv in cv_db), settings.cv_batch_size):
        for cv_items in p.map(lambda cv: items_from_cv(cv, from_year, to_year), batch):
            for item in cv_items:
                print(item)


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
