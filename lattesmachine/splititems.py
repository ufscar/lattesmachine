import multiprocessing
import more_itertools
import logging
import pydecor
import plyvel
import json
import sys
import re
from typing import *
from .norm import *
from .jsonwalk import *
from .nametools import AuthorSet, dist
from .schema.item_keys import item_keys
from .schema.subkind import subkind_getter
from .schema.author_list import author_list_pattern
from . import settings


logger = logging.getLogger(__name__)
_author_norm = letters_no_spaces


class CVAuthor:
    def __init__(self, cv):
        cv = cv['CURRICULO-VITAE']
        self.idcnpq = cv['@NUMERO-IDENTIFICADOR']
        self.nome_completo = cv['DADOS-GERAIS']['@NOME-COMPLETO']
        self.nomes_em_citacoes = {s.strip() for s in cv['DADOS-GERAIS']['@NOME-EM-CITACOES-BIBLIOGRAFICAS'].split(';')}
        self.nome_completo_norm = _author_norm(self.nome_completo)
        self.nomes_em_citacoes_norm = {_author_norm(s) for s in self.nomes_em_citacoes}


def authors_norm_keys(authors: List[Dict]):
    for author in authors:
        renkey(r'@ORDEM.*', '@ORDEM', author)
        renkey(r'@NOME-COMPLETO.*', '@NOME-COMPLETO', author)
        renkey(r'@NOME-PARA-CITACAO.*', '@NOME-PARA-CITACAO', author)


@pydecor.intercept(ValueError)
def ensure_authors_sorted(authors: List[Dict]):
    authors.sort(key=lambda metadatum: int(metadatum['@ORDEM']))


def fix_multiple_citation_names(metadata: List[Dict]):
    for metadatum in metadata:
        author = AuthorSet.to_author(metadatum)
        # Alguns registros possuem mais de um nome para citação na mesma entrada
        # bibliográfica (!) Nesses casos, escolhe o nome cujas iniciais são mais
        # próximas do nome completo e, como critério de desempate, o maior nome.
        if author.cn and ';' in author.cn:
            unused, unused, nome_citacao = \
                max((-dist(nome, author.fn), len(nome), nome)
                    for nome in re.split(r'\s*;\s*', author.cn))
            metadatum['@NOME-PARA-CITACAO'] = nome_citacao


@pydecor.intercept(ValueError)   # max() arg is an empty sequence
def ensure_author_in_item(cv_author: CVAuthor, metadata: List[Dict]):
    authors = AuthorSet.to_author_set(metadata)
    if cv_author.idcnpq not in (a.id for a in authors):
        # Autor proprietário do currículo não está marcado no atributo NRO-ID-CNPQ
        # Faz score dos autores que não possuem idcnpq especificado

        def compute_score(a):
            return (int(_author_norm(a.fn) == cv_author.nome_completo_norm) +
                    int(_author_norm(a.cn) in cv_author.nomes_em_citacoes_norm))

        score, idx = max((compute_score(a), i)
                         for i, a in enumerate(authors)
                         if not a.id)
        if score == 0:
            return False
        metadata[idx]['@NRO-ID-CNPQ'] = cv_author.idcnpq

    return True


def norm_fields(item):
    for path, value in jsoniter(item):
        key = path[-1]
        if key == '@DOI':
            # Às vezes o Lattes insere dentro de [...], que precisa ser removido.
            doi = re.sub(r'\]\s*$', '', value)
            # Procura uma string no formato de um DOI dentro do campo.
            # https://web.archive.org/web/20021021021703/http://www.crossref.org/01company/15doi_info.html
            # É muito comum ela estar inserida dentro de uma URL de um resolvedor de DOI.
            m = re.search(r'10\.[\d.]+/.+', doi)
            doi = m.group(0) if m else ''
            # Espaços são permitidos no DOI, mas isso não é comum, e supostamente
            # eles deveriam ser codificados como uma URL (transformados em %20).
            # DOI contendo espaço geralmente indica erro de digitação no Lattes.
            doi = re.sub(r'\s+', '', doi)
            # O DOI não é case-sensitive
            # https://web.archive.org/web/20190918153920/http://www.doi.org/10DEC99_presentation/faq.html#3.12
            doi = doi.lower()
            if doi != value:
                jsonset(item, path, doi)
        elif key.startswith('@HOME-PAGE'):
            # Às vezes o Lattes insere dentro de [...], que precisa ser removido.
            url = re.sub(r'\]\s*$', '', value)
            # O Lattes não reconhece outros protocolos e insere 'http://' na frente.
            url = re.sub(r'http://(https|ftp)://', r'\1', url)
            # Procura uma string no formato de uma URL dentro do campo.
            m = re.search(r'(https?|ftp)://.+', url)
            url = m.group(0) if m else ''
            if url != value:
                jsonset(item, path, url)
        elif key.startswith('@ISSN') or key.startswith('@ISBN'):
            # https://www.bl.uk/issn
            issn_isbn = re.sub(r'[^0-9X]', '', value.upper())
            if len(issn_isbn) not in {8, 10, 13}:
                issn_isbn = ''
            if issn_isbn != value:
                jsonset(item, path, issn_isbn)


@pydecor.intercept(ValueError)   # ano não-inteiro
def process_item(from_year, to_year, cv_author: CVAuthor, kind, item):
    dados_basicos = renkey(r'DADOS-BASICOS.*', 'DADOS-BASICOS', item)
    renkey(r'DETALHAMENTO.*', 'DETALHAMENTO', item)

    # Produção precisa ter ano, e precisa estar no intervalo solicitado
    ano = renkey(r'@ANO.*', '@ANO', dados_basicos)
    if not ano or int(ano) < from_year or int(ano) > to_year:
        return

    # Produção precisa ter título
    titulo = renkey(r'@TITULO.*|@DENOMINACAO', '@TITULO', dados_basicos)
    if not titulo:
        return
    renkey(r'@TITULO.*?-INGLES', '@TITULO-INGLES', dados_basicos)

    # Constrói chave do item
    seqno = item['@SEQUENCIA-PRODUCAO']
    key = kind
    getter = subkind_getter.get(kind)
    subkind = getter and getter(dados_basicos)
    if subkind:
        key += ':' + subkind
    key += '/' + ano + '/' + cv_author.idcnpq + '/' + seqno

    # Padroniza chaves com informações de autores
    authors = renkey(author_list_pattern, 'AUTORES', item)
    if not authors:
        logger.warn('Produção não possui autores: %s', key)
        return
    authors_norm_keys(authors)

    # Autor do CV precisa ser autor da própria produção
    ensure_authors_sorted(authors)
    fix_multiple_citation_names(authors)
    if not ensure_author_in_item(cv_author, authors):
        logger.warn('Autor não identificado na sua própria produção: %s', key)
        return

    # Normaliza campos diversos
    norm_fields(item)

    return key.encode('utf-8'), json.dumps(item).encode('utf-8')


def _splititems(from_year, to_year, cv):
    res = []
    cv = json.loads(cv)
    cv_author = CVAuthor(cv)
    for path, items in jsoniterkeys(cv, item_keys):
        kind = path[-1]
        for item in items:
            key_item = process_item(from_year, to_year, cv_author, kind, item)
            if key_item:
                res.append(key_item)
    return res


def splititems(cv_db, items_db, from_year, to_year, report_status=True):
    p = multiprocessing.Pool()
    batch_no = 1
    for batch in more_itertools.chunked((cv for unused, cv in cv_db), settings.cv_batch_size):
        with items_db.write_batch() as wb:
            for cv_items in p.map(lambda cv: _splititems(from_year, to_year, cv), batch):
                for key, item in cv_items:
                    wb.put(key, item)
        if report_status:
            sys.stderr.write('\r' + batch_no * '#')
            sys.stderr.flush()
            batch_no += 1
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
