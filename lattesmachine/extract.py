import re
import json
import logging
import warnings
import plyvel
import xmltodict
import more_itertools
import pydecor
from multiprocessing import Pool
from bs4 import BeautifulSoup
from .schema.force_list import force_list
from .ws import WSCurriculo
from . import settings


logger = logging.getLogger(__name__)

# Suprime warning do BeautifulSoup quando processa strings contendo URLs
warnings.filterwarnings('ignore', category=UserWarning, module='bs4')


def _postproc_html(path, key, value):
    # O XML do CV Lattes codifica entities XML duas vezes porque o contéudo dos campos
    # é, na verdade, HTML. Dessa forma, um caractere `&`, por exemplo, vira `&amp;amp;`.
    # Além disso, em alguns casos os campos chegam a conter tags, por exemplo `<b>`.
    try:
        if isinstance(value, str) and value != '':
            # 'html.parser' é mais rápido que 'lxml', ao menos no PyPy
            value = BeautifulSoup(value, 'html.parser').get_text()\
                .replace(u'\xa0', u' ')  # converte '&nbsp;' para espaço comum
    except:
        pass   # HTML inválido, usa o valor original
    return key, value


def cvtodict(xmlcv):
    return xmltodict.parse(xmlcv,
                           postprocessor=_postproc_html,
                           dict_constructor=dict,
                           force_list=force_list)


@pydecor.intercept()
def _extract(person):
    ws = WSCurriculo()

    if person.get('idcnpq'):
        idcnpq = person.get('idcnpq')
    elif person.get('cpf'):
        idcnpq = ws.obterIdCNPq(cpf=person.get('cpf'))
    elif person.get('nomeCompleto') and person.get('dataNascimento'):
        idcnpq = ws.obterIdCNPq(nomeCompleto=person.get('nomeCompleto'),
                                dataNascimento=person.get('dataNascimento'))
    else:
        logger.error('Informações da pessoa %r não permitem extração', person)
        return None

    if idcnpq is None:
        return None

    logger.info('Obtendo CV de %s', idcnpq)

    try:
        xmlcv = ws.obterCV(idcnpq)
    except:
        ocorrencia = None
        try:
            ocorrencia = ws.obterOcorrencia(idcnpq)
        except:
            pass
        logger.error('Impossível obter CV de %s: %r', idcnpq, ocorrencia)
        return None

    cv = cvtodict(xmlcv)
    # Em alguns casos, a Plataforma Lattes não preenche esse campo corretamente.
    cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR'] = idcnpq

    return idcnpq.encode('utf-8'), json.dumps(cv).encode('utf-8')


def extract(db, people, report_status=True):
    with Pool(processes=settings.extract_jobs) as p:
        done = 0
        for batch in more_itertools.chunked(people, settings.cv_batch_size):
            with db.write_batch() as wb:
                for res in p.map(_extract, batch):
                    if res:
                        wb.put(*res)
            done += len(batch)
            if report_status:
                logger.info('Concluído: %.1f%%', 100 * done / len(people))


def extract_cmd(db_path, people_file):
    people = []

    for line in people_file:
        line = line.strip()
        person = {}
        if ';' in line:
            person['nomeCompleto'], person['dataNascimento'] = line.split(';')
        else:
            numbers = re.sub(r'[^\d]', '', line)
            if len(numbers) == 11:
                person['cpf'] = numbers
            elif len(numbers) == 16:
                person['idcnpq'] = numbers
            else:
                logger.error('Tipo de entrada não reconhecido para pessoa %r', line)
                continue
        people.append(person)

    db = plyvel.DB(db_path, create_if_missing=True)
    extract(db, people)
    db.close()
