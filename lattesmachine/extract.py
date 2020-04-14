import logging
import multiprocessing
import traceback
import xmltodict
import warnings
from bs4 import BeautifulSoup
from . import settings
from .ws import WSCurriculo


logger = logging.getLogger(__name__)

# Suprime warning do BeautifulSoup quando processa strings contendo urls.
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
                           dict_constructor=dict)


def extract_person(person):
    ws = WSCurriculo()

    if 'idcnpq' in person:
        idcnpq = person['idcnpq']
    elif 'cpf' in person:
        idcnpq = ws.obterIdCNPq(cpf=person['cpf'])
    elif 'nomeCompleto' in person and 'dataNascimento' in person:
        idcnpq = ws.obterIdCNPq(nomeCompleto=person['nomeCompleto'],
                                dataNascimento=person['dataNascimento'])
    else:
        logger.error('Informações da pessoa %r não permitem extração' % person)
        return

    try:
        xmlcv = ws.obterCV(idcnpq)
    except:
        ocorrencia = None
        try:
            ocorrencia = ws.obterOcorrencia(idcnpq)
        except:
            traceback.print_exc()
        logger.error('Impossível obter CV do idcnpq %s: %r', idcnpq, ocorrencia)
        return

    cv = cvtodict(xmlcv)
    # Em alguns casos, a Plataforma Lattes não completa esse campo corretamente.
    # Preenche para evitar que o idcnpq de cada CV seja perdido.
    cv['CURRICULO-VITAE']['@NUMERO-IDENTIFICADOR'] = idcnpq

    return cv


def extract(people):
    p = multiprocessing.Pool(processes=settings.extract_jobs)
    return p.map(extract_person, people)
