import time
import traceback
import base64
import io
import zipfile
import suds
import suds.client
from . import settings


class Retry(object):
    def __init__(decorator, times=3, sleeptime=2.0):
        decorator.times = times
        decorator.sleeptime = sleeptime

    def __call__(decorator, func):
        def newFunc(*args, **kwargs):
            for unused in range(decorator.times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    lasterr = e
                    traceback.print_exc()
                    time.sleep(decorator.sleeptime)
            raise lasterr
        return newFunc


class WSCurriculo(suds.client.Client):
    def __init__(self):
        suds.client.Client.__init__(self, settings.ws_url)

    @Retry()
    def obterCV(self, idCNPq):
        b64 = self.service.getCurriculoCompactado(id=idCNPq)
        if b64 is None:
            return None
        xmlz = zipfile.ZipFile(io.BytesIO(base64.b64decode(b64)))
        xml = xmlz.read(xmlz.namelist()[0])
        return xml.decode(settings.ws_encoding, 'ignore')

    @Retry()
    def obterIdCNPq(self, cpf=None, nomeCompleto=None, dataNascimento=None):
        """ obterIdCNPq(cpf) ou obterIdCNPq(nomeCompleto, dataNascimento) """
        if cpf is not None and nomeCompleto is None and dataNascimento is None:
            return self.service.getIdentificadorCNPq(cpf=cpf,
                                                     nomeCompleto='',
                                                     dataNascimento='')
        if cpf is None and nomeCompleto is not None and dataNascimento is not None:
            return self.service.getIdentificadorCNPq(cpf='',
                                                     nomeCompleto=nomeCompleto,
                                                     dataNascimento=dataNascimento)
        raise ValueError('Passe somente cpf ou {nomeCompleto e dataNascimento}')

    @Retry()
    def obterOcorrencia(self, idCNPq):
        return self.service.getOcorrenciaCV(id=idCNPq)
