import base64
import io
import zipfile
import suds
import suds.client
from . import settings
from retry import retry


class WSCurriculo(suds.client.Client):
    def __init__(self):
        suds.client.Client.__init__(self, settings.ws_url)

    @retry(tries=3, delay=0.2)
    def obterCV(self, idCNPq):
        b64 = self.service.getCurriculoCompactado(id=idCNPq)
        if b64 is None:
            return None
        xmlz = zipfile.ZipFile(io.BytesIO(base64.b64decode(b64)))
        xml = xmlz.read(xmlz.namelist()[0])
        return xml.decode(settings.ws_encoding, 'ignore')

    @retry(tries=3, delay=0.2)
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

    @retry(tries=3, delay=0.2)
    def obterOcorrencia(self, idCNPq):
        return self.service.getOcorrenciaCV(id=idCNPq)
