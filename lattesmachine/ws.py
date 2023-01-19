import io
import zipfile
import zeep
from . import settings
from retry import retry


class WSCurriculo(zeep.Client):
    def __init__(self):
        zeep.Client.__init__(self, settings.ws_url)

    @retry(tries=3, delay=1)
    def obterCV(self, idCNPq):
        b = self.service.getCurriculoCompactado(id=idCNPq)
        if b is None:
            return None
        xmlz = zipfile.ZipFile(io.BytesIO(b))
        xml = xmlz.read(xmlz.namelist()[0])
        return xml.decode(settings.ws_encoding, 'ignore')

    @retry(tries=3, delay=1)
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

    @retry(tries=3, delay=1)
    def obterOcorrencia(self, idCNPq):
        return self.service.getOcorrenciaCV(id=idCNPq)
