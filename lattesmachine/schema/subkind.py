def _tipo(dados_basicos):
    return dados_basicos['@TIPO']


def _natureza(dados_basicos):
    return dados_basicos['@NATUREZA']


subkind_getter = {
    'LIVRO-PUBLICADO-OU-ORGANIZADO': _tipo,
    'PROCESSOS-OU-TECNICAS': _natureza,
    'TRABALHO-TECNICO': _natureza,
    'EDITORACAO': _natureza,
    'MUSICA': _natureza,
    'OBRA-DE-ARTES-VISUAIS': _natureza,
    'APRESENTACAO-DE-OBRA-ARTISTICA': _natureza,
    'SONOPLASTIA': _natureza,
}
