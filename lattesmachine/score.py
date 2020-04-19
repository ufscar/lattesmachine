from .nametools import AuthorSet
from .jsonwalk import *


def score_item(item):
    score = 0

    # Pontuação por ter um DOI definido nos metados
    if item['DADOS-BASICOS'].get('@DOI'):
        score += 500

    # Pontuação para cada autor com ID de autoridade definido
    author_set = AuthorSet.to_author_set(item['AUTORES'])
    for author in author_set:
        if author.id:
            score += 50

    # Pontuação para flag de relevância (trabalho em destaque no CV)
    if item['DADOS-BASICOS'].get('@FLAG-RELEVANCIA') == 'SIM':
        score += 10

    # Pontuação para cada tipo diferente de metadado encontrado no registro
    for path, value in jsoniter(item):
        # convenção: chaves começando em @@ são geradas pela lattesmachine
        if value and not any(isinstance(p, str) and p.startswith('@@') for p in path):
            score += 1

    return score
