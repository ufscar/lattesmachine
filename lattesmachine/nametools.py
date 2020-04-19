import re
from .norm import *
from collections import namedtuple
from stringdist import levenshtein, levenshtein_norm


_nobiliary_particles = {'de', 'dit', 'la', 'von', 'af', 'der', 'und', 'zu', 'of'}
_nobiliary_regex = re.compile('|'.join(r'\b%s\b' % word for word in _nobiliary_particles))


def name_reorder(name):
    """ Reordena "sobrenome, nome" -> "nome sobrenome" """
    if ',' in name:
        return ' '.join(reversed(name.split(',', 2)))
    return name


def initials(name):
    """ Obtém as iniciais de um nome normalizadas """
    name = no_accents(name)
    # Retira partículas e reordena
    name = name_reorder(_nobiliary_regex.sub(' ', name))
    # Separa nomes por espaços ou pontos, e junta apenas as iniciais
    name = ''.join([word[:1] for word in re.split(r'[\s.]+', name)])
    return re.sub(r'[^a-z]', '', name)


def dist_initials(a, b):
    """ Distância de edição entre as inicias dos nomes `a` e `b` """
    return levenshtein(initials(a), initials(b))


Author = namedtuple('Author', ['id', 'cn', 'fn'])


class AuthorSet(list):
    def compare(self, other):
        """
        Comparação heurística, gulosa e tolerante entre conjuntos de autores

        Retorna uma distância normalizada. Quanto menor a distância,
        mais similares os conjuntos.
        """
        # Encontra IDs de autoridade que estejam em ambos os conjuntos
        a, b = (set(x.id for x in xs) for xs in (self, other))
        commonIds = a.intersection(b) - {None, }
        # Tenta uma comparação entre nomes usando cada um dos campos
        # de nome (cn - nome em citações, fn - nome completo)
        results = []
        for f in (lambda x: x.cn, lambda x: x.fn):
            # Obtém duas listas de nomes, excluindo os que possuem IDs
            # de autoridade em comum
            a, b = ([letters_spaces(name_reorder(f(x)))
                     for x in xs if x.id not in commonIds]
                    for xs in (self, other))
            results.append(self._compare_names(a, b))
        return min(results) / min(len(self), len(other))

    @staticmethod
    def _compare_names(a, b):
        # Garante que não existem nomes vazios nos conjuntos
        a = [x for x in a if len(x) > 0]
        b = [y for y in b if len(y) > 0]
        # Garante que o primeiro conjunto seja o menor
        if len(a) > len(b):
            a, b = b, a
        # Se já o primeiro conjunto for vazio (pode ter sido zerado
        # via ID de autoridade), considera distância zero
        if len(a) == 0:
            return 0.
        # Para cada nome do primeiro conjunto, retira os nomes mais
        # similares do segundo conjunto (algoritmo guloso)
        total_dist = 0.
        for x in a:
            min_dist, idx_b = min((levenshtein_norm(x, y), i) for i, y in enumerate(b))
            y = b.pop(idx_b)
            total_dist += min_dist
        return total_dist

    @staticmethod
    def to_author(metadatum):
        return Author(fn=metadatum.get('@NOME-COMPLETO'),
                      cn=metadatum.get('@NOME-PARA-CITACAO'),
                      id=metadatum.get('@NRO-ID-CNPQ'))

    @staticmethod
    def to_author_set(metadata):
        return AuthorSet([AuthorSet.to_author(x) for x in metadata])
