import itertools
from stringdist import levenshtein
from .nametools import AuthorSet
from .norm import *


def authority_mix(main_item, other_key_item_pais):
    author_set = AuthorSet.to_author_set(main_item['AUTORES'])
    already_present = {author.id for author in author_set if author.id}
    author_set = list(enumerate(author_set))  # guarda o índice original
    # Algoritmo guloso (dá prioridade para os autores na ordem de busca)
    # A ordem de busca é primeiro autor, último autor, demais autores
    for idx, author in itertools.chain(author_set[:1], author_set[-1:] if len(author_set) > 1 else [], author_set[1:-1]):
        if not author.id:
            # Se não estiver definido ID de autoridade para este autor, procura
            # o ID do nome de autor mais próximo presente nas duplicatas
            new_id: str = None
            id_from: bytes = b''
            try:
                unused, new_id, id_from = min(
                    (min(levenshtein(other_author.fn, author.fn),
                         levenshtein(other_author.cn, author.cn)),
                     other_author.id,
                     other_key)
                    for other_key, other_item in other_key_item_pais
                    for other_author in AuthorSet.to_author_set(other_item['AUTORES'])
                    if other_author.id and other_author.id not in already_present)
            except ValueError:  # min() arg is an empty sequence
                pass
            if new_id:
                already_present.add(new_id)
                main_item['AUTORES'][idx]['@NRO-ID-CNPQ'] = new_id
                main_item['AUTORES'][idx]['@@id_from'] = id_from.decode('utf-8')
