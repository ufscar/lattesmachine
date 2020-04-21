# Web-service a ser utilizado para acesso aos CVs
ws_url = 'https://cnpqwsproxy.ufscar.br:7443/srvcurriculo/WSCurriculo?wsdl'
ws_encoding = 'iso-8859-1'

# A extração é I/O-bound, então faz sentido usar um número de jobs
# maior que o número de CPUs disponíveis
extract_jobs = 32

# Quantidade de CVs de um batch a ser carregado em memória de uma vez
cv_batch_size = 1024

# Quantidade de items de um batch a ser carregado em memória de uma vez
item_batch_size = 1024

# Paraleliza etapa de simstring do dedup se houver pelo menos essa
# quantidade de batches a serem processados
parallel_batch_threshold = 3

# Tamanho dos n-grams utilizados na geração do índice
title_ngram = 5

# Inserir marcas especiais para começo e fim de strings nos n-grams?
title_be = False

# Métrica de similaridade (simstring)
import simstring
title_measure = simstring.jaccard

# Limiar de similaridade (simstring)
title_threshold = 0.6

# Limiar de similaridade (Levenshtein normalizado)
title_threshold_levnorm = 0.9

# Para os autores, é realizada uma comparação bastante conservativa
# (vide algoritmo em nametools.AuthorSet). Assume-se que algum dos proprietários
# de CV pode ter esquecido de digitar o nome de alguns coautores, ou que possa ter
# digitado em uma ordem incorreta. Se a distância de edição normalizada pelo
# tamanho dos nomes e pelo número de nomes for acima do limiar definido abaixo,
# as produções correspondentes não são consideradas duplicatas.
author_threshold = 1.5
