# Web-service a ser utilizado para acesso aos CVs
ws_url = 'https://cnpqwsproxy.ufscar.br:7443/srvcurriculo/WSCurriculo?wsdl'
ws_encoding = 'iso-8859-1'

# A extração é I/O-bound, então faz sentido usar um número de jobs
# maior que o número de CPUs disponíveis
extract_jobs = 32

# Quantidade de CVs de um batch a ser carregado em memória de uma vez
cv_batch_size = 1024
