# Geração do container

```bash
docker built -t lattesmachine .
```

# Fluxo de execução

```bash
# Crie um diretório de trabalho para mapear como volume docker

mkdir work

# Copie uma lista de CPFs, de ID CNPqs ou de pares no formato nomeCompleto;dataNascimento
# para o diretório de trabalho

cp lista.csv work

# Download dos currículos para uma base de CVs

docker run -v ./work:/work lattesmachine extract --people /work/lista.csv --db_cv /work/db-cv

# OPCIONAL: Download recursivo de CVs referenciados na base já existente (termine com Ctrl-C quando
# estiver satisfeito com a quantidade de CVs, ou execute com controle de timeout da shell)

docker run -v ./work:/work lattesmachine recurse --db_cv /work/db-cv

# OPCIONAL: Exporta currículos em formato JSON

docker run -v ./work:/work lattesmachine exportjson --db /work/db-cv > cvs.json

# Cria uma base de itens a partir da base de CVs

docker run -v ./work:/work lattesmachine splititems --db_cv /work/db-cv --db_items /work/db-items

# Detecta e marca itens que sejam duplicatas de outros

docker run -v ./work:/work lattesmachine dedup --db_items /work/db-items

# OPCIONAL: Baixa indicadores da Scopus para periódicos referenciados na base de itens

docker run -v ./work:/work lattesmachine scopus --db_items /work/db-items --cache /work/cache

# Exporta itens em formato JSON excluindo as duplicatas

docker run -v ./work:/work lattesmachine exportjson --db /work/db-items --skip_dup > items.json
```
