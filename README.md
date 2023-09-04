# hadoop-precos-combustiveis

Projeto HADOOP em Google Cloud para dados de preços de combustíveis da ANP

O trabalho faz uso de dados publicados pela ANP (Agência Nacional de Petróleo)

> Em cumprimento às determinações da Lei do Petróleo (Lei nº 9478/1997, artigo 8º), a ANP acompanha os preços praticados por revendedores de combustíveis automotivos e de gás liquefeito de petróleo envasilhado em botijões de 13 quilos (GLP P13), por meio de uma pesquisa semanal de preços realizada por empresa contratada.

- [Série Histórica de Preços de Combustíveis e de GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)

Coletamos a série histórica de "Combustíveis automotivos" que vai de 2004 a 2023. São 39 arquivos CSV totalizando aproximadamente 3.7 GB.

- [Metadados em PDF](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/metadados-serie-historica-precos-combustiveis-1.pdf)

Temos as seguintes colunas nos arquivos CSV, em conformidade com a documentação de metadados:

- Regiao - Sigla (ex: S, N, SE)
- Estado - Sigla (ex: RJ, SP, MG)
- Municipio (nome do município)
- Revenda (razão social)
- CNPJ da Revenda (ex: 00.003.188/0001-21)
- Nome da Rua (informação de logradouro)
- Numero Rua (informação de logradouro)
- Complemento (informação de logradouro)
- Bairro (informação de logradouro)
- Cep (informação de logradouro)
- Produto (produto combustível) (ex: GASOLINA, ETANOL, DIESEL)
- Data da Coleta (data no formato (dd/mm/aaaa)
- Valor de Venda (numero em formato brasileiro com até 4 casas decimais)
- Valor de Compra (numero em formato brasileiro com até 4 casas decimais)
- Unidade de Medida (unidade ao qual o custo se refere) (ex: R$ / litro)
- Bandeira (nome de marca do posto de revenda) (ex: IPIRANGA, BRANCA, COSAN, etc.)
```

A princípio não vamos trabalhar com os dados de GLP.

## 1. Storage no Google Cloud

Criamos um bucket de nome `hadoop-dados-brutos` em uma região padrão (us-east1).

![Criação do bucket](assets/gcp-create-bucket.png)

Os arquivos do site da ANP foram descompactados e renomeados para um padrão `ca-<ano>-<semestre>.csv`.

Fizemos o upload dos arquivos para a pasta `anp-combustiveis-automotivos` do bucket.

![Arquivos CSV no bucket](assets/gcp-upload-files.png)

## 2. Cluster Hadoop para processamento dos dados

Para o processamento dos dados com o Hadoop, vamos usar o serviço Dataproc do Google Cloud

> Dataproc is a fully managed and highly scalable service for running Apache Hadoop, Apache Spark, Apache Flink, Presto, and 30+ open source tools.

![Criação do cluster](assets/gcp-create-dataproc-cluster.png)

- Nome: cluster-hadoop-anp
- Local: us-east1
- Tipo de Cluster: Padrão (1 mestre, N workers)

Há princípio não vamos usar nenhum componente extra.

![Componentes do cluster](assets/gcp-dataproc-components.png)

Para o nó do administrador, visando otimização de custo, selecionamos uma máquina padrão de menor capacidade, embora fosse possível customizar para um tamanho ainda menor.

> Contém o YARN Resource Manager, HDFS NameNode e todos os drivers do job.

- Série: N1
- Tipo de máquina: n1-standard-2 (2 vCPU, 7.5 GB de memória)
- Disco de 166 GB SSD

Para os nós de trabalho, o mesmo padrão de máquina

> Cada um contém um YARN NodeManager e um HDFS DataNode. O fator de replicação de HDFS é 2.

- Série: N1
- Tipo de máquina: n1-standard-2 (2 vCPU, 7.5 GB de memória)
- Disco de 166 GB SSD
- Number of worker nodes: 2

Os discos ficaram com tamanho 166 GB pois a soma dos 3 deve ser menor do que a quota de 500 GB da conta do Google Cloud.

Todas as outras configurações do cluster ficaram com preenchimento / seleção padrão.

Quando o cluster for criado com sucesso é possível acessar a máquina principal via SSH

![Acesso SSH ao cluster](assets/gcp-dataproc-ssh.png)

## 3. Acesso ao bucket

O acesso ao bucket criado no passo 1 pode ser feito por uma ferramenta de nome GCSFUSE, que faz o mapeamento (montagem) do bucket como uma pasta no sistema de arquivos local.

Comando para instalação da ferramenta:

```
############ Instalação do GCSFUSE #######################
####### GCSFUSE é uma aplicação que permite que você monte um 'bucket' do GCP no sistema de arquivos da sua VM
####### Dessa forma, ao fazer upload de arquivos para o bucket você pode utilizá-los dentro da VM
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install gcsfuse
```

O comando para fazer a montagem é `gcsfuse <bucket-name> <local-dir>`. O utilitário espera que a pasta esteja criada.

```
mkdir ./dados-brutos
gcsfuse hadoop-dados-brutos ./dados-brutos

I0904 00:10:57.440104 2023/09/04 00:10:57.440062 Start gcsfuse/1.1.0 (Go version go1.20.5) for app "" using mount point: /home/<username>/dados-brutos
```

Listando os arquivos:

```
cd ./dados-brutos/anp-combustiveis-automotivos
ls

ca-2004-01.csv  ca-2005-02.csv  ca-2007-01.csv  ca-2008-02.csv  ca-2010-01.csv  ca-2011-02.csv  ca-2013-01.csv  ca-2014-02.csv  ca-2016-01.csv  ca-2017-02.csv  ca-2019-01.csv  ca-2020-02.csv  ca-2022-01.csv ...
```

## 4. Interagindo com o HDFS

O comando `hdfs dfs` é um atalho para execução de comandos sobre o Hadoop File System (HDFS). 

Listando o conteúdo da raiz e da pasta `/user`:

```
hdfs dfs -ls /

Found 3 items
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:26 /tmp
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /var
```

```
hdfs dfs -ls /user

Found 11 items
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/dataproc
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/hbase
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/hdfs
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/hive
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/mapred
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/pig
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/solr
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/spark
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/yarn
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/zeppelin
drwxrwxrwt   - hdfs hadoop          0 2023-09-03 23:25 /user/zookeeper
```

## 5. Subindo os dados brutos para o HDFS

Criamos uma pasta `/data` e subimos a pasta `./dados-brutos/anp-combustiveis-automotivos` com todo o conteúdo

```
hdfs dfs -mkdir /data
hdfs dfs -put ./dados-brutos/anp-combustiveis-automotivos  /data
hdfs dfs -ls /data
```

Checando a listagem de arquivos no HDFS:

```
hdfs dfs -ls /data/anp-combustiveis-automotivos

Found 39 items
-rw-r--r--   2 <username> hadoop   48637859 2023-09-04 00:24 /data/anp-combustiveis-automotivos/ca-2004-01.csv
-rw-r--r--   2 <username> hadoop  158110155 2023-09-04 00:24 /data/anp-combustiveis-automotivos/ca-2004-02.csv
-rw-r--r--   2 <username> hadoop  157737721 2023-09-04 00:24 /data/anp-combustiveis-automotivos/ca-2005-01.csv
-rw-r--r--   2 <username> hadoop  127779846 2023-09-04 00:24 /data/anp-combustiveis-automotivos/ca-2005-02.csv
```

## 6. Criando o schema de dados no Hive

Idéia é criar uma visão de dados no Hive para os dados que subimos no Hadoop e realizar consultas e processamentos sobre estes dados.

Para a interação com o Hive usamos a ferramenta Beeline

```
beeline -u jdbc:hive2://localhost:10000/default

Connecting to jdbc:hive2://localhost:10000/default
Connected to: Apache Hive (version 3.1.3)
Driver: Hive JDBC (version 3.1.3)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 3.1.3 by Apache Hive
0: jdbc:hive2://localhost:10000/default> 
```

Criamos primeiro a nossa base de dados:

```sql
create database if not exists precos_anp
comment 'Base de dados de preços de combustíveis fornecidos pela ANP e alguns indicadores de mercado';
```

O resultado sugere que foi criada a base de dados

```
INFO  : Compiling command(queryId=hive_20230904010015_1b26f610-62d0-446e-aba0-fb0c7f9f8c65): create database if not exists precos_anp
comment 'Base de dados de preços de combustíveis fornecidos pela ANP e alguns indicadores de mercado'
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20230904010015_1b26f610-62d0-446e-aba0-fb0c7f9f8c65); Time taken: 0.648 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20230904010015_1b26f610-62d0-446e-aba0-fb0c7f9f8c65): create database if not exists precos_anp
comment 'Base de dados de preços de combustíveis fornecidos pela ANP e alguns indicadores de mercado'
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20230904010015_1b26f610-62d0-446e-aba0-fb0c7f9f8c65); Time taken: 0.251 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (1.179 seconds)
```

Podemos consultar os metadados

```sql
describe database extended precos_anp;
```

Uma tabela é retornada com as propriedades da base de dados:

- db_name: precos_anp
- comment: Base de dados de preços de combustíveis fornecidos pela ANP e alguns indicadores de mercado
- location: hdfs://cluster-hadoop-anp-m/user/hive/warehouse/precos_anp.db
- owner_type: anonymous
- parameters: USER

A nossa tabela de dados brutos terá um schema baseado no conteúdo dos nossos arquivos CSV que já estão no HDFS.

Por enquanto consideramos todos os campos como STRING.

O uso do formato SERDE é necessário pois encontramos alguns registros onde o valor de uma coluna pode ocupar várias linhas, usando neste caso o caractére `"` como marcador de início e fim.

```
use precos_anp;

create table combustiveis_automotivos (
    regiao STRING COMMENT 'Região do país (sigla)',
    estado STRING  COMMENT 'Estado do país (sigla)',
    municipio STRING COMMENT 'Nome do município',
    revenda STRING COMMENT 'Nome social do revendedor de combustível',
    cnpj_revenda STRING COMMENT 'CNPJ do revendedor',
    endereco_rua STRING COMMENT 'Nome da rua',
    endereco_numero STRING COMMENT 'Número na rua',
    endereco_complemento STRING COMMENT 'Complemento do endereço',
    endereco_bairro STRING COMMENT 'Nome do bairro',
    endereco_cep STRING COMMENT 'Código Postal',
    produto STRING COMMENT 'Nome do produto (ex: ETANOL, DIESEL, GASOLINA, etc.)',
    data STRING COMMENT 'Data da coleta (dd/mm/aaaa)',
    valor_venda STRING COMMENT 'Valor da venda (formato brasileiro)',
    valor_compra STRING COMMENT 'Valor da compra (formato brasileiro)',
    unidade_medida STRING COMMENT 'Unidade de medida (ex: R$ / litro)',
    bandeira STRING COMMENT 'Nome da bandeira do revendedor'
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  "separatorChar" = ";",
  "quoteChar"     = "\""
)
stored as textfile
tblproperties("skip.header.line.count"="1");
```

Confirmando a criação da tabela:

```sql
describe table extended combustiveis_automotivos
```

```
+-----------------------+------------+----------------------------------------------------+
|       col_name        | data_type  |                      comment                       |
+-----------------------+------------+----------------------------------------------------+
| regiao                | string     | Região do país (sigla)                             |
| estado                | string     | Estado do país (sigla)                             |
| municipio             | string     | Nome do município                                  |
| revenda               | string     | Nome social do revendedor de combustível           |
| cnpj_revenda          | string     | CNPJ do revendedor                                 |
| endereco_rua          | string     | Nome da rua                                        |
| endereco_numero       | string     | Número na rua                                      |
| endereco_complemento  | string     | Complemento do endereço                            |
| endereco_bairro       | string     | Nome do bairro                                     |
| endereco_cep          | string     | Código Postal                                      |
| produto               | string     | Nome do produto (ex: ETANOL, DIESEL, GASOLINA, ... |
| data                  | string     | Data da coleta (dd/mm/aaaa)                        |
| valor_venda           | string     | Valor da venda (formato brasileiro)                |
| valor_compra          | string     | Valor da compra (formato brasileiro)               |
| unidade_medida        | string     | Unidade de medida (ex: R$ / litro)                 |
| bandeira              | string     | Nome da bandeira do revendedor                     |
+-----------------------+------------+----------------------------------------------------+
```

## 7. Carregando os dados

Usamos o comando 'load data' para transferir os dados em CSV para o nosso schema criado.

```sql
load data inpath '/data/anp-combustiveis-automotivos' overwrite into table combustiveis_automotivos
```

```
...
INFO  : Executing command(queryId=hive_20230904013527_a1e0a2b4-f30a-4bdb-a636-378fca0cb1bb): load data inpath '/data/anp-combustiveis-automotivos' overwrite into table combustiveis_automotivos
INFO  : Loading data to table precos_anp.combustiveis_automotivos from hdfs://cluster-hadoop-anp-m/data/anp-combustiveis-automotivos
No rows affected (1.055 seconds)
```

Verificando se temos dados na tabela `combustiveis_automotivos`

```
select count(*) from combustiveis_automotivos
```

```
----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      6          6        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 23.97 s    
----------------------------------------------------------------------------------------------
INFO  : Completed executing command(queryId=hive_20230904013619_0f8b7571-e728-47ee-b94f-ab5a5bf239f1); Time taken: 44.727 seconds
+-----------+
|    _c0    |
+-----------+
| 22645940  |
+-----------+
1 row selected (47.726 seconds)
```

Temos cerca de 23 milhões de registros.

Verificando o quantitativo de registros por `produto`

```sql
select produto, count(*) from combustiveis_automotivos group by produto;
```

```
----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      6          6        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED    156        156        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 168.34 s   
----------------------------------------------------------------------------------------------
INFO  : Completed executing command(queryId=hive_20230904020550_44081b4b-b4f7-4e57-9c4e-5aaf4703ea4a); Time taken: 179.732 seconds
INFO  : OK
+----------------------------------------------------+----------+
|                      produto                       |   _c1    |
+----------------------------------------------------+----------+
| DIESEL S10                                         | 1978185  |
| DIESEL S50                                         | 44495    |
| DIESEL                                             | 5406872  |
| GNV                                                | 435900   |
| ETANO                                              | 1        |
| GASOLINA                                           | 7457216  |
| ETANOL                                             | 6896581  |
| GASOLINA ADITIVADA                                 | 426690   |
+----------------------------------------------------+----------+
8 rows selected (182.242 seconds)
```

Os tempos de consulta parecem altos pois estamos trabalhando em um formato de dados não otimizado.

Temos um registro com o nome de produto `ETANO` quando deveria ser `ETANOL`. Podemos corrigir em uma outra etapa.

Saindo do `beeline` para verificar o HDFS, podemos ver que a pasta `/data`` se encontra vazia.

```
hdfs dfs -ls /data
```

Enquanto a pasta de localização da tabela `combustiveis_automotivos` contém os arquivos CSV.

O comando `load data` fez a migração dos arquivos.

```
hdfs dfs -ls /user/hive/warehouse/precos_anp.db/combustiveis_automotivos
Found 39 items
-rw-r--r--   2 <username> hadoop   48637859 2023-09-04 00:24 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos/ca-2004-01.csv
-rw-r--r--   2 <username> hadoop  158110155 2023-09-04 00:24 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos/ca-2004-02.csv
-rw-r--r--   2 <username> hadoop  157737721 2023-09-04 00:24 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos/ca-2005-01.csv
...
```

## 8. Otimizando a tabela de dados

Idéia é migrar os dados para uma tabela mais otimizada para as consultas que executarmos.

Escolhemos o formato ORC por entender que é mais amigável ao Hadoop.

- [Tipos de arquivos em Bigdata](https://medium.com/rescuepoint/tipos-de-dados-em-bigdata-6ab0debec30a)

> O projeto do ORC nasceu em 2013 como uma iniciativa de acelerar o Hive e reduzir armazenamento no Hadoop.

Adotamos a compressão SNAPPY por ser menos custosa em termos de CPU, dado que não estamos lidando com um volume tão grande de dados.

Inicialmente pensamos em fazer a partição por `produto` e `estado` para otimizar o processamento ao realizar filtros e agrupamentos sobre estes campos.

> In Hadoop, partitioning is used to split data into smaller chunks, which are then distributed across multiple nodes in a cluster for processing.
> Partitioning works better when the cardinality of the partitioning field is not too high.

Há um limite para o número de partições. Apesar de ser configurável, seria melhor evitar um número alto de partições.

> The maximum number of dynamic partitions is controlled by hive.exec.max.dynamic.partitions and hive.exec.max.dynamic.partitions.pernode. Maximum was set to 100 partitions per node.

Consideramos então particionar a tabela somente pelo campo `produto`.

Usamos a compartimentação (bucketing) por `mes` (data no formato aaaa-mm) para otimizar a combinação dos dados (JOIN) com outras séries históricas.

> Bucketing works well when the field has high cardinality and data is evenly distributed among buckets. 

- [HIVE PARTITIONING VS. BUCKETING](https://data-flair.training/forums/topic/hive-partitioning-vs-bucketing/)

Com isso chegamos nesse schema para a tabela `combustiveis_automotivos_otimizada` 

```
use precos_anp;

create table combustiveis_automotivos_otimizada (
  regiao STRING,
  estado STRING,
  municipio STRING,
  revenda STRING,
  cnpj_revenda STRING,
  endereco_rua STRING,
  endereco_numero STRING,
  endereco_complemento STRING,
  endereco_bairro STRING,
  endereco_cep STRING,
  mes STRING,
  data DATE,
  valor_venda DECIMAL(10,4),  -- Tipo decimal com até 4 casas decimais
  valor_compra DECIMAL(10,4),  -- Tipo decimal com até 4 casas decimais
  unidade_medida STRING,
  bandeira STRING
)
partitioned by (produto STRING)
clustered by (mes) into 8 buckets
stored as orc
tblproperties ("orc.compress"="SNAPPY");
```

```
show tables;

+-------------------------------------+
|              tab_name               |
+-------------------------------------+
| combustiveis_automotivos            |
| combustiveis_automotivos_otimizada  |
+-------------------------------------+
```

```
describe combustiveis_automotivos_otimizada;

+--------------------------+----------------+----------+
|         col_name         |   data_type    | comment  |
+--------------------------+----------------+----------+
| regiao                   | string         |          |
| estado                   | string         |          |
| municipio                | string         |          |
| revenda                  | string         |          |
| cnpj_revenda             | string         |          |
| endereco_rua             | string         |          |
| endereco_numero          | string         |          |
| endereco_complemento     | string         |          |
| endereco_bairro          | string         |          |
| endereco_cep             | string         |          |
| mes                      | string         |          |
| data                     | date           |          |
| valor_venda              | decimal(10,4)  |          |
| valor_compra             | decimal(10,4)  |          |
| unidade_medida           | string         |          |
| bandeira                 | string         |          |
| produto                  | string         |          |
|                          | NULL           | NULL     |
| # Partition Information  | NULL           | NULL     |
| # col_name               | data_type      | comment  |
| produto                  | string         |          |
+--------------------------+----------------+----------+
```

Uma vez criada a tabela otimizada, podemos inserir os registros a partir da tabela de dados brutos, fazendo também a conversão e correção de campos.

```
use precos_anp;
insert overwrite table combustiveis_automotivos_otimizada 
select
  regiao,
  estado,
  municipio,
  revenda,
  cnpj_revenda,
  endereco_rua,
  endereco_numero,
  endereco_complemento,
  endereco_bairro,
  endereco_cep,
  regexp_replace(data, '^(\\d{2})/(\\d{2})/(\\d{4})$', '$3-$2') as mes,
  cast(regexp_replace(data, '^(\\d{2})/(\\d{2})/(\\d{4})$', '$3-$2-$1') as date) as data,
  cast(replace(valor_venda, ',', '.') as decimal(10,4)) as valor_venda,
  cast(replace(valor_compra, ',', '.') as decimal(10,4)) as valor_compra,
  unidade_medida,
  bandeira,
  case 
    when produto = 'ETANO' then 'ETANOL'
    else produto
  end as produto
from combustiveis_automotivos;
```

Em caso de `Out of Memory`, fazer a inserção por partes usando o `insert into` com cláusula `where` para inserir um conjunto de dados por vez.

Refazendo a contagem de registros por produto

```
use precos_anp;
select produto, count(*) from combustiveis_automotivos_otimizada group by produto;
```

```
+---------------------+----------+
|       produto       |   _c1    |
+---------------------+----------+
| DIESEL S50          | 44495    |
| DIESEL              | 5406872  |
| GASOLINA            | 7457216  |
| ETANOL              | 6896582  |
| GNV                 | 435900   |
| DIESEL S10          | 1978185  |
| GASOLINA ADITIVADA  | 426690   |
+---------------------+----------+
7 rows selected (23.448 seconds)
```

A consulta executou consideravelmente mais rápido na nova tabela.

Checando o conteúdo da tabela no HDFS verificamos que cada partição fica em uma pasta

```
hdfs dfs -ls /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada

Found 7 items
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 04:43 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL S10
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL S50
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 05:01 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=ETANOL
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 05:06 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=GASOLINA
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 05:18 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=GASOLINA ADITIVADA
drwxr-xr-x   - anonymous hadoop          0 2023-09-04 04:57 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=GNV
```

E dentro de cada pasta temos 8 buckets com tamanhos parecidos, em acordo com o esperado.

```
hdfs dfs -ls /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL

Found 8 items
-rw-r--r--   2 anonymous hadoop   15494126 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000000_0
-rw-r--r--   2 anonymous hadoop    9358828 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000001_0
-rw-r--r--   2 anonymous hadoop    9610799 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000002_0
-rw-r--r--   2 anonymous hadoop    9142598 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000003_0
-rw-r--r--   2 anonymous hadoop    8739530 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000004_0
-rw-r--r--   2 anonymous hadoop    9195491 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000005_0
-rw-r--r--   2 anonymous hadoop   10241446 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000006_0
-rw-r--r--   2 anonymous hadoop    9465032 2023-09-04 04:48 /user/hive/warehouse/precos_anp.db/combustiveis_automotivos_otimizada/produto=DIESEL/000007_0
```

## 9. Dados complementares

Como segunda tabela de dados, coletamos a série histórica mensal dos preços de distribuição de combustíveis líquidos, com dados desde 2020/09.

- [Preços de distribuição de combustíveis](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-de-distribuicao-de-combustiveis)

O arquivo está em formato do Excel. Foi necessário limpar e converter para o formato CSV. Usamos o nome `combustiveis-liquidos-municipios.csv`.

Temos as seguintes colunas no CSV:

- MÊS
- PRODUTO
- REGIÃO
- ESTADO
- MUNICÍPIO
- UNIDADE DE MEDIDA
- PREÇO MÉDIO DE DISTRIBUIÇÃO
- DESVIO PADRÃO

Subimos o arquivo para o bucket `hadoop-dados-brutos` na pasta `anp-combustiveis-automotivos`.

Usando o beeline, criamos uma tabela `distribuicao_combustiveis` para os dados em CSV:

```sql
use precos_anp;

create table distribuicao_combustiveis (
    mes STRING COMMENT 'Mês de coleta do preço (aaaa-mm)',
    produto STRING COMMENT 'Nome do produto (ex: ETANOL HIDRATADO COMUM, GASOLINA C COMUM, etc.)',
    regiao STRING COMMENT 'Região do país (ex: NORTE, CENTRO OESTE, etc.)',
    estado STRING COMMENT 'Estado do país (ex: CENTRO OESTE)',
    municipio STRING COMMENT 'Nome do município (ex: CRUZEIRO DO SUL)',
    unidade_medida STRING COMMENT 'Unidade de medida (ex: R%/l)',
    preco_medio STRING COMMENT 'Preço médio com 3 casas decimais (formato brasileiro)',
    desvio_padrao STRING COMMENT 'Desvio padrão da amostra com 3 casas decimais (formato brasileiro)'
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  "separatorChar" = ";",
  "quoteChar"     = "\""
)
stored as textfile
tblproperties("skip.header.line.count"="1");
```

Usamos o `load data` dessa vez com a diretiva `local` pois o arquivo CSV não está no HDFS.

```
load data local inpath '/home/<username>/combustiveis-liquidos-municipios.csv' overwrite into table distribuicao_combustiveis
```

Nota: Não foi possível transferir diretamente do `bucket` montado. O `beeline` não localizava o arquivo.

Criamos uma tabela otimizada, porém sem preocupação com compressão, particionamento ou `bucket` por ser uma tabela pequena (80000 registros).

```sql
create table distribuicao_combustiveis_otimizada (
  mes STRING,
  produto STRING,
  regiao STRING,
  estado STRING,
  municipio STRING,
  unidade_medida STRING,
  preco_medio DECIMAL(10,3),
  desvio_padrao DECIMAL(10,3)
)
stored as orc
tblproperties ("orc.compress"="NONE");
```

```
show tables

+--------------------------------------+
|               tab_name               |
+--------------------------------------+
| combustiveis_automotivos             |
| combustiveis_automotivos_otimizada   |
| distribuicao_combustiveis            |
| distribuicao_combustiveis_otimizada  |
+--------------------------------------+
```

Executando a carga da tabela otimizada com algumas conversões de valores para compatibilizar com a tabela `combustiveis_automotivos_otimizada`

```sql
insert overwrite table distribuicao_combustiveis_otimizada 
select
  mes,
  case
    when produto = 'ETANOL HIDRATADO COMUM' then 'ETANOL'
    when produto = 'GASOLINA C COMUM' then 'GASOLINA'
    when produto = 'GASOLINA C COMUM ADITIVADA' then 'GASOLINA ADITIVADA'
    when produto = 'ÓLEO DIESEL B S10 - COMUM' then 'DIESEL S10'
    when produto = 'ÓLEO DIESEL B S500 - COMUM' then 'DIESEL S500'
    else NULL
  end as produto,
  case
    when regiao = 'CENTRO OESTE' then 'CO'
    when regiao = 'NORDESTE' then 'NE'
    when regiao = 'NORTE' then 'N'
    when regiao = 'SUDESTE' then 'SE'
    when regiao = 'SUL' then 'S'
    else NULL
  end as regiao,
  case
    when estado = 'ACRE' then 'AC'
    when estado = 'ALAGOAS' then 'AL'
    when estado = 'AMAZONAS' then 'AM'
    when estado = 'AMAPA' then 'AP'
    when estado = 'BAHIA' then 'BA'
    when estado = 'CEARA' then 'CE'
    when estado = 'DISTRITO FEDERAL' then 'DF'
    when estado = 'ESPIRITO SANTO' then 'ES'
    when estado = 'GOIAS' then 'GO'
    when estado = 'MARANHAO' then 'MA'
    when estado = 'MINAS GERAIS' then 'MG'
    when estado = 'MATO GROSSO DO SUL' then 'MS'
    when estado = 'MATO GROSSO' then 'MT'
    when estado = 'PARA' then 'PA'
    when estado = 'PARAIBA' then 'PB'
    when estado = 'PERNAMBUCO' then 'PE'
    when estado = 'PIAUI' then 'PI'
    when estado = 'PARANA' then 'PR'
    when estado = 'RIO DE JANEIRO' then 'RJ'
    when estado = 'RIO GRANDE DO NORTE' then 'RN'
    when estado = 'RONDONIA' then 'RO'
    when estado = 'RORAIMA' then 'RR'
    when estado = 'RIO GRANDE DO SUL' then 'RS'
    when estado = 'SANTA CATARINA' then 'SC'
    when estado = 'SERGIPE' then 'SE'
    when estado = 'SAO PAULO' then 'SP'
    when estado = 'TOCANTINS' then 'TO'
    else NULL
  end as estado,
  municipio,
  unidade_medida,
  cast(replace(preco_medio, ',', '.') as decimal(10,3)),
  cast(replace(desvio_padrao, ',', '.') as decimal(10,3))
from distribuicao_combustiveis
where trim(preco_medio) not in ('', '-');
```

Verificando os dados

```sql
select produto, count(*)
from distribuicao_combustiveis_otimizada
group by produto;
```

```
+---------------------+--------+
|       produto       |  _c1   |
+---------------------+--------+
| DIESEL S10          | 16062  |
| DIESEL S500         | 15246  |
| ETANOL              | 15892  |
| GASOLINA            | 16065  |
| GASOLINA ADITIVADA  | 16056  |
+---------------------+--------+
5 rows selected (21.159 seconds)
```

## 10. Consultas

1. Qual a distribuição de CNPJs por bandeira?

```sql
select
  bandeira, count(distinct cnpj_revenda) c
from combustiveis_automotivos
group by bandeira
order by c desc;
```

```
+-------------------------------+--------+
|           bandeira            |   c    |
+-------------------------------+--------+
| BRANCA                        | 26021  |
| PETROBRAS DISTRIBUIDORA S.A.  | 9208   |
| IPIRANGA                      | 7126   |
| RAIZEN                        | 5819   |
| CBPI                          | 4033   |
| VIBRA ENERGIA                 | 3549   |
| COSAN LUBRIFICANTES           | 2395   |
| ALESAT                        | 1774   |
| LIQUIGÁS                      | 498    |
| SABBÁ                         | 484    |
| SATELITE                      | 463    |
...
```

A consulta retornou 268 bandeiras diferentes. Pelo alto numero de bandeiras, consideramos que não é interessante fazer outras comparações de dados entre as bandeiras.

Decidimos focar nas comparações entre os estados.

2. Qual o preço médio de revenda de combustíveis por mês, produto, estado?

Consideramos declarar como uma `view materializada` para facilitar o reuso em consultas subsequentes.

```sql
create materialized view vw_media_venda_combustiveis_automotivos
disable rewrite
as
select
  mes, regiao, estado, produto,
  avg(valor_venda) valor_venda_medio
 from combustiveis_automotivos_otimizada
 group by mes, regiao, estado, produto
 order by mes desc, regiao, estado, produto;
```

```
show tables

+------------------------------------------+
|                 tab_name                 |
+------------------------------------------+
| combustiveis_automotivos                 |
| combustiveis_automotivos_otimizada       |
| distribuicao_combustiveis                |
| distribuicao_combustiveis_otimizada      |
| vw_media_venda_combustiveis_automotivos  |
+------------------------------------------+
```

Fazendo uma comparação do preço médio da gasolina entre os estados, considerando o último mês da série (2023-06)

```sql
select
 regiao, estado, valor_venda_medio
from vw_media_venda_combustiveis_automotivos
where mes='2023-06' and produto = 'GASOLINA'
order by valor_venda_medio desc;
```

```
+---------+---------+---------------------------+
| regiao  | estado  |     valor_venda_medio     |
+---------+---------+---------------------------+
| N       | AM      | 6.1963485477178423236515  |
| N       | AC      | 6.0998591549295774647887  |
| N       | RO      | 5.9115189873417721518987  |
| N       | RR      | 5.7430882352941176470588  |
| NE      | RN      | 5.7096984924623115577889  |
| N       | TO      | 5.6135135135135135135135  |
| SE      | ES      | 5.5868246445497630331754  |
| NE      | BA      | 5.5735056876938986556360  |
| NE      | AL      | 5.5541255605381165919283  |
| S       | SC      | 5.5486000000000000000000  |
| N       | PA      | 5.4800686498855835240275  |
| S       | PR      | 5.4466319772942289498581  |
| NE      | CE      | 5.4353145695364238410596  |
| S       | RS      | 5.4297478991596638655462  |
| CO      | DF      | 5.4250000000000000000000  |
| NE      | PE      | 5.4204094488188976377953  |
| CO      | MS      | 5.4188059701492537313433  |
| SE      | RJ      | 5.4183605393896380411639  |
| NE      | SE      | 5.4177124183006535947712  |
| CO      | MT      | 5.3905769230769230769231  |
| CO      | GO      | 5.3576572958500669344043  |
| NE      | PI      | 5.2799528301886792452830  |
| NE      | MA      | 5.2708883248730964467005  |
| SE      | SP      | 5.2611028287461773700306  |
| SE      | MG      | 5.2551664254703328509407  |
| NE      | PB      | 5.2194736842105263157895  |
| N       | AP      | 5.0321590909090909090909  |
+---------+---------+---------------------------+
```

Os dados indicam que os valores mais altos se concentram no norte do país (vamos analisar mais abaixo o caso do Amapá).

A consulta pode ser reutilizada para comparações de valores de outros produtos e meses.

3. Qual o preço médio de distribuição de combustíveis por mês, produto, estado?

Consideramos declarar como uma `view materializada` para facilitar o reuso em consultas subsequentes.

```sql
create materialized view vw_media_distribuicao_combustiveis_automotivos
disable rewrite
as
select
  mes, regiao, estado, produto,
  avg(preco_medio) valor_distribuicao_medio
 from distribuicao_combustiveis_otimizada
 group by mes, regiao, estado, produto
 order by mes desc, regiao, estado, produto;
```

Fazendo uma comparação do preço médio de distribuição da gasolina entre os estados, considerando o último mês da série (2023-06)

```sql
select
 regiao, estado, valor_distribuicao_medio
from vw_media_distribuicao_combustiveis_automotivos
where mes='2023-06' and produto = 'GASOLINA'
order by valor_distribuicao_medio desc;
```

```
+---------+---------+---------------------------+
| regiao  | estado  | valor_distribuicao_medio  |
+---------+---------+---------------------------+
| N       | AC      | 5.031000000000000000000   |
| N       | RR      | 4.908000000000000000000   |
| SE      | ES      | 4.877400000000000000000   |
| NE      | AL      | 4.877200000000000000000   |
| NE      | CE      | 4.867307692307692307692   |
| N       | AM      | 4.866400000000000000000   |
| NE      | RN      | 4.858500000000000000000   |
| CO      | MS      | 4.857142857142857142857   |
| NE      | SE      | 4.838500000000000000000   |
| CO      | MT      | 4.824428571428571428571   |
| N       | RO      | 4.823000000000000000000   |
| N       | AP      | 4.808500000000000000000   |
| N       | PA      | 4.802250000000000000000   |
| CO      | DF      | 4.794000000000000000000   |
| N       | TO      | 4.771600000000000000000   |
| NE      | BA      | 4.770433333333333333333   |
| NE      | PI      | 4.767500000000000000000   |
| NE      | PB      | 4.727500000000000000000   |
| SE      | RJ      | 4.721718750000000000000   |
| S       | SC      | 4.721714285714285714286   |
| NE      | PE      | 4.716111111111111111111   |
| S       | RS      | 4.703166666666666666667   |
| NE      | MA      | 4.666166666666666666667   |
| SE      | MG      | 4.653810344827586206897   |
| CO      | GO      | 4.652117647058823529412   |
| S       | PR      | 4.647172413793103448276   |
| SE      | SP      | 4.558657407407407407407   |
+---------+---------+---------------------------+
```

4. Qual o ágio médio entre os preços de revenda e distribuição de combustíveis por mês, produto, estado?

Combinamos as duas tabelas para calcular a razão preço de revenda / preço de distribuição.

Exemplo de execução para o preço da gasolina no último mês da série

```
select
  v.mes,
  v.regiao,
  v.estado,
  v.produto,
  (v.valor_venda_medio / d.valor_distribuicao_medio) as agio_preco_medio
from
  vw_media_venda_combustiveis_automotivos v join
  vw_media_distribuicao_combustiveis_automotivos d on (
    v.mes = d.mes and
    v.regiao = d.regiao and
    v.estado = d.estado and
    v.produto = d.produto
  )
  where v.mes='2023-06' and v.produto = 'GASOLINA'
  order by agio_preco_medio desc
```

```
+----------+-----------+-----------+------------+-------------------+
|  v.mes   | v.regiao  | v.estado  | v.produto  | agio_preco_medio  |
+----------+-----------+-----------+------------+-------------------+
| 2023-06  | N         | AM        | GASOLINA   | 1.273292          |
| 2023-06  | N         | RO        | GASOLINA   | 1.225693          |
| 2023-06  | N         | AC        | GASOLINA   | 1.212455          |
| 2023-06  | N         | TO        | GASOLINA   | 1.176443          |
| 2023-06  | NE        | RN        | GASOLINA   | 1.175198          |
| 2023-06  | S         | SC        | GASOLINA   | 1.175124          |
| 2023-06  | S         | PR        | GASOLINA   | 1.172031          |
| 2023-06  | N         | RR        | GASOLINA   | 1.170148          |
| 2023-06  | NE        | BA        | GASOLINA   | 1.168344          |
| 2023-06  | S         | RS        | GASOLINA   | 1.154488          |
| 2023-06  | SE        | SP        | GASOLINA   | 1.154090          |
| 2023-06  | CO        | GO        | GASOLINA   | 1.151660          |
| 2023-06  | NE        | PE        | GASOLINA   | 1.149339          |
| 2023-06  | SE        | RJ        | GASOLINA   | 1.147540          |
| 2023-06  | SE        | ES        | GASOLINA   | 1.145451          |
| 2023-06  | N         | PA        | GASOLINA   | 1.141146          |
| 2023-06  | NE        | AL        | GASOLINA   | 1.138794          |
| 2023-06  | CO        | DF        | GASOLINA   | 1.131623          |
| 2023-06  | NE        | MA        | GASOLINA   | 1.129597          |
| 2023-06  | SE        | MG        | GASOLINA   | 1.129218          |
| 2023-06  | NE        | SE        | GASOLINA   | 1.119709          |
| 2023-06  | CO        | MT        | GASOLINA   | 1.117350          |
| 2023-06  | NE        | CE        | GASOLINA   | 1.116698          |
| 2023-06  | CO        | MS        | GASOLINA   | 1.115637          |
| 2023-06  | NE        | PI        | GASOLINA   | 1.107489          |
| 2023-06  | NE        | PB        | GASOLINA   | 1.104066          |
| 2023-06  | N         | AP        | GASOLINA   | 1.046513          |
+----------+-----------+-----------+------------+-------------------+
```

Acompanhando análise anterior, os dados indicam que os maiores ágios se concentram no norte do país.

O caso do Amapá é uma exceção. Encontramos notícia relacionada de 2022-04 e é possível confirmar os dados também nesta janela, parametrizando as consultas documentadas acima.

- [Por que Macapá é a capital com a gasolina mais barata do Brasil](https://www.uol.com.br/carros/noticias/redacao/2022/05/18/por-que-macapa-e-a-capital-com-a-gasolina-mais-barata-do-brasil.htm)

```
+----------+-----------+-----------+------------+-------------------+
|  v.mes   | v.regiao  | v.estado  | v.produto  | agio_preco_medio  |
+----------+-----------+-----------+------------+-------------------+
...
| 2022-04  | SE        | RJ        | GASOLINA   | 1.091584          |
| 2022-04  | S         | RS        | GASOLINA   | 1.089768          |
| 2022-04  | NE        | SE        | GASOLINA   | 1.084377          |
| 2022-04  | NE        | PB        | GASOLINA   | 1.079490          |
| 2022-04  | CO        | MS        | GASOLINA   | 1.072820          |
| 2022-04  | N         | AP        | GASOLINA   | 1.033654          |
+----------+-----------+-----------+------------+-------------------+
```

```
+---------+---------+---------------------------+
| regiao  | estado  |     valor_venda_medio     |
+---------+---------+---------------------------+
...
| NE      | PB      | 7.0913492063492063492063  |
| N       | RR      | 7.0538461538461538461538  |
| CO      | MT      | 7.0150873786407766990291  |
| S       | RS      | 6.9307730496453900709220  |
| SE      | SP      | 6.9178419402738821976572  |
| N       | AP      | 6.3693750000000000000000  |
+---------+---------+---------------------------+
```

5. Em quais estados é mais vantajoso abastecer o carro com alcool?

Diz-se que se o preço do alcool está menor do que 70% do preço da gasolina, é mais vantajoso abastecer com alcool.

Podemos então consultar a razão entre os preços médios de revenda do alcool (etanol) e da gasolina.

```
select
  g.mes,
  g.regiao,
  g.estado,
  ( e.valor_venda_medio / g.valor_venda_medio ) as razao_etanol_gasolina
from
(
    select * from
        vw_media_venda_combustiveis_automotivos
    where produto = 'GASOLINA'
) g inner join
(
    select * from
        vw_media_venda_combustiveis_automotivos
    where produto = 'ETANOL'
) e on (
    g.mes = e.mes AND
    g.regiao = e.regiao AND
    g.estado = e.estado
)
where g.mes = '2023-06'
order by razao_etanol_gasolina desc;
```
