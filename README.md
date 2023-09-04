# hadoop-precos-combustiveis

Projeto HADOOP em Google Cloud para dados de preços de combustíveis da ANP

O trabalho faz uso de dados publicados pela ANP (Agência Nacional de Petróleo)

> Em cumprimento às determinações da Lei do Petróleo (Lei nº 9478/1997, artigo 8º), a ANP acompanha os preços praticados por revendedores de combustíveis automotivos e de gás liquefeito de petróleo envasilhado em botijões de 13 quilos (GLP P13), por meio de uma pesquisa semanal de preços realizada por empresa contratada.

- [Série Histórica de Preços de Combustíveis e de GLP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)

Coletamos a série histórica de "Combustíveis automotivos" que vai de 2004 a 2023. São 39 arquivos CSV totalizando aproximadamente 3.7 GB.

- [Metadados em PDF](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/metadados-serie-historica-precos-combustiveis-1.pdf)

Temos as seguintes colunas no CSV, em conformidade com a documentação de metadados:

- Regiao - Sigla
- Estado - Sigla
- Municipio
- Revenda
- CNPJ da Revenda
- Nome da Rua
- Numero Rua
- Complemento
- Bairro
- Cep
- Produto
- Data da Coleta
- Valor de Venda
- Valor de Compra
- Unidade de Medida
- Bandeira
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
    data STRING COMMENT 'Data da coleta (dd/mm/yyyy)',
    valor_venda STRING COMMENT 'Valor da venda (formato brasileiro)',
    valor_compra STRING COMMENT 'Valor da compra (formato brasileiro)',
    unidade_medida STRING COMMENT 'Unidade de medida (ex: R$ / litro)',
    bandeira STRING COMMENT 'Nome da bandeira do revendedor'
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
fields terminated by ';'
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
| data                  | string     | Data da coleta (dd/mm/yyyy)                        |
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

O tempo de consulta parece alto pois estamos trabalhando em um formato não otimizado.



```
ALTER TABLE combustiveis_automotivos SET SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
  "separatorChar" = ";",
  "quoteChar"     = "\""
);
```