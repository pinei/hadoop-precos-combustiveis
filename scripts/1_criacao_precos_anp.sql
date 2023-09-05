create database if not exists precos_anp
comment 'Base de dados de preços de combustíveis fornecidos pela ANP e alguns indicadores de mercado';

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
  "separatorChar" = ";"
)
stored as textfile
tblproperties("skip.header.line.count"="1");

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
  "separatorChar" = ";"
)
stored as textfile
tblproperties("skip.header.line.count"="1");

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

create materialized view vw_media_venda_combustiveis_automotivos
disable rewrite
as
select
  mes, regiao, estado, produto,
  avg(valor_venda) valor_venda_medio
 from combustiveis_automotivos_otimizada
 group by mes, regiao, estado, produto
 order by mes desc, regiao, estado, produto;

create materialized view vw_media_distribuicao_combustiveis_automotivos
disable rewrite
as
select
  mes, regiao, estado, produto,
  avg(preco_medio) valor_distribuicao_medio
 from distribuicao_combustiveis_otimizada
 group by mes, regiao, estado, produto
 order by mes desc, regiao, estado, produto;