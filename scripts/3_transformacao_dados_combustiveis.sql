use precos_anp;

-- Carregando uma parte por vez evita o erro OutOfMemory

-- DIESEL S10
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'DIESEL S10';

-- DIESEL S50
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'DIESEL S50';

-- DIESEL
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'DIESEL';

-- GNV
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'GNV';

-- ETANOL
insert into table combustiveis_automotivos_otimizada 
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
    when trim(produto) = 'ETANO' then 'ETANOL'
    else produto
  end as produto
from combustiveis_automotivos
where trim(produto) in ('ETANO', 'ETANOL');

-- GASOLINA
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'GASOLINA';

-- GASOLINA ADITIVADA
insert into table combustiveis_automotivos_otimizada 
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
from combustiveis_automotivos 
where produto = 'GASOLINA ADITIVADA';

-- Preços dos Distribuidores
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