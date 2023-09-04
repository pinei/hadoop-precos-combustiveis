use precos_anp;

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