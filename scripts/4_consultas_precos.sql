use precos_anp;

-- Contagem de registros da tabela principal
select count(*) from combustiveis_automotivos;

-- Contagem por produtos
select produto, count(*) from combustiveis_automotivos group by produto;

-- Contagem por produtos na tabela otimizada de preços de revenda
select produto, count(*) from combustiveis_automotivos_otimizada group by produto;

-- Contatem por produtos na tabela otimizada de preços dos distribuidores
select produto, count(*) from distribuicao_combustiveis_otimizada group by produto;

-- Contatem de CNPJs por bandeira 
select
  bandeira, count(distinct cnpj_revenda) c
from combustiveis_automotivos_otimizada
group by bandeira
order by c desc;

-- Ranking de preço médio de gasolina dos revendedores por estado
select
 regiao, estado, valor_venda_medio
from vw_media_venda_combustiveis_automotivos
where mes='2023-06' and produto = 'GASOLINA'
order by valor_venda_medio desc;

-- Ranking de preço médio de gasolina dos distribuidores por estado
select
 regiao, estado, valor_distribuicao_medio
from vw_media_distribuicao_combustiveis_automotivos
where mes='2023-06' and produto = 'GASOLINA'
order by valor_distribuicao_medio desc;

-- Ranking de agio do preço da gasolina (revenda/distribuição) por estado
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
  order by agio_preco_medio desc;

-- Ranking de razão de preço entre etanol e gasolina por estado
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