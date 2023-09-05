use precos_anp;

-- O download dos arquivos é feito manualmente no site da ANP

-- Carrega os arquivos CSV do HDFS
load data inpath '/data/anp-combustiveis-automotivos' overwrite into table combustiveis_automotivos;

-- Carrega um CSV do sistema de arquivos local
load data local inpath '/home/<username>/combustiveis-liquidos-municipios.csv' overwrite into table distribuicao_combustiveis;