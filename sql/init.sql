CREATE SCHEMA IF NOT EXISTS anatel_datamart;
COMMENT ON SCHEMA anatel_datamart IS 'Esquema para o Data Mart de Índices de Desempenho no Atendimento da Anatel.';

-- TABELAS DE DIMENSÕES --

CREATE TABLE IF NOT EXISTS anatel_datamart.DimTempo (
    id_tempo SERIAL PRIMARY KEY,
    data_completa DATE NOT NULL UNIQUE,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano_mes VARCHAR(7) NOT NULL
);

COMMENT ON TABLE anatel_datamart.DimTempo IS 'Dimensão Tempo para análise dos indicadores.';
COMMENT ON COLUMN anatel_datamart.DimTempo.id_tempo IS 'Chave primária da dimensão tempo.';
COMMENT ON COLUMN anatel_datamart.DimTempo.data_completa IS 'Data completa (primeiro dia do mês).';
COMMENT ON COLUMN anatel_datamart.DimTempo.ano IS 'Ano referente ao indicador.';
COMMENT ON COLUMN anatel_datamart.DimTempo.mes IS 'Mês referente ao indicador (1-12).';
COMMENT ON COLUMN anatel_datamart.DimTempo.ano_mes IS 'Representação textual do ano e mês (ex: 2023-01).';

CREATE TABLE IF NOT EXISTS anatel_datamart.DimGrupoEconomico (
    id_grupo_economico SERIAL PRIMARY KEY,
    nome_grupo_economico VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE anatel_datamart.DimGrupoEconomico IS 'Dimensão Grupo Econômico das prestadoras de serviço.';
COMMENT ON COLUMN anatel_datamart.DimGrupoEconomico.id_grupo_economico IS 'Chave primária da dimensão grupo econômico.';
COMMENT ON COLUMN anatel_datamart.DimGrupoEconomico.nome_grupo_economico IS 'Nome do grupo econômico ao qual a prestadora pertence.';

CREATE TABLE IF NOT EXISTS anatel_datamart.DimServico (
    id_servico SERIAL PRIMARY KEY,
    nome_servico VARCHAR(255) NOT NULL UNIQUE,
    sigla_servico VARCHAR(10) NOT NULL UNIQUE
);

COMMENT ON TABLE anatel_datamart.DimServico IS 'Dimensão Serviço de telecomunicações.';
COMMENT ON COLUMN anatel_datamart.DimServico.id_servico IS 'Chave primária da dimensão serviço.';
COMMENT ON COLUMN anatel_datamart.DimServico.nome_servico IS 'Nome do serviço de telecomunicações (ex: Telefonia Celular).';
COMMENT ON COLUMN anatel_datamart.DimServico.sigla_servico IS 'Sigla do serviço de telecomunicações (ex: SMP, STFC, SCM).';

CREATE TABLE IF NOT EXISTS anatel_datamart.DimIndicador (
    id_indicador SERIAL PRIMARY KEY,
    nome_indicador VARCHAR(255) NOT NULL UNIQUE
);

COMMENT ON TABLE anatel_datamart.DimIndicador IS 'Dimensão Indicador de desempenho.';
COMMENT ON COLUMN anatel_datamart.DimIndicador.id_indicador IS 'Chave primária da dimensão indicador.';
COMMENT ON COLUMN anatel_datamart.DimIndicador.nome_indicador IS 'Nome do indicador de desempenho (ex: Taxa de Resolvidas em 5 dias úteis).';

-- TABELA FATO --

CREATE TABLE IF NOT EXISTS anatel_datamart.FactIndicadorDesempenho (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL,
    id_grupo_economico INTEGER NOT NULL,
    id_servico INTEGER NOT NULL,
    id_indicador INTEGER NOT NULL,
    valor NUMERIC(15, 4),

    CONSTRAINT fk_tempo FOREIGN KEY (id_tempo) REFERENCES anatel_datamart.DimTempo(id_tempo) ON DELETE RESTRICT,
    CONSTRAINT fk_grupo_economico FOREIGN KEY (id_grupo_economico) REFERENCES anatel_datamart.DimGrupoEconomico(id_grupo_economico) ON DELETE RESTRICT,
    CONSTRAINT fk_servico FOREIGN KEY (id_servico) REFERENCES anatel_datamart.DimServico(id_servico) ON DELETE RESTRICT,
    CONSTRAINT fk_indicador FOREIGN KEY (id_indicador) REFERENCES anatel_datamart.DimIndicador(id_indicador) ON DELETE RESTRICT
);

COMMENT ON TABLE anatel_datamart.FactIndicadorDesempenho IS 'Tabela de fatos contendo os valores dos indicadores de desempenho.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.id_fato IS 'Chave primária da tabela de fatos.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.id_tempo IS 'Chave estrangeira para DimTempo.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.id_grupo_economico IS 'Chave estrangeira para DimGrupoEconomico.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.id_servico IS 'Chave estrangeira para DimServico.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.id_indicador IS 'Chave estrangeira para DimIndicador.';
COMMENT ON COLUMN anatel_datamart.FactIndicadorDesempenho.valor IS 'Valor numérico do indicador.';

-- Criação de índices para otimizar consultas na tabela fato --

CREATE INDEX IF NOT EXISTS idx_fact_tempo ON anatel_datamart.FactIndicadorDesempenho(id_tempo);
CREATE INDEX IF NOT EXISTS idx_fact_grupo_economico ON anatel_datamart.FactIndicadorDesempenho(id_grupo_economico);
CREATE INDEX IF NOT EXISTS idx_fact_servico ON anatel_datamart.FactIndicadorDesempenho(id_servico);
CREATE INDEX IF NOT EXISTS idx_fact_indicador ON anatel_datamart.FactIndicadorDesempenho(id_indicador);

-- Criação da View (Task 4) --

CREATE OR REPLACE VIEW anatel_datamart.ViewTaxaVariacaoResolvidas5Dias AS
WITH
-- obtendo os valores do indicador específico
IndicatorValues AS (
    SELECT
        fid.id_tempo,
        fid.id_grupo_economico,
        fid.id_servico,
        fid.valor
    FROM
        anatel_datamart.FactIndicadorDesempenho fid
    JOIN
        anatel_datamart.DimIndicador di ON fid.id_indicador = di.id_indicador
    WHERE
        di.nome_indicador = 'Taxa de Resolvidas em 5 dias Úteis'
),
-- calculando o valor médio do IDA para cada grupo por mês
AvgGroupMonthIndicator AS (
    SELECT
        iv.id_tempo,
        iv.id_grupo_economico,
        AVG(iv.valor) AS avg_ida_valor
    FROM
        IndicatorValues iv
    GROUP BY
        iv.id_tempo,
        iv.id_grupo_economico
),
-- salvando valor do mes anterior
IndividualRateOfChange AS (
    SELECT
        agmi.id_tempo,
        dt.ano_mes,
        agmi.id_grupo_economico,
        ge.nome_grupo_economico,
        agmi.avg_ida_valor,
        LAG(agmi.avg_ida_valor, 1, NULL) OVER (
            PARTITION BY agmi.id_grupo_economico
            ORDER BY dt.data_completa
        ) AS prev_avg_ida_valor
    FROM
        AvgGroupMonthIndicator agmi
    JOIN
        anatel_datamart.DimTempo dt ON agmi.id_tempo = dt.id_tempo
    JOIN
        anatel_datamart.DimGrupoEconomico ge ON agmi.id_grupo_economico = ge.id_grupo_economico
),
-- cálculo da taxa de variação individual
CalculatedIndividualRoC AS (
    SELECT
        iroc.id_tempo,
        iroc.ano_mes,
        iroc.id_grupo_economico,
        iroc.nome_grupo_economico,
        iroc.avg_ida_valor,
        iroc.prev_avg_ida_valor,
        (CASE
            WHEN iroc.prev_avg_ida_valor IS NOT NULL AND iroc.prev_avg_ida_valor <> 0
            THEN ((iroc.avg_ida_valor - iroc.prev_avg_ida_valor) / iroc.prev_avg_ida_valor) * 100.0
            ELSE NULL
        END) AS taxa_variacao_individual
    FROM
        IndividualRateOfChange iroc
),
-- calculo da taxa de variação média mensal
MonthlyAvgRateOfChange AS (
    SELECT
        ciroc.id_tempo,
        AVG(ciroc.taxa_variacao_individual) AS taxa_variacao_media_mensal
    FROM
        CalculatedIndividualRoC ciroc
    WHERE
        ciroc.taxa_variacao_individual IS NOT NULL
    GROUP BY
        ciroc.id_tempo
),
-- calculo da diferença entre taxa individual e taxa media
FinalDataForPivot AS (
    SELECT
        ciroc.id_tempo,
        ciroc.ano_mes,
        ciroc.nome_grupo_economico,
        maroc.taxa_variacao_media_mensal,
        ciroc.taxa_variacao_individual,
        (ciroc.taxa_variacao_individual - maroc.taxa_variacao_media_mensal) AS diferenca_grupo
    FROM
        CalculatedIndividualRoC ciroc
    JOIN
        MonthlyAvgRateOfChange maroc ON ciroc.id_tempo = maroc.id_tempo
    WHERE
        ciroc.taxa_variacao_individual IS NOT NULL
)

SELECT
    fdp.ano_mes AS "Mes",
    ROUND(fdp.taxa_variacao_media_mensal, 2) AS "Taxa de variação media",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'ALGAR' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "ALGAR",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'CLARO' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "CLARO",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'EMBRATEL' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "EMBRATEL",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'GVT' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "GVT",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'NET' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "NET",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'OI' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "OI",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'SERCOMTEL' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "SERCOMTEL",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'SKY' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "SKY",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'TIM' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "TIM",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'VIACABO' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "VIACABO",
    ROUND(MAX(CASE WHEN fdp.nome_grupo_economico = 'VIVO' THEN fdp.diferenca_grupo ELSE NULL END), 2) AS "VIVO"
FROM
    FinalDataForPivot fdp
GROUP BY
    fdp.ano_mes,
    fdp.taxa_variacao_media_mensal
ORDER BY
    fdp.ano_mes;

COMMENT ON VIEW anatel_datamart.ViewTaxaVariacaoResolvidas5Dias IS
'View que calcula a taxa de variação mensal para o valor médio da "Taxa de Resolvidas em 5 dias úteis"
(média entre serviços SMP, SCM, STFC por grupo econômico), a taxa de variação média geral por mês,
e a diferença entre a taxa de variação média e a taxa individual de cada grupo econômico,
com os grupos econômicos pivotados nas colunas.';

COMMENT ON COLUMN anatel_datamart.ViewTaxaVariacaoResolvidas5Dias."Mes" IS 'Mês de referência no formato AAAA-MM.';
COMMENT ON COLUMN anatel_datamart.ViewTaxaVariacaoResolvidas5Dias."Taxa de variação media" IS 'Taxa de variação média mensal do indicador "Taxa de Resolvidas em 5 dias úteis" entre todos os grupos econômicos.';