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
    valor NUMERIC(15, 4) NOT NULL,

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