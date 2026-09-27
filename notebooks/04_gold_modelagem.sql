-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Gold: modelo estrela (ETL Silver → Gold)
-- MAGIC
-- MAGIC **Modelo escolhido: Esquema Estrela (Star Schema).** O "fato" central é o **vínculo do colaborador** (1 linha por colaborador, com o status de desligamento e as métricas numéricas). Ao redor, dimensões descritivas usadas para filtrar e agrupar.
-- MAGIC
-- MAGIC ```
-- MAGIC                    dim_departamento
-- MAGIC                          |
-- MAGIC  dim_colaborador --- fato_colaborador --- dim_cargo
-- MAGIC                          |
-- MAGIC                   dim_escolaridade
-- MAGIC ```
-- MAGIC
-- MAGIC Por que estrela: as perguntas de negócio são do tipo "taxa de turnover **por** departamento / cargo / faixa". Uma tabela fato ligada a dimensões simples torna essas consultas diretas (um JOIN por dimensão).
-- MAGIC
-- MAGIC Os comentários de tabela e coluna criados aqui ficam registrados no **Unity Catalog** e formam o **Catálogo de Dados** do MVP.

-- COMMAND ----------

-- Permite reexecutar o notebook: a fato referencia as dimensoes (FK), entao ela e removida primeiro
DROP TABLE IF EXISTS workspace.gold.fato_colaborador;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão Departamento

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.dim_departamento (
  departamento_id INT    COMMENT 'Chave substituta do departamento (1 a 3)',
  departamento    STRING COMMENT 'Nome do departamento: Pesquisa e Desenvolvimento, Recursos Humanos, Vendas'
) COMMENT 'Gold - Dimensao de departamentos. Origem: silver.rh_colaboradores.departamento';

-- COMMAND ----------

INSERT INTO workspace.gold.dim_departamento
SELECT DENSE_RANK() OVER (ORDER BY departamento) AS departamento_id, departamento
FROM (SELECT DISTINCT departamento FROM workspace.silver.rh_colaboradores) d;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão Cargo
-- MAGIC Obs.: o mesmo cargo (ex.: Gerente) pode existir em mais de um departamento, por isso cargo e departamento são dimensões separadas.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.dim_cargo (
  cargo_id INT    COMMENT 'Chave substituta do cargo (1 a 9)',
  cargo    STRING COMMENT 'Nome do cargo em portugues (9 categorias)'
) COMMENT 'Gold - Dimensao de cargos. Origem: silver.rh_colaboradores.cargo (JobRole traduzido)';

-- COMMAND ----------

INSERT INTO workspace.gold.dim_cargo
SELECT DENSE_RANK() OVER (ORDER BY cargo) AS cargo_id, cargo
FROM (SELECT DISTINCT cargo FROM workspace.silver.rh_colaboradores) c;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão Escolaridade

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.dim_escolaridade (
  escolaridade_id INT    COMMENT 'Nivel de escolaridade (1 a 5), igual ao codigo da origem (Education)',
  escolaridade    STRING COMMENT 'Descricao: Ensino medio, Superior incompleto, Graduacao, Mestrado, Doutorado'
) COMMENT 'Gold - Dimensao de escolaridade. Origem: silver.rh_colaboradores';

-- COMMAND ----------

INSERT INTO workspace.gold.dim_escolaridade
SELECT DISTINCT escolaridade_nivel, escolaridade
FROM workspace.silver.rh_colaboradores;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão Colaborador (perfil demográfico)

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.dim_colaborador (
  colaborador_id    INT    COMMENT 'Identificador do colaborador (EmployeeNumber na origem). Nao identifica a pessoa: base ficticia e sem nomes',
  genero            STRING COMMENT 'Feminino ou Masculino',
  estado_civil      STRING COMMENT 'Solteiro(a), Casado(a) ou Divorciado(a)',
  faixa_etaria      STRING COMMENT 'Faixa de idade: 18-25, 26-35, 36-45, 46-60',
  area_formacao     STRING COMMENT 'Area de formacao: Ciencias biologicas, Saude, Marketing, Curso tecnico, Recursos Humanos, Outra',
  frequencia_viagem STRING COMMENT 'Frequencia de viagens a trabalho: Nao viaja, Raramente, Frequentemente'
) COMMENT 'Gold - Dimensao com o perfil descritivo de cada colaborador. Origem: silver.rh_colaboradores';

-- COMMAND ----------

INSERT INTO workspace.gold.dim_colaborador
SELECT colaborador_id, genero, estado_civil, faixa_etaria, area_formacao, frequencia_viagem
FROM workspace.silver.rh_colaboradores;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fato Colaborador
-- MAGIC Granularidade: **1 linha por colaborador**. Contém as chaves para as dimensões, as métricas numéricas, as faixas usadas nas análises e as flags `flag_desligado` e `flag_hora_extra`.
-- MAGIC Taxa de turnover de qualquer grupo = `AVG(flag_desligado)` × 100.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.fato_colaborador (
  colaborador_id                  INT    COMMENT 'Chave para dim_colaborador',
  departamento_id                 INT    COMMENT 'Chave para dim_departamento',
  cargo_id                        INT    COMMENT 'Chave para dim_cargo',
  escolaridade_id                 INT    COMMENT 'Chave para dim_escolaridade',
  nivel_cargo                     INT    COMMENT 'Nivel hierarquico do cargo, 1 (entrada) a 5 (senior)',
  idade                           INT    COMMENT 'Idade em anos (18 a 60)',
  salario_mensal                  INT    COMMENT 'Salario mensal (1.009 a 19.999; moeda nao informada)',
  faixa_salarial                  STRING COMMENT 'Faixa salarial: 1 Ate 2.999, 2 3.000-5.999, 3 6.000-9.999, 4 10.000+',
  perc_ultimo_aumento             INT    COMMENT 'Percentual do ultimo aumento (11 a 25)',
  nivel_stock_options             INT    COMMENT 'Nivel de stock options (0 a 3)',
  distancia_casa                  INT    COMMENT 'Distancia casa-trabalho (1 a 29)',
  faixa_distancia                 STRING COMMENT 'Faixa de distancia: 01-05, 06-10, 11-20, 21-29',
  anos_experiencia_total          INT    COMMENT 'Anos de experiencia total (0 a 40)',
  qtd_empresas_anteriores         INT    COMMENT 'Empresas anteriores (0 a 9)',
  anos_empresa                    INT    COMMENT 'Anos de empresa (0 a 40)',
  faixa_tempo_casa                STRING COMMENT 'Faixa de tempo de casa: 1 0-1 ano, 2 2-5 anos, 3 6-10 anos, 4 Mais de 10 anos',
  anos_cargo_atual                INT    COMMENT 'Anos no cargo atual (0 a 18)',
  anos_desde_promocao             INT    COMMENT 'Anos desde a ultima promocao (0 a 15)',
  anos_com_gestor                 INT    COMMENT 'Anos com o gestor atual (0 a 17)',
  qtd_treinamentos_ano            INT    COMMENT 'Treinamentos no ultimo ano (0 a 6)',
  nota_satisfacao_trabalho        INT    COMMENT 'Satisfacao com o trabalho, 1 Baixa a 4 Muito alta',
  nota_satisfacao_ambiente        INT    COMMENT 'Satisfacao com o ambiente, 1 Baixa a 4 Muito alta',
  nota_satisfacao_relacionamentos INT    COMMENT 'Satisfacao com relacionamentos, 1 Baixa a 4 Muito alta',
  nota_envolvimento               INT    COMMENT 'Envolvimento com o trabalho, 1 Baixo a 4 Muito alto',
  nota_equilibrio_vida            INT    COMMENT 'Equilibrio vida-trabalho, 1 Ruim a 4 Excelente',
  nota_desempenho                 INT    COMMENT 'Avaliacao de desempenho, 3 Excelente ou 4 Excepcional',
  flag_hora_extra                 INT    COMMENT '1 = faz hora extra; 0 = nao faz',
  flag_desligado                  INT    COMMENT '1 = desligado (turnover); 0 = ativo. Metrica principal do MVP'
) COMMENT 'Gold - Fato de vinculo do colaborador (1 linha por colaborador) com metricas e status de desligamento. Origem: silver.rh_colaboradores + dimensoes gold';

-- COMMAND ----------

INSERT INTO workspace.gold.fato_colaborador
SELECT
  s.colaborador_id, d.departamento_id, c.cargo_id, s.escolaridade_nivel AS escolaridade_id,
  s.nivel_cargo, s.idade, s.salario_mensal, s.faixa_salarial, s.perc_ultimo_aumento, s.nivel_stock_options,
  s.distancia_casa, s.faixa_distancia, s.anos_experiencia_total, s.qtd_empresas_anteriores,
  s.anos_empresa, s.faixa_tempo_casa, s.anos_cargo_atual, s.anos_desde_promocao, s.anos_com_gestor,
  s.qtd_treinamentos_ano, s.nota_satisfacao_trabalho, s.nota_satisfacao_ambiente,
  s.nota_satisfacao_relacionamentos, s.nota_envolvimento, s.nota_equilibrio_vida, s.nota_desempenho,
  s.flag_hora_extra, s.flag_desligado
FROM workspace.silver.rh_colaboradores s
JOIN workspace.gold.dim_departamento d ON s.departamento = d.departamento
JOIN workspace.gold.dim_cargo        c ON s.cargo        = c.cargo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Chaves primárias e estrangeiras (informativas)
-- MAGIC O Unity Catalog permite declarar PK/FK. Elas não bloqueiam dados, mas documentam o modelo e aparecem no Catalog Explorer (inclusive no diagrama de relacionamentos).

-- COMMAND ----------

ALTER TABLE workspace.gold.dim_departamento ALTER COLUMN departamento_id SET NOT NULL;
ALTER TABLE workspace.gold.dim_cargo        ALTER COLUMN cargo_id        SET NOT NULL;
ALTER TABLE workspace.gold.dim_escolaridade ALTER COLUMN escolaridade_id SET NOT NULL;
ALTER TABLE workspace.gold.dim_colaborador  ALTER COLUMN colaborador_id  SET NOT NULL;
ALTER TABLE workspace.gold.fato_colaborador ALTER COLUMN colaborador_id  SET NOT NULL;

ALTER TABLE workspace.gold.dim_departamento ADD CONSTRAINT pk_dim_departamento PRIMARY KEY (departamento_id);
ALTER TABLE workspace.gold.dim_cargo        ADD CONSTRAINT pk_dim_cargo        PRIMARY KEY (cargo_id);
ALTER TABLE workspace.gold.dim_escolaridade ADD CONSTRAINT pk_dim_escolaridade PRIMARY KEY (escolaridade_id);
ALTER TABLE workspace.gold.dim_colaborador  ADD CONSTRAINT pk_dim_colaborador  PRIMARY KEY (colaborador_id);
ALTER TABLE workspace.gold.fato_colaborador ADD CONSTRAINT pk_fato_colaborador PRIMARY KEY (colaborador_id);

ALTER TABLE workspace.gold.fato_colaborador ADD CONSTRAINT fk_fato_departamento FOREIGN KEY (departamento_id) REFERENCES workspace.gold.dim_departamento;
ALTER TABLE workspace.gold.fato_colaborador ADD CONSTRAINT fk_fato_cargo        FOREIGN KEY (cargo_id)        REFERENCES workspace.gold.dim_cargo;
ALTER TABLE workspace.gold.fato_colaborador ADD CONSTRAINT fk_fato_escolaridade FOREIGN KEY (escolaridade_id) REFERENCES workspace.gold.dim_escolaridade;
ALTER TABLE workspace.gold.fato_colaborador ADD CONSTRAINT fk_fato_colaborador  FOREIGN KEY (colaborador_id)  REFERENCES workspace.gold.dim_colaborador;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência do modelo
-- MAGIC Esperado: fato com 1.470 linhas e nenhuma linha "órfã" (sem correspondência nas dimensões).

-- COMMAND ----------

SELECT 'dim_departamento' AS tabela, COUNT(*) AS linhas FROM workspace.gold.dim_departamento
UNION ALL SELECT 'dim_cargo',        COUNT(*) FROM workspace.gold.dim_cargo
UNION ALL SELECT 'dim_escolaridade', COUNT(*) FROM workspace.gold.dim_escolaridade
UNION ALL SELECT 'dim_colaborador',  COUNT(*) FROM workspace.gold.dim_colaborador
UNION ALL SELECT 'fato_colaborador', COUNT(*) FROM workspace.gold.fato_colaborador;

-- COMMAND ----------

SELECT COUNT(*) AS linhas_orfas
FROM workspace.gold.fato_colaborador f
LEFT JOIN workspace.gold.dim_colaborador  dc ON f.colaborador_id  = dc.colaborador_id
LEFT JOIN workspace.gold.dim_departamento dd ON f.departamento_id = dd.departamento_id
LEFT JOIN workspace.gold.dim_cargo        dg ON f.cargo_id        = dg.cargo_id
LEFT JOIN workspace.gold.dim_escolaridade de ON f.escolaridade_id = de.escolaridade_id
WHERE dc.colaborador_id IS NULL OR dd.departamento_id IS NULL OR dg.cargo_id IS NULL OR de.escolaridade_id IS NULL;
