-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - Silver: limpeza e padronização (ETL Bronze → Silver)
-- MAGIC
-- MAGIC **Entrada:** `workspace.bronze.rh_colaboradores_raw` (tudo texto, nomes em inglês)
-- MAGIC **Saída:** `workspace.silver.rh_colaboradores` (1 linha por colaborador, tipada e em português)
-- MAGIC
-- MAGIC Transformações aplicadas (cada uma justificada pelo notebook 02 - Qualidade):
-- MAGIC 1. **Deduplicação** por `EmployeeNumber` (mantém o registro mais recente pela data de ingestão) — proteção para recargas.
-- MAGIC 2. **Tipagem**: textos numéricos convertidos para `INT`.
-- MAGIC 3. **Flags 0/1**: `Attrition` (Yes/No) → `flag_desligado`; `OverTime` (Yes/No) → `flag_hora_extra`. Facilita calcular taxas com `AVG` e `SUM`.
-- MAGIC 4. **Remoção de colunas sem valor analítico**: `EmployeeCount`, `Over18`, `StandardHours` (constantes) e `DailyRate`, `HourlyRate`, `MonthlyRate` (sem documentação e incoerentes com o salário).
-- MAGIC 5. **Tradução e padronização** de categorias e nomes de colunas para português.
-- MAGIC 6. **Rótulos** para as escalas numéricas (ex.: satisfação 1 = Baixa ... 4 = Muito alta).
-- MAGIC 7. **Faixas** (idade, salário, tempo de casa, distância) para análises menos sensíveis a outliers.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.rh_colaboradores (
  colaborador_id                  INT     COMMENT 'Identificador unico do colaborador (EmployeeNumber na origem)',
  idade                           INT     COMMENT 'Idade em anos (18 a 60)',
  faixa_etaria                    STRING  COMMENT 'Faixa de idade: 18-25, 26-35, 36-45, 46-60',
  genero                          STRING  COMMENT 'Genero: Feminino, Masculino',
  estado_civil                    STRING  COMMENT 'Estado civil: Solteiro(a), Casado(a), Divorciado(a)',
  escolaridade_nivel              INT     COMMENT 'Nivel de escolaridade codificado de 1 a 5',
  escolaridade                    STRING  COMMENT 'Escolaridade: 1 Ensino medio, 2 Superior incompleto, 3 Graduacao, 4 Mestrado, 5 Doutorado',
  area_formacao                   STRING  COMMENT 'Area de formacao academica (6 categorias)',
  departamento                    STRING  COMMENT 'Departamento: Vendas, Pesquisa e Desenvolvimento, Recursos Humanos',
  cargo                           STRING  COMMENT 'Cargo ocupado (9 categorias)',
  nivel_cargo                     INT     COMMENT 'Nivel hierarquico do cargo de 1 (entrada) a 5 (senior)',
  frequencia_viagem               STRING  COMMENT 'Frequencia de viagens a trabalho: Nao viaja, Raramente, Frequentemente',
  distancia_casa                  INT     COMMENT 'Distancia de casa ate o trabalho (1 a 29, unidade nao informada pela fonte)',
  faixa_distancia                 STRING  COMMENT 'Faixa de distancia: 01-05, 06-10, 11-20, 21-29',
  salario_mensal                  INT     COMMENT 'Salario mensal (1.009 a 19.999; moeda nao informada pela fonte)',
  faixa_salarial                  STRING  COMMENT 'Faixa salarial mensal: 1 Ate 2.999, 2 3.000-5.999, 3 6.000-9.999, 4 10.000+',
  perc_ultimo_aumento             INT     COMMENT 'Percentual do ultimo aumento salarial (11 a 25)',
  nivel_stock_options             INT     COMMENT 'Nivel de participacao em acoes da empresa (0 a 3)',
  qtd_empresas_anteriores         INT     COMMENT 'Numero de empresas em que ja trabalhou (0 a 9)',
  anos_experiencia_total          INT     COMMENT 'Anos totais de experiencia profissional (0 a 40)',
  anos_empresa                    INT     COMMENT 'Anos de empresa (0 a 40)',
  faixa_tempo_casa                STRING  COMMENT 'Faixa de tempo de casa: 1 0-1 ano, 2 2-5 anos, 3 6-10 anos, 4 Mais de 10 anos',
  anos_cargo_atual                INT     COMMENT 'Anos no cargo atual (0 a 18)',
  anos_desde_promocao             INT     COMMENT 'Anos desde a ultima promocao (0 a 15)',
  anos_com_gestor                 INT     COMMENT 'Anos com o gestor atual (0 a 17)',
  qtd_treinamentos_ano            INT     COMMENT 'Quantidade de treinamentos no ultimo ano (0 a 6)',
  nota_satisfacao_trabalho        INT     COMMENT 'Satisfacao com o trabalho (1 Baixa a 4 Muito alta)',
  nota_satisfacao_ambiente        INT     COMMENT 'Satisfacao com o ambiente de trabalho (1 Baixa a 4 Muito alta)',
  nota_satisfacao_relacionamentos INT     COMMENT 'Satisfacao com relacionamentos no trabalho (1 Baixa a 4 Muito alta)',
  nota_envolvimento               INT     COMMENT 'Envolvimento com o trabalho (1 Baixo a 4 Muito alto)',
  nota_equilibrio_vida            INT     COMMENT 'Equilibrio vida pessoal e trabalho (1 Ruim a 4 Excelente)',
  nota_desempenho                 INT     COMMENT 'Avaliacao de desempenho (3 Excelente ou 4 Excepcional; sem notas 1 e 2 na base)',
  flag_hora_extra                 INT     COMMENT '1 = faz hora extra, 0 = nao faz (OverTime na origem)',
  flag_desligado                  INT     COMMENT '1 = saiu da empresa (turnover), 0 = permanece (Attrition na origem)',
  _data_ingestao                  TIMESTAMP COMMENT 'Linhagem: data e hora da ingestao na Bronze',
  _data_processamento             TIMESTAMP COMMENT 'Data e hora do processamento na Silver'
)
COMMENT 'Silver: colaboradores (IBM HR Attrition) limpos, tipados, traduzidos e enriquecidos com faixas. Origem: workspace.bronze.rh_colaboradores_raw';

-- COMMAND ----------

INSERT INTO workspace.silver.rh_colaboradores
WITH dedup AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY TRIM(EmployeeNumber) ORDER BY _data_ingestao DESC) AS rn
  FROM workspace.bronze.rh_colaboradores_raw
),
tipado AS (
  SELECT
    CAST(TRIM(EmployeeNumber) AS INT)          AS colaborador_id,
    CAST(Age AS INT)                           AS idade,
    TRIM(Gender)                               AS genero_en,
    TRIM(MaritalStatus)                        AS estado_civil_en,
    CAST(Education AS INT)                     AS escolaridade_nivel,
    TRIM(EducationField)                       AS area_formacao_en,
    TRIM(Department)                           AS departamento_en,
    TRIM(JobRole)                              AS cargo_en,
    CAST(JobLevel AS INT)                      AS nivel_cargo,
    TRIM(BusinessTravel)                       AS viagem_en,
    CAST(DistanceFromHome AS INT)              AS distancia_casa,
    CAST(MonthlyIncome AS INT)                 AS salario_mensal,
    CAST(PercentSalaryHike AS INT)             AS perc_ultimo_aumento,
    CAST(StockOptionLevel AS INT)              AS nivel_stock_options,
    CAST(NumCompaniesWorked AS INT)            AS qtd_empresas_anteriores,
    CAST(TotalWorkingYears AS INT)             AS anos_experiencia_total,
    CAST(YearsAtCompany AS INT)                AS anos_empresa,
    CAST(YearsInCurrentRole AS INT)            AS anos_cargo_atual,
    CAST(YearsSinceLastPromotion AS INT)       AS anos_desde_promocao,
    CAST(YearsWithCurrManager AS INT)          AS anos_com_gestor,
    CAST(TrainingTimesLastYear AS INT)         AS qtd_treinamentos_ano,
    CAST(JobSatisfaction AS INT)               AS nota_satisfacao_trabalho,
    CAST(EnvironmentSatisfaction AS INT)       AS nota_satisfacao_ambiente,
    CAST(RelationshipSatisfaction AS INT)      AS nota_satisfacao_relacionamentos,
    CAST(JobInvolvement AS INT)                AS nota_envolvimento,
    CAST(WorkLifeBalance AS INT)               AS nota_equilibrio_vida,
    CAST(PerformanceRating AS INT)             AS nota_desempenho,
    CASE WHEN UPPER(TRIM(OverTime))  = 'YES' THEN 1 ELSE 0 END AS flag_hora_extra,
    CASE WHEN UPPER(TRIM(Attrition)) = 'YES' THEN 1 ELSE 0 END AS flag_desligado,
    _data_ingestao
  FROM dedup
  WHERE rn = 1
)
SELECT
  colaborador_id,
  idade,
  CASE WHEN idade <= 25 THEN '18-25' WHEN idade <= 35 THEN '26-35'
       WHEN idade <= 45 THEN '36-45' ELSE '46-60' END                            AS faixa_etaria,
  CASE genero_en WHEN 'Female' THEN 'Feminino' WHEN 'Male' THEN 'Masculino' ELSE 'Nao informado' END AS genero,
  CASE estado_civil_en WHEN 'Single' THEN 'Solteiro(a)' WHEN 'Married' THEN 'Casado(a)'
       WHEN 'Divorced' THEN 'Divorciado(a)' ELSE 'Nao informado' END             AS estado_civil,
  escolaridade_nivel,
  CASE escolaridade_nivel WHEN 1 THEN 'Ensino medio' WHEN 2 THEN 'Superior incompleto'
       WHEN 3 THEN 'Graduacao' WHEN 4 THEN 'Mestrado' WHEN 5 THEN 'Doutorado' END AS escolaridade,
  CASE area_formacao_en WHEN 'Life Sciences' THEN 'Ciencias biologicas' WHEN 'Medical' THEN 'Saude'
       WHEN 'Marketing' THEN 'Marketing' WHEN 'Technical Degree' THEN 'Curso tecnico'
       WHEN 'Human Resources' THEN 'Recursos Humanos' ELSE 'Outra' END           AS area_formacao,
  CASE departamento_en WHEN 'Sales' THEN 'Vendas' WHEN 'Research & Development' THEN 'Pesquisa e Desenvolvimento'
       WHEN 'Human Resources' THEN 'Recursos Humanos' END                        AS departamento,
  CASE cargo_en
       WHEN 'Sales Executive'           THEN 'Executivo(a) de Vendas'
       WHEN 'Sales Representative'      THEN 'Representante de Vendas'
       WHEN 'Research Scientist'        THEN 'Cientista Pesquisador(a)'
       WHEN 'Laboratory Technician'     THEN 'Tecnico(a) de Laboratorio'
       WHEN 'Manufacturing Director'    THEN 'Diretor(a) de Manufatura'
       WHEN 'Healthcare Representative' THEN 'Representante de Saude'
       WHEN 'Manager'                   THEN 'Gerente'
       WHEN 'Research Director'         THEN 'Diretor(a) de Pesquisa'
       WHEN 'Human Resources'           THEN 'Analista de RH'
       ELSE cargo_en END                                                          AS cargo,
  nivel_cargo,
  CASE viagem_en WHEN 'Non-Travel' THEN 'Nao viaja' WHEN 'Travel_Rarely' THEN 'Raramente'
       WHEN 'Travel_Frequently' THEN 'Frequentemente' END                        AS frequencia_viagem,
  distancia_casa,
  CASE WHEN distancia_casa <= 5 THEN '01-05' WHEN distancia_casa <= 10 THEN '06-10'
       WHEN distancia_casa <= 20 THEN '11-20' ELSE '21-29' END                   AS faixa_distancia,
  salario_mensal,
  CASE WHEN salario_mensal < 3000 THEN '1. Ate 2.999' WHEN salario_mensal < 6000 THEN '2. 3.000-5.999'
       WHEN salario_mensal < 10000 THEN '3. 6.000-9.999' ELSE '4. 10.000+' END   AS faixa_salarial,
  perc_ultimo_aumento,
  nivel_stock_options,
  qtd_empresas_anteriores,
  anos_experiencia_total,
  anos_empresa,
  CASE WHEN anos_empresa <= 1 THEN '1. 0-1 ano' WHEN anos_empresa <= 5 THEN '2. 2-5 anos'
       WHEN anos_empresa <= 10 THEN '3. 6-10 anos' ELSE '4. Mais de 10 anos' END AS faixa_tempo_casa,
  anos_cargo_atual,
  anos_desde_promocao,
  anos_com_gestor,
  qtd_treinamentos_ano,
  nota_satisfacao_trabalho,
  nota_satisfacao_ambiente,
  nota_satisfacao_relacionamentos,
  nota_envolvimento,
  nota_equilibrio_vida,
  nota_desempenho,
  flag_hora_extra,
  flag_desligado,
  _data_ingestao,
  current_timestamp()                                                             AS _data_processamento
FROM tipado;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validação pós-transformação
-- MAGIC Esperado: 1.470 linhas, 1.470 IDs distintos, nenhuma categoria sem tradução (nulos = 0).

-- COMMAND ----------

SELECT
  COUNT(*)                                                         AS total_linhas,
  COUNT(DISTINCT colaborador_id)                                   AS ids_distintos,
  SUM(CASE WHEN departamento IS NULL OR cargo IS NULL OR escolaridade IS NULL
            OR frequencia_viagem IS NULL THEN 1 ELSE 0 END)        AS linhas_com_categoria_nula,
  SUM(CASE WHEN genero = 'Nao informado' OR estado_civil = 'Nao informado' THEN 1 ELSE 0 END) AS linhas_nao_informado,
  SUM(flag_desligado)                                              AS total_desligados
FROM workspace.silver.rh_colaboradores;

-- COMMAND ----------

SELECT * FROM workspace.silver.rh_colaboradores LIMIT 10;
