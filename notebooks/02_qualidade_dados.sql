-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Qualidade de Dados (sobre a camada Bronze)
-- MAGIC
-- MAGIC Antes de transformar, verificamos a qualidade de **cada atributo** do dado bruto, nas 5 dimensões pedidas:
-- MAGIC
-- MAGIC | Dimensão | Pergunta | Onde |
-- MAGIC |---|---|---|
-- MAGIC | Completude | Existem valores nulos ou vazios? | Célula 1 (perfil por atributo) |
-- MAGIC | Consistência | Valores seguem o padrão/tipo esperado? Categorias estão padronizadas? | Células 1 e 3 |
-- MAGIC | Unicidade | Existem colaboradores ou linhas duplicadas? | Célula 2 |
-- MAGIC | Acurácia | Os valores fazem sentido no contexto de RH? | Célula 4 |
-- MAGIC | Outliers | Existem valores extremos que distorcem análises? | Célula 5 |
-- MAGIC
-- MAGIC Os achados e o tratamento dado a cada um estão resumidos no final deste notebook e no README.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Perfil de cada atributo (completude, tipo, domínio)
-- MAGIC Para cada uma das 35 colunas: total de linhas, nulos/vazios, quantidade de valores distintos, se o conteúdo é inteiramente numérico ou texto, e mínimo/máximo. Colunas que deveriam ser numéricas e aparecessem como `texto` indicariam valores inválidos.

-- COMMAND ----------

-- Transforma a tabela em formato "longo" (1 linha por atributo x colaborador) para medir todos os atributos de uma vez
WITH longo AS (
  SELECT atributo, valor
  FROM workspace.bronze.rh_colaboradores_raw
  UNPIVOT INCLUDE NULLS (valor FOR atributo IN (
    Age, Attrition, BusinessTravel, DailyRate, Department, DistanceFromHome, Education, EducationField, EmployeeCount, EmployeeNumber, EnvironmentSatisfaction, Gender, HourlyRate, JobInvolvement, JobLevel, JobRole, JobSatisfaction, MaritalStatus, MonthlyIncome, MonthlyRate, NumCompaniesWorked, Over18, OverTime, PercentSalaryHike, PerformanceRating, RelationshipSatisfaction, StandardHours, StockOptionLevel, TotalWorkingYears, TrainingTimesLastYear, WorkLifeBalance, YearsAtCompany, YearsInCurrentRole, YearsSinceLastPromotion, YearsWithCurrManager
  ))
)
SELECT
  atributo,
  COUNT(*)                                                                        AS total_linhas,
  SUM(CASE WHEN valor IS NULL OR TRIM(valor) = '' THEN 1 ELSE 0 END)              AS qtd_nulos_ou_vazios,
  COUNT(DISTINCT valor)                                                           AS qtd_valores_distintos,
  CASE WHEN SUM(CASE WHEN TRY_CAST(valor AS INT) IS NULL THEN 1 ELSE 0 END) = 0
       THEN 'numerica' ELSE 'texto' END                                           AS tipo_identificado,
  MIN(TRY_CAST(valor AS INT))                                                     AS valor_minimo,
  MAX(TRY_CAST(valor AS INT))                                                     AS valor_maximo
FROM longo
GROUP BY atributo
ORDER BY atributo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Unicidade
-- MAGIC `EmployeeNumber` é o identificador do colaborador e não pode se repetir. Também verificamos linhas 100% duplicadas.

-- COMMAND ----------

SELECT
  COUNT(*)                                        AS total_linhas,
  COUNT(DISTINCT EmployeeNumber)                  AS colaboradores_distintos,
  COUNT(*) - COUNT(DISTINCT EmployeeNumber)       AS ids_duplicados,
  COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * EXCEPT (_data_ingestao, _arquivo_origem) FROM workspace.bronze.rh_colaboradores_raw) d) AS linhas_duplicadas
FROM workspace.bronze.rh_colaboradores_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Consistência das categorias
-- MAGIC Listamos os valores de cada coluna de texto para checar grafias diferentes, espaços, maiúsculas/minúsculas etc.

-- COMMAND ----------

SELECT 'Attrition' AS atributo, Attrition AS valor, COUNT(*) AS qtd FROM workspace.bronze.rh_colaboradores_raw GROUP BY Attrition
UNION ALL SELECT 'BusinessTravel', BusinessTravel, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY BusinessTravel
UNION ALL SELECT 'Department', Department, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY Department
UNION ALL SELECT 'EducationField', EducationField, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY EducationField
UNION ALL SELECT 'Gender', Gender, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY Gender
UNION ALL SELECT 'JobRole', JobRole, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY JobRole
UNION ALL SELECT 'MaritalStatus', MaritalStatus, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY MaritalStatus
UNION ALL SELECT 'Over18', Over18, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY Over18
UNION ALL SELECT 'OverTime', OverTime, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY OverTime
UNION ALL SELECT 'PerformanceRating', PerformanceRating, COUNT(*) FROM workspace.bronze.rh_colaboradores_raw GROUP BY PerformanceRating
ORDER BY atributo, valor;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Acurácia: regras de negócio de RH
-- MAGIC Cada regra abaixo deveria ter **0 violações** se os dados fizerem sentido:
-- MAGIC - Tempo de empresa não pode ser maior que a experiência total.
-- MAGIC - Tempo no cargo atual, tempo desde a última promoção e tempo com o gestor atual não podem ser maiores que o tempo de empresa.
-- MAGIC - Idade entre 18 e 70 anos e idade de início de carreira (idade - experiência) de pelo menos 14 anos.
-- MAGIC - Salário mensal positivo e percentual de aumento entre 0 e 100.

-- COMMAND ----------

WITH t AS (
  SELECT
    CAST(Age AS INT) AS idade, CAST(TotalWorkingYears AS INT) AS exp_total,
    CAST(YearsAtCompany AS INT) AS anos_empresa, CAST(YearsInCurrentRole AS INT) AS anos_cargo,
    CAST(YearsSinceLastPromotion AS INT) AS anos_promo, CAST(YearsWithCurrManager AS INT) AS anos_gestor,
    CAST(MonthlyIncome AS INT) AS salario, CAST(PercentSalaryHike AS INT) AS aumento
  FROM workspace.bronze.rh_colaboradores_raw
)
SELECT 'Anos de empresa > experiencia total'        AS regra, SUM(CASE WHEN anos_empresa > exp_total THEN 1 ELSE 0 END) AS violacoes FROM t
UNION ALL SELECT 'Anos no cargo atual > anos de empresa',     SUM(CASE WHEN anos_cargo  > anos_empresa THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Anos desde promocao > anos de empresa',     SUM(CASE WHEN anos_promo  > anos_empresa THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Anos com gestor atual > anos de empresa',   SUM(CASE WHEN anos_gestor > anos_empresa THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Idade fora de 18-70',                       SUM(CASE WHEN idade < 18 OR idade > 70 THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Inicio de carreira antes dos 14 anos',      SUM(CASE WHEN idade - exp_total < 14 THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Salario mensal <= 0',                       SUM(CASE WHEN salario <= 0 THEN 1 ELSE 0 END) FROM t
UNION ALL SELECT 'Percentual de aumento fora de 0-100',       SUM(CASE WHEN aumento < 0 OR aumento > 100 THEN 1 ELSE 0 END) FROM t;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Outliers (método do intervalo interquartil - IQR)
-- MAGIC Um valor é considerado *outlier* quando está abaixo de Q1 - 1,5×IQR ou acima de Q3 + 1,5×IQR.

-- COMMAND ----------

WITH v AS (
  SELECT 'MonthlyIncome' AS atributo, CAST(MonthlyIncome AS DOUBLE) AS valor FROM workspace.bronze.rh_colaboradores_raw
  UNION ALL SELECT 'YearsAtCompany',          CAST(YearsAtCompany AS DOUBLE)          FROM workspace.bronze.rh_colaboradores_raw
  UNION ALL SELECT 'TotalWorkingYears',       CAST(TotalWorkingYears AS DOUBLE)       FROM workspace.bronze.rh_colaboradores_raw
  UNION ALL SELECT 'YearsSinceLastPromotion', CAST(YearsSinceLastPromotion AS DOUBLE) FROM workspace.bronze.rh_colaboradores_raw
  UNION ALL SELECT 'DistanceFromHome',        CAST(DistanceFromHome AS DOUBLE)        FROM workspace.bronze.rh_colaboradores_raw
  UNION ALL SELECT 'Age',                     CAST(Age AS DOUBLE)                     FROM workspace.bronze.rh_colaboradores_raw
),
q AS (
  SELECT atributo, percentile(valor, 0.25) AS q1, percentile(valor, 0.75) AS q3
  FROM v GROUP BY atributo
)
SELECT q.atributo, q.q1, q.q3,
       q.q3 + 1.5 * (q.q3 - q.q1) AS limite_superior,
       SUM(CASE WHEN v.valor > q.q3 + 1.5 * (q.q3 - q.q1) OR v.valor < q.q1 - 1.5 * (q.q3 - q.q1) THEN 1 ELSE 0 END) AS qtd_outliers,
       MAX(v.valor) AS valor_maximo
FROM v JOIN q ON v.atributo = q.atributo
GROUP BY q.atributo, q.q1, q.q3
ORDER BY qtd_outliers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Resumo dos achados e tratamento
-- MAGIC
-- MAGIC | # | Problema encontrado | Dimensão | Tratamento (na Silver) |
-- MAGIC |---|---|---|---|
-- MAGIC | 1 | Caractere invisível (BOM) no início do cabeçalho do CSV | Consistência | Esquema informado explicitamente na leitura (notebook 01) |
-- MAGIC | 2 | Todas as colunas chegam como texto | Consistência | Conversão para INT; `Attrition` e `OverTime` (Yes/No) viram flags 0/1 |
-- MAGIC | 3 | `EmployeeCount` (sempre 1), `Over18` (sempre Y) e `StandardHours` (sempre 80) são constantes | Completude/utilidade | Removidas: não trazem informação |
-- MAGIC | 4 | `DailyRate`, `HourlyRate` e `MonthlyRate` sem documentação na fonte e sem relação coerente com `MonthlyIncome` | Acurácia | Removidas do modelo; o salário usado é `MonthlyIncome` |
-- MAGIC | 5 | Escalas codificadas em números (1 a 4 / 1 a 5) sem rótulo | Consistência | Criados rótulos em português (ex.: 1 = Baixa, 4 = Muito alta) |
-- MAGIC | 6 | `PerformanceRating` só tem notas 3 e 4 (nenhuma avaliação baixa) | Acurácia | Mantida, mas **não usada** como fator explicativo (pouca variação) |
-- MAGIC | 7 | Outliers em salário, tempo de empresa e tempo sem promoção | Outliers | Mantidos (são valores plausíveis, ex.: diretores). Análises usam **faixas** e taxas, menos sensíveis a extremos |
-- MAGIC | 8 | Nulos, IDs duplicados, linhas duplicadas e violações de regras de negócio | Completude/Unicidade/Acurácia | Nenhum encontrado (ver resultados acima). Mesmo assim a Silver aplica deduplicação por `EmployeeNumber` como proteção para cargas futuras |
