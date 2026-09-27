-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Bronze: ingestão do dado bruto
-- MAGIC
-- MAGIC **Objetivo:** ler o CSV original que foi enviado para o volume `workspace.bronze.arquivos_brutos` e gravá-lo como tabela Delta **sem nenhuma transformação de conteúdo**.
-- MAGIC
-- MAGIC Decisões:
-- MAGIC - Todas as colunas são lidas como **texto (STRING)**. Assim, preservamos o dado exatamente como veio; a tipagem acontece na Silver.
-- MAGIC - O esquema (nomes das 35 colunas) é informado explicitamente. Motivo: o arquivo original tem um caractere invisível (BOM, `U+FEFF`) no início do cabeçalho, que "grudaria" no nome da primeira coluna (`Age`). Informando o esquema, o problema é evitado sem alterar os dados.
-- MAGIC - São adicionados dois **metadados de controle**: data/hora da ingestão e arquivo de origem (rastreabilidade).

-- COMMAND ----------

LIST '/Volumes/workspace/bronze/arquivos_brutos/';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.bronze.rh_colaboradores_raw
COMMENT 'Bronze: copia fiel do arquivo WA_Fn-UseC_-HR-Employee-Attrition.csv (IBM HR Analytics, Kaggle). Todas as colunas como texto, mais metadados de ingestao.'
AS
SELECT
  *,
  current_timestamp()   AS _data_ingestao,
  _metadata.file_name   AS _arquivo_origem
FROM read_files(
  '/Volumes/workspace/bronze/arquivos_brutos/',
  format => 'csv',
  header => true,
  schema => 'Age STRING, Attrition STRING, BusinessTravel STRING, DailyRate STRING, Department STRING, DistanceFromHome STRING, Education STRING, EducationField STRING, EmployeeCount STRING, EmployeeNumber STRING, EnvironmentSatisfaction STRING, Gender STRING, HourlyRate STRING, JobInvolvement STRING, JobLevel STRING, JobRole STRING, JobSatisfaction STRING, MaritalStatus STRING, MonthlyIncome STRING, MonthlyRate STRING, NumCompaniesWorked STRING, Over18 STRING, OverTime STRING, PercentSalaryHike STRING, PerformanceRating STRING, RelationshipSatisfaction STRING, StandardHours STRING, StockOptionLevel STRING, TotalWorkingYears STRING, TrainingTimesLastYear STRING, WorkLifeBalance STRING, YearsAtCompany STRING, YearsInCurrentRole STRING, YearsSinceLastPromotion STRING, YearsWithCurrManager STRING'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conferência da carga
-- MAGIC Esperado: **1.470 linhas** (mesmo número de registros do arquivo original).

-- COMMAND ----------

SELECT COUNT(*) AS total_linhas, MIN(_data_ingestao) AS data_ingestao, MIN(_arquivo_origem) AS arquivo
FROM workspace.bronze.rh_colaboradores_raw;

-- COMMAND ----------

SELECT * FROM workspace.bronze.rh_colaboradores_raw LIMIT 10;
