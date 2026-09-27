-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 99 - Executar o pipeline completo
-- MAGIC Roda todas as etapas em ordem (Bronze → Qualidade → Silver → Gold). Útil para reprocessar tudo quando chegar um arquivo novo no volume.
-- MAGIC Pré-requisito: o arquivo CSV já deve estar em `/Volumes/workspace/bronze/arquivos_brutos/`.

-- COMMAND ----------

-- MAGIC %run ./00_setup

-- COMMAND ----------

-- MAGIC %run ./01_bronze_ingestao

-- COMMAND ----------

-- MAGIC %run ./03_silver_transformacao

-- COMMAND ----------

-- MAGIC %run ./04_gold_modelagem

-- COMMAND ----------

SELECT 'bronze.rh_colaboradores_raw' AS tabela, COUNT(*) AS linhas FROM workspace.bronze.rh_colaboradores_raw
UNION ALL SELECT 'silver.rh_colaboradores', COUNT(*) FROM workspace.silver.rh_colaboradores
UNION ALL SELECT 'gold.fato_colaborador',   COUNT(*) FROM workspace.gold.fato_colaborador;
