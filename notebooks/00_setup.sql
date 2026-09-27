-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup do ambiente
-- MAGIC
-- MAGIC **MVP: Pipeline de Dados de Turnover (RH)**
-- MAGIC
-- MAGIC Este notebook prepara a estrutura do Lakehouse no Unity Catalog seguindo a **Arquitetura Medalhão**:
-- MAGIC
-- MAGIC | Schema | Camada | Conteúdo |
-- MAGIC |---|---|---|
-- MAGIC | `workspace.bronze` | Bronze | Arquivo CSV original (volume) e tabela bruta, sem alterações |
-- MAGIC | `workspace.silver` | Silver | Dados limpos, tipados e padronizados |
-- MAGIC | `workspace.gold` | Gold | Modelo estrela (fato + dimensões) pronto para análise |
-- MAGIC
-- MAGIC Obs.: no Databricks Free Edition o catálogo padrão se chama `workspace`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze COMMENT 'Camada Bronze: dados brutos de RH exatamente como recebidos da fonte';
CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Camada Silver: dados de RH limpos, tipados e padronizados';
CREATE SCHEMA IF NOT EXISTS workspace.gold   COMMENT 'Camada Gold: modelo estrela de turnover pronto para consumo analitico';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Volume para o arquivo bruto
-- MAGIC Um *volume* é uma pasta gerenciada pelo Unity Catalog onde guardamos arquivos (CSV, JSON...). É aqui que o arquivo baixado do Kaggle será enviado (upload).

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.bronze.arquivos_brutos
COMMENT 'Arquivos originais recebidos da fonte (Kaggle - IBM HR Analytics Employee Attrition)';

-- COMMAND ----------

SHOW SCHEMAS IN workspace;
