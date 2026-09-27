-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Análise: respondendo às perguntas de negócio
-- MAGIC
-- MAGIC **Problema:** *Quais fatores estão mais associados ao desligamento (turnover) de colaboradores, e onde o RH deve priorizar ações de retenção?*
-- MAGIC
-- MAGIC Métrica principal: **taxa de turnover (%)** = desligados ÷ total de colaboradores do grupo × 100.
-- MAGIC Todas as consultas usam a camada **Gold** (modelo estrela).
-- MAGIC
-- MAGIC > Dica: em cada resultado, clique em **+ → Visualization** para gerar um gráfico de barras e tirar o print.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P1. Qual é a taxa geral de turnover e como ela varia por departamento e por cargo?

-- COMMAND ----------

SELECT COUNT(*) AS colaboradores, SUM(flag_desligado) AS desligados,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador;

-- COMMAND ----------

SELECT d.departamento, COUNT(*) AS colaboradores, SUM(f.flag_desligado) AS desligados,
       ROUND(100.0 * AVG(f.flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador f
JOIN workspace.gold.dim_departamento d ON f.departamento_id = d.departamento_id
GROUP BY d.departamento
ORDER BY taxa_turnover_pct DESC;

-- COMMAND ----------

SELECT c.cargo, COUNT(*) AS colaboradores, SUM(f.flag_desligado) AS desligados,
       ROUND(100.0 * AVG(f.flag_desligado), 1) AS taxa_turnover_pct,
       ROUND(AVG(f.salario_mensal), 0) AS salario_medio
FROM workspace.gold.fato_colaborador f
JOIN workspace.gold.dim_cargo c ON f.cargo_id = c.cargo_id
GROUP BY c.cargo
ORDER BY taxa_turnover_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P2. Quem faz hora extra sai mais da empresa?

-- COMMAND ----------

SELECT CASE WHEN flag_hora_extra = 1 THEN 'Faz hora extra' ELSE 'Nao faz hora extra' END AS hora_extra,
       COUNT(*) AS colaboradores, SUM(flag_desligado) AS desligados,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY 1
ORDER BY taxa_turnover_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P3. O salário de quem saiu é menor? Em quais faixas salariais o turnover é maior?

-- COMMAND ----------

SELECT CASE WHEN flag_desligado = 1 THEN 'Desligados' ELSE 'Ativos' END AS status,
       COUNT(*) AS colaboradores,
       ROUND(AVG(salario_mensal), 0) AS salario_medio,
       percentile(salario_mensal, 0.5) AS salario_mediano
FROM workspace.gold.fato_colaborador
GROUP BY 1;

-- COMMAND ----------

SELECT faixa_salarial, COUNT(*) AS colaboradores, SUM(flag_desligado) AS desligados,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY faixa_salarial
ORDER BY faixa_salarial;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P4. Em que momento da jornada o risco de saída é maior (tempo de casa e idade)?

-- COMMAND ----------

SELECT faixa_tempo_casa, COUNT(*) AS colaboradores, SUM(flag_desligado) AS desligados,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY faixa_tempo_casa
ORDER BY faixa_tempo_casa;

-- COMMAND ----------

SELECT dc.faixa_etaria, COUNT(*) AS colaboradores, SUM(f.flag_desligado) AS desligados,
       ROUND(100.0 * AVG(f.flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador f
JOIN workspace.gold.dim_colaborador dc ON f.colaborador_id = dc.colaborador_id
GROUP BY dc.faixa_etaria
ORDER BY dc.faixa_etaria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P5. Satisfação, envolvimento e equilíbrio vida-trabalho estão relacionados ao desligamento?
-- MAGIC Taxa de turnover por nota (1 = pior, 4 = melhor) em cada dimensão de clima.

-- COMMAND ----------

SELECT 'Satisfacao com o trabalho' AS indicador, nota_satisfacao_trabalho AS nota, COUNT(*) AS colaboradores,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador GROUP BY nota_satisfacao_trabalho
UNION ALL
SELECT 'Satisfacao com o ambiente', nota_satisfacao_ambiente, COUNT(*), ROUND(100.0 * AVG(flag_desligado), 1)
FROM workspace.gold.fato_colaborador GROUP BY nota_satisfacao_ambiente
UNION ALL
SELECT 'Satisfacao com relacionamentos', nota_satisfacao_relacionamentos, COUNT(*), ROUND(100.0 * AVG(flag_desligado), 1)
FROM workspace.gold.fato_colaborador GROUP BY nota_satisfacao_relacionamentos
UNION ALL
SELECT 'Envolvimento com o trabalho', nota_envolvimento, COUNT(*), ROUND(100.0 * AVG(flag_desligado), 1)
FROM workspace.gold.fato_colaborador GROUP BY nota_envolvimento
UNION ALL
SELECT 'Equilibrio vida-trabalho', nota_equilibrio_vida, COUNT(*), ROUND(100.0 * AVG(flag_desligado), 1)
FROM workspace.gold.fato_colaborador GROUP BY nota_equilibrio_vida
ORDER BY indicador, nota;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P6. Tempo sem promoção, distância de casa e viagens influenciam a saída?

-- COMMAND ----------

SELECT CASE WHEN anos_desde_promocao = 0 THEN '1. Menos de 1 ano (inclui recem-contratados)'
            WHEN anos_desde_promocao <= 3 THEN '2. 1-3 anos sem promocao'
            WHEN anos_desde_promocao <= 6 THEN '3. 4-6 anos sem promocao'
            ELSE '4. 7+ anos sem promocao' END AS tempo_sem_promocao,
       COUNT(*) AS colaboradores, ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

SELECT faixa_distancia, COUNT(*) AS colaboradores, ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY faixa_distancia ORDER BY faixa_distancia;

-- COMMAND ----------

SELECT dc.frequencia_viagem, COUNT(*) AS colaboradores, ROUND(100.0 * AVG(f.flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador f
JOIN workspace.gold.dim_colaborador dc ON f.colaborador_id = dc.colaborador_id
GROUP BY dc.frequencia_viagem ORDER BY taxa_turnover_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P7. Qual perfil concentra o maior risco? (combinação de fatores)
-- MAGIC Cruzamos os três fatores mais fortes das perguntas anteriores: hora extra, faixa salarial e tempo de casa. Mostramos apenas grupos com pelo menos 20 pessoas para evitar conclusões baseadas em poucos casos.

-- COMMAND ----------

SELECT CASE WHEN flag_hora_extra = 1 THEN 'Sim' ELSE 'Nao' END AS hora_extra,
       faixa_salarial, faixa_tempo_casa,
       COUNT(*) AS colaboradores, SUM(flag_desligado) AS desligados,
       ROUND(100.0 * AVG(flag_desligado), 1) AS taxa_turnover_pct
FROM workspace.gold.fato_colaborador
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 20
ORDER BY taxa_turnover_pct DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Painel visual (resumo)
-- MAGIC A célula abaixo, em Python, apenas **lê as tabelas Gold** e desenha 4 gráficos de taxa de turnover para o resumo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC def taxa(sql):
-- MAGIC     return spark.sql(sql).toPandas()
-- MAGIC
-- MAGIC base = "FROM workspace.gold.fato_colaborador f "
-- MAGIC graficos = [
-- MAGIC     ("Hora extra", taxa("SELECT CASE WHEN flag_hora_extra=1 THEN 'Faz' ELSE 'Nao faz' END AS grupo, ROUND(100*AVG(flag_desligado),1) AS taxa " + base + "GROUP BY 1 ORDER BY 1")),
-- MAGIC     ("Faixa salarial", taxa("SELECT faixa_salarial AS grupo, ROUND(100*AVG(flag_desligado),1) AS taxa " + base + "GROUP BY 1 ORDER BY 1")),
-- MAGIC     ("Tempo de casa", taxa("SELECT faixa_tempo_casa AS grupo, ROUND(100*AVG(flag_desligado),1) AS taxa " + base + "GROUP BY 1 ORDER BY 1")),
-- MAGIC     ("Departamento", taxa("SELECT d.departamento AS grupo, ROUND(100*AVG(flag_desligado),1) AS taxa " + base + "JOIN workspace.gold.dim_departamento d USING (departamento_id) GROUP BY 1 ORDER BY 2 DESC")),
-- MAGIC ]
-- MAGIC geral = float(taxa("SELECT ROUND(100*AVG(flag_desligado),1) AS taxa " + base)["taxa"][0])
-- MAGIC
-- MAGIC fig, axes = plt.subplots(2, 2, figsize=(13, 8))
-- MAGIC for ax, (titulo, df) in zip(axes.flat, graficos):
-- MAGIC     barras = ax.barh(df["grupo"].astype(str), df["taxa"].astype(float), color="#2a5d9f")
-- MAGIC     ax.axvline(geral, color="#b03a2e", linestyle="--", linewidth=1, label=f"Media geral {geral}%")
-- MAGIC     ax.bar_label(barras, fmt="%.1f%%", padding=3)
-- MAGIC     ax.set_title(f"Taxa de turnover por {titulo.lower()}")
-- MAGIC     ax.invert_yaxis(); ax.set_xlabel("% de desligados"); ax.legend(loc="lower right")
-- MAGIC     ax.spines[["top", "right"]].set_visible(False)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
