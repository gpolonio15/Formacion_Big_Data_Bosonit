// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # M&M Scala-Python-Sql

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC mnm_file = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

// COMMAND ----------

// MAGIC %md # Leer el csv

// COMMAND ----------

// MAGIC %python
// MAGIC mnm_df = (spark
// MAGIC           .read
// MAGIC           .format("csv")
// MAGIC           .option("header", "true")
// MAGIC           .option("inferSchema", "true")
// MAGIC           .load(mnm_file))
// MAGIC 
// MAGIC display(mnm_df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por colores y groupBy por estado y color, orderBy en orden ascendente

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_asc = (mnm_df
// MAGIC                 .select("State", "Color", "Count")
// MAGIC                 .groupBy("State", "Color")
// MAGIC                 .agg(count("Count").alias("Total"))
// MAGIC                 .orderBy("Total", ascending=True))
// MAGIC 
// MAGIC count_mnm_df_asc.show(n=60, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Número de filas en la agrupación

// COMMAND ----------

// MAGIC %python
// MAGIC print(f"Total filas = {count_mnm_df_asc.count()}")

// COMMAND ----------

// MAGIC %md ### Recuento por colores y groupBy por estado y color, orderBy en orden descendente

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_desc = (mnm_df
// MAGIC                 .select("State", "Color", "Count")
// MAGIC                 .groupBy("State", "Color")
// MAGIC                 .agg(count("Count").alias("Total"))
// MAGIC                 .orderBy("Total", ascending=False))
// MAGIC count_mnm_df_desc.show(n=60) 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Agrupamos por colores solo.

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_color = (mnm_df
// MAGIC                 .select("Color", "Count")
// MAGIC                 .groupBy("Color")
// MAGIC                 .agg(count("Count").alias("Total"))
// MAGIC                 .orderBy("Total", ascending=False))
// MAGIC count_mnm_df_color.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Agrupamos por estados solo.

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_state = (mnm_df
// MAGIC                 .select("State", "Count")
// MAGIC                 .groupBy("State")
// MAGIC                 .agg(count("Count").alias("Total"))
// MAGIC                 .orderBy("Total", ascending=False))
// MAGIC count_mnm_df_state.show(n=60)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Lo máximo pedido por estado y color, en un pedido.

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_max = (mnm_df
// MAGIC                 .select("State","Color", "Count")
// MAGIC                 .groupBy("State","Color")
// MAGIC                 .agg(max("Count").alias("Total"))
// MAGIC                 .orderBy("Total", ascending=False))
// MAGIC count_mnm_df_max.show(n=60)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por estados utilizando ubn filtro Where.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### California

// COMMAND ----------

// MAGIC %python
// MAGIC ca_count_mnm_df_CA = (mnm_df
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(mnm_df.State == "CA") 
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy("Total", ascending=False))
// MAGIC 
// MAGIC ca_count_mnm_df_CA.show(n=10, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Washigton

// COMMAND ----------

// MAGIC %python
// MAGIC ca_count_mnm_df_WA = (mnm_df
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(mnm_df.State == "WA") 
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy("Total", ascending=False))
// MAGIC 
// MAGIC ca_count_mnm_df_WA.show(n=10, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Orlando

// COMMAND ----------

// MAGIC %python
// MAGIC ca_count_mnm_df_OR = (mnm_df
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(mnm_df.State == "OR") 
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy("Total", ascending=False))
// MAGIC 
// MAGIC ca_count_mnm_df_OR.show(n=10, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Utah

// COMMAND ----------

// MAGIC %python
// MAGIC ca_count_mnm_df_UT = (mnm_df
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(mnm_df.State == "UT") 
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy("Total", ascending=False))
// MAGIC 
// MAGIC ca_count_mnm_df_UT.show(n=10, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculemos la media, mínimo, máximo y total por color y estado.

// COMMAND ----------

// MAGIC %python
// MAGIC count_mnm_df_completo = (mnm_df
// MAGIC                 .select("State", "Color", "Count")
// MAGIC                 .groupBy("State", "Color")
// MAGIC                 .agg(count("Count").alias("Total"), 
// MAGIC                      avg("Count").alias("Media"), 
// MAGIC                      min("Count").alias("Mínimo"),
// MAGIC                      max("Count").alias("Máximo"))
// MAGIC                 .orderBy("Total", ascending=False))
// MAGIC 
// MAGIC count_mnm_df_completo.show(n=60, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC # Scala

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Haremos los mismos ejercicios, pero utilizando el lenguaje scala

// COMMAND ----------

// MAGIC %md
// MAGIC #### Cargamos el csv y lo abrimos

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %scala
// MAGIC val mnmDF = spark
// MAGIC   .read
// MAGIC   .format("csv")
// MAGIC   .option("header", "true")
// MAGIC   .option("inferSchema", "true")
// MAGIC   .load("/databricks-datasets/learning-spark-v2/mnm_dataset.csv")
// MAGIC 
// MAGIC display(mnmDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por colores y groupBy por estado y color, orderBy en orden ascendente

// COMMAND ----------

// MAGIC %scala
// MAGIC val countMnMDF_asc = mnmDF
// MAGIC   .select("State", "Color", "Count")
// MAGIC   .groupBy("State", "Color")
// MAGIC   .agg(count("Count").alias("Total"))
// MAGIC   .orderBy(asc("Total"))
// MAGIC 
// MAGIC countMnMDF_asc.show(60)

// COMMAND ----------

// MAGIC %md ### Número total de filas

// COMMAND ----------

// MAGIC %scala
// MAGIC println(s"Total Rows = ${countMnMDF_asc.count()}")

// COMMAND ----------

// MAGIC %md ### Recuento por colores y groupBy por estado y color, orderBy en orden descendente.

// COMMAND ----------

// MAGIC %scala
// MAGIC val countMnMDF_asc = mnmDF
// MAGIC   .select("State", "Color", "Count")
// MAGIC   .groupBy("State", "Color")
// MAGIC   .agg(count("Count").alias("Total"))
// MAGIC   .orderBy(desc("Total"))
// MAGIC 
// MAGIC countMnMDF_asc.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Agrupamos por colores solo.

// COMMAND ----------

// MAGIC %scala
// MAGIC val countMnMDF_color = mnmDF
// MAGIC   .select("Color", "Count")
// MAGIC   .groupBy("Color")
// MAGIC   .agg(count("Count").alias("Total"))
// MAGIC   .orderBy(desc("Total"))
// MAGIC 
// MAGIC countMnMDF_color.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Agrupamos por estados solo.

// COMMAND ----------

// MAGIC %scala
// MAGIC val countMnMDF_state = mnmDF
// MAGIC   .select("State", "Count")
// MAGIC   .groupBy("State")
// MAGIC   .agg(count("Count").alias("Total"))
// MAGIC   .orderBy(desc("Total"))
// MAGIC 
// MAGIC countMnMDF_state.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Lo máximo pedido por estado y color, en un pedido.

// COMMAND ----------

// MAGIC %scala
// MAGIC val  countMnMDF_max = (mnmDF
// MAGIC                 .select("State","Color", "Count")
// MAGIC                 .groupBy("State","Color")
// MAGIC                 .agg(max("Count").alias("Total"))
// MAGIC                 .orderBy(desc("Total")))
// MAGIC countMnMDF_max.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por estados utilizando ubn filtro Where.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### California

// COMMAND ----------

// MAGIC %scala
// MAGIC val ca_count_mnm_df_CA = (mnmDF
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(col("State") === "CA")
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy(desc("Total")))
// MAGIC 
// MAGIC ca_count_mnm_df_CA.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Washigton

// COMMAND ----------

// MAGIC %scala
// MAGIC val ca_count_mnm_df_WA = (mnmDF
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(col("State") === "WA")
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy(desc("Total")))
// MAGIC 
// MAGIC ca_count_mnm_df_WA.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Orlando

// COMMAND ----------

// MAGIC %scala
// MAGIC val ca_count_mnm_df_WA = (mnmDF
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(col("State") === "OR")
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy(desc("Total")))
// MAGIC 
// MAGIC ca_count_mnm_df_WA.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Utah

// COMMAND ----------

// MAGIC %scala
// MAGIC val ca_count_mnm_df_WA = (mnmDF
// MAGIC                    .select("State", "Color", "Count") 
// MAGIC                    .where(col("State") === "UT")
// MAGIC                    .groupBy("State", "Color") 
// MAGIC                    .agg(count("Count").alias("Total")) 
// MAGIC                    .orderBy(desc("Total")))
// MAGIC 
// MAGIC ca_count_mnm_df_WA.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculemos la media, mínimo, máximo y total por color y estado.

// COMMAND ----------

// MAGIC %scala
// MAGIC val count_mnm_df_completo = (mnmDF
// MAGIC                 .select("State", "Color", "Count")
// MAGIC                 .groupBy("State", "Color")
// MAGIC                 .agg(count("Count").alias("Total"), 
// MAGIC                      avg("Count").alias("Media"), 
// MAGIC                      min("Count").alias("Mínimo"),
// MAGIC                      max("Count").alias("Máximo"))
// MAGIC                 .orderBy(desc("Total")))
// MAGIC 
// MAGIC count_mnm_df_completo.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC # SQL

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cargamos los datos

// COMMAND ----------

// MAGIC %python
// MAGIC df_mnm = spark.read.csv("/databricks-datasets/learning-spark-v2/mnm_dataset.csv", header=True, inferSchema=True)

// COMMAND ----------

// MAGIC %python
// MAGIC df_mnm.createOrReplaceTempView('tmp_mnm')

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("SELECT * FROM tmp_mnm").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por colores y groupBy por estado y color, orderBy en orden ascendente

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("SELECT State, Color,Count(Count) FROM tmp_mnm GROUP BY State, Color ORDER BY Count(Count) DESC;").show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Recuento por estados utilizando ubn filtro Where.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### California

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("SELECT State, Color,Count(Count) FROM tmp_mnm WHERE State='CA' GROUP BY State, Color ORDER BY Count(Count) DESC;").show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Calculemos la media, mínimo, máximo y total por color y estado.

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("SELECT State, Color,Count(Count), avg(Count),min(Count), max(Count) FROM tmp_mnm GROUP BY State, Color ORDER BY Count(Count) DESC;").show(60)