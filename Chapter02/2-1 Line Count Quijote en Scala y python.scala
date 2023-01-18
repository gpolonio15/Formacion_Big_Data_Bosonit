// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC ## Example 2-1 Line Count-Quijote

// COMMAND ----------

// MAGIC %python
// MAGIC spark.version

// COMMAND ----------

// MAGIC %python
// MAGIC strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
// MAGIC strings.show(14, truncate=False)

// COMMAND ----------

// MAGIC %python
// MAGIC strings.count()

// COMMAND ----------

// MAGIC %python
// MAGIC filtered = strings.filter(strings.value.contains("Spark"))
// MAGIC filtered.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## QUIJOTE

// COMMAND ----------

// MAGIC %md
// MAGIC ### Descargamos el Quijote.

// COMMAND ----------

// MAGIC %python
// MAGIC quijote = spark.read.text("/FileStore/tables/el_quijote.md")
// MAGIC quijote.show(5, truncate=False)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Contamos el número de lineas

// COMMAND ----------

// MAGIC %python
// MAGIC quijote.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 2186 líneas

// COMMAND ----------

// MAGIC %md 
// MAGIC Truncate en True

// COMMAND ----------

// MAGIC %python
// MAGIC quijote.show(5, truncate=True)

// COMMAND ----------

// MAGIC %md
// MAGIC Se observa que cada línea tiene un tamaño máximo.

// COMMAND ----------

// MAGIC %md
// MAGIC Método head.

// COMMAND ----------

// MAGIC %python
// MAGIC quijote.head(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Método take.

// COMMAND ----------

// MAGIC %python
// MAGIC quijote.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Método first.

// COMMAND ----------

// MAGIC %python
// MAGIC quijote.first()

// COMMAND ----------

// MAGIC %md
// MAGIC First y head no necesitan argumentos, mientras take si.
// MAGIC 
// MAGIC First solo da la primera linea y no se le puede introducir ningún argumento. 
// MAGIC 
// MAGIC Head si acepta argumentos y si le introduces el número de líneas que quieres, te da una lista de Row, al igual que take.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scala

// COMMAND ----------

// MAGIC %scala
// MAGIC val quijote_scala = spark.read.text("/FileStore/tables/el_quijote.md")
// MAGIC quijote_scala.show(5, false)

// COMMAND ----------

quijote_scala.count

// COMMAND ----------

// MAGIC %md
// MAGIC 2186 Lineas, similares a las contadas utilizando python.

// COMMAND ----------

// MAGIC %scala
// MAGIC quijote_scala.head(5)

// COMMAND ----------

// MAGIC %scala
// MAGIC quijote_scala.take(5)

// COMMAND ----------

// MAGIC %scala
// MAGIC quijote_scala.first()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Comprobamos que en scala se crean Array, cuando utilizamos take, first o head.