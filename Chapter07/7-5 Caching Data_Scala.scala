// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Caching Data

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use _cache()_

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un gran conjunto de datos con un par de columnas

// COMMAND ----------

val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
df.cache().count()

// COMMAND ----------

df.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Contamos el número de valores.

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Compruebe la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos.

// COMMAND ----------

df.unpersist() // Si no unpersist, df2 a continuación no se almacenará en caché porque tiene el mismo plan de consulta que df

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use _persist(StorageLevel.Level)_

// COMMAND ----------

// MAGIC %md
// MAGIC El método persist en Spark permite persistir los datos en memoria o en disco para reducir la cantidad de tiempo que se tarda en recompute los mismos datos en futuras operaciones.
// MAGIC 
// MAGIC El método persist toma un argumento StorageLevel, que indica el nivel de almacenamiento que se desea para los datos persistidos. StorageLevel.Level es una constante que representa el nivel de almacenamiento que se desea para los datos.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

val df2 = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
df2.persist(StorageLevel.DISK_ONLY).count()

// COMMAND ----------

df2.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Compruebamos la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos

// COMMAND ----------

df2.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una tabla temporal y la almacenamos en memoria ('CACHE TABLE')

// COMMAND ----------

df.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")

// COMMAND ----------

// MAGIC %md
// MAGIC Compruebamos la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos

// COMMAND ----------

df2.unpersist()

// COMMAND ----------

spark.sql("SELECT count(*) FROM dfTable").show()