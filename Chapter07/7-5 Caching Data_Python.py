# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Caching Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usamo _cache()_

# COMMAND ----------

# MAGIC %md
# MAGIC cache() es un método de Apache Spark que se utiliza para almacenar en caché el contenido de un Spark DataFrame o RDD en memoria. El propósito del almacenamiento en caché es mejorar el rendimiento de las aplicaciones Spark evitando el coste de volver a calcular un DataFrame o RDD cada vez que se utiliza.
# MAGIC 
# MAGIC Cuando se almacena en caché un DataFrame o RDD, Spark guarda su contenido en memoria, lo que permite a Spark acceder a los datos mucho más rápido de lo que lo haría si tuviera que volver a calcular el DataFrame o RDD a partir de sus fuentes de datos originales cada vez que se utiliza. El almacenamiento en caché es especialmente útil cuando se necesita reutilizar un DataFrame o RDD varias veces en la aplicación.
# MAGIC 
# MAGIC En Spark, el almacenamiento en caché es una operación manual que debes realizar explícitamente en tu código. Puedes almacenar en caché un DataFrame o RDD llamando al método cache() en el DataFrame o RDD.

# COMMAND ----------

# MAGIC %md
# MAGIC Crear un gran conjunto de datos con un par de columnas

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
df.cache().count()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Compruebe la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos.

# COMMAND ----------

df.unpersist() # If you do not unpersist, df2 below will not be cached because it has the same query plan as df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use _persist(StorageLevel.Level)_

# COMMAND ----------

# MAGIC %md
# MAGIC Persist(StorageLevel.Level) es un método de Apache Spark que se utiliza para almacenar en caché el contenido de un Spark DataFrame o RDD en memoria o en disco. El método permite especificar el nivel de almacenamiento a utilizar cuando se almacenan en caché los datos.
# MAGIC 
# MAGIC El método persist funciona de forma similar al método cache, pero proporciona un control más preciso sobre el nivel de almacenamiento de los datos en caché. Por ejemplo, puedes utilizar persist para almacenar datos en caché sólo en memoria, sólo en disco, o almacenar datos en caché en memoria y volcarlos al disco si es necesario.
# MAGIC 
# MAGIC El nivel de almacenamiento se especifica como parámetro del método persist.

# COMMAND ----------

from pyspark import StorageLevel

df2 = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
df2.persist(StorageLevel.DISK_ONLY).count()

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Compruebe la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos

# COMMAND ----------

df2.unpersist()

# COMMAND ----------

df.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")

# COMMAND ----------

# MAGIC %md
# MAGIC Compruebe la pestaña de almacenamiento de Spark UI para ver dónde se almacenan los datos

# COMMAND ----------

spark.sql("SELECT count(*) FROM dfTable").show()