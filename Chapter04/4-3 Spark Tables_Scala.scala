// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Spark Tables Scala
// MAGIC 
// MAGIC Este cuaderno muestra cómo utilizar la API de la interfaz de catálogo de Spark para consultar bases de datos, tablas y columnas.
// MAGIC 
// MAGIC Hay disponible una lista completa de métodos documentados [here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Catalog)

// COMMAND ----------

// MAGIC %md
// MAGIC Cargamos el csv, con los retrasos en salida de vuelos.

// COMMAND ----------

val us_flights_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Creamos Managed Tables

// COMMAND ----------

// Crear databases y managed tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mostramos las databases

// COMMAND ----------

display(spark.catalog.listDatabases())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Leemos nuestra US Flights table

// COMMAND ----------

val df = spark
  .read
  .format("csv")
  .schema("`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING")
  .option("header", "true")
  .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Guardamos en nuestra tabla creada us_delay_flights_tbl

// COMMAND ----------

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cache a la Tabla

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE us_delay_flights_tbl

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobamos que la tabla esta cached

// COMMAND ----------

spark.catalog.isCached("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Visualizar tablas dentro de un Database
// MAGIC 
// MAGIC Tenga en cuenta que la tabla es MANGED por Spark

// COMMAND ----------

// MAGIC %md
// MAGIC En un entorno MANAGED Spark se encarga de todos los recursos necesarios para los cálculos y el usuario puede centrarse en la lógica de procesamiento de datos. En un entorno no gestionado, el usuario es responsable de gestionar los recursos y las configuraciones del clúster.

// COMMAND ----------

display(spark.catalog.listTables(dbName="learn_spark_db"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mostrar las columnas de la tabla.

// COMMAND ----------

display(spark.catalog.listColumns("us_delay_flights_tbl"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Creamos una Unmanaged Tables

// COMMAND ----------

// Eliminar database y crear unmanaged tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl (date INT, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (path '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')")

// COMMAND ----------

display(spark.catalog.listDatabases())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mostrar tablas
// MAGIC 
// MAGIC **Nota**: El tipo de tabla aquí es tableType='EXTERNAL', lo que indica que no esta gestionado por Spark, como por ejemplo con el tipo de tabla tableType='MANAGED'

// COMMAND ----------

display(spark.catalog.listTables(dbName="learn_spark_db"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mostrar columnas de la tabla

// COMMAND ----------

display(spark.catalog.listColumns("us_delay_flights_tbl"))


// COMMAND ----------

// MAGIC %md
// MAGIC ## Diferencias a la hora de crear una managed table y una unmanaged table

// COMMAND ----------

// MAGIC %md
// MAGIC En Apache Spark, la diferencia principal entre crear una tabla managed y otra unmanaged es cómo se administran y almacenan los metadatos de la tabla.
// MAGIC 
// MAGIC   1. Una tabla managed es administrada por Spark, lo que significa que Spark se encarga de almacenar y administrar los metadatos de la tabla, como el esquema y las estadísticas. Esto permite a Spark optimizar automáticamente las consultas a la tabla.
// MAGIC 
// MAGIC   2. Una tabla unmanaged, en cambio, no es administrada por Spark, lo que significa que el usuario es responsable de almacenar y administrar los metadatos de la tabla. Esto significa que el usuario debe proporcionar información sobre el esquema y las estadísticas de la tabla para que Spark pueda optimizar las consultas.
// MAGIC 
// MAGIC 
// MAGIC Otra diferencia es en el almacenamiento de los datos, una tabla managed es almacenada en el sistema de almacenamiento de Spark (por ejemplo, en memoria o en disco), mientras que una tabla unmanaged se almacena en una ubicación externa, como un sistema de archivos o un almacenamiento en la nube.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Diferencias al crearlas.

// COMMAND ----------

// MAGIC %md
// MAGIC A la hora de crear el schema se carga directamente el file de los datos csv: USING csv OPTIONS (path '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv').