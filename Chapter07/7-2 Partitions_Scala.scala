// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Partitions
// MAGIC Este cuaderno muestra cómo comprobar y cambiar el número de particiones.

// COMMAND ----------

// MAGIC %md
// MAGIC Este código está creando un DataFrame que contiene un rango de mil millones de elementos y reparticionándolo en 16 particiones. Se realizan los siguientes pasos:
// MAGIC 
// MAGIC 1. Se llama al método spark.range para crear un DataFrame que contenga un rango de 1000 millones de elementos.
// MAGIC 
// MAGIC 2. Se llama al método repartition sobre el DataFrame para repartirlo en 16 particiones.
// MAGIC 
// MAGIC 3. Se accede a la propiedad rdd del DataFrame para recuperar el RDD subyacente (Resilient Distributed Dataset), y se llama al método getNumPartitions para recuperar el número de particiones en el RDD.
// MAGIC 
// MAGIC El resultado de este código será el número de particiones en el RDD, que es 16. El número de particiones determina el grado de paralelismo al procesar los datos en el RDD, y cuanto mayor sea el número de particiones, mayor será el grado de paralelismo. Sin embargo, tener demasiadas particiones también puede resultar en un aumento de la sobrecarga, por lo que es importante encontrar el equilibrio adecuado entre el paralelismo y la sobrecarga.

// COMMAND ----------

val numDF = spark.range(1000L * 1000 * 1000).repartition(16)
numDF.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md Tarda bastante en crearse.

// COMMAND ----------

// MAGIC %md
// MAGIC Este código está creando un RDD (Resilient Distributed Dataset) que contiene los números de 1 a 100 millones y lo divide en 16 particiones. Se realizan los siguientes pasos:
// MAGIC 
// MAGIC 1. Se llama al método sparkContext.parallelize para crear un RDD a partir de una colección local de enteros (de 1 a 100 millones).
// MAGIC 
// MAGIC 2. El segundo argumento de 16 es el número de particiones a utilizar al paralelizar el RDD.
// MAGIC 
// MAGIC 3. El método getNumPartitions es llamado en el RDD para recuperar el número de particiones en el RDD.
// MAGIC 
// MAGIC El resultado de este código será el número de particiones en el RDD, que es 16. El número de particiones determina el grado de paralelismo al procesar los datos en el RDD, y cuanto mayor sea el número de particiones, mayor será el grado de paralelismo. Sin embargo, tener demasiadas particiones también puede resultar en un aumento de la sobrecarga, por lo que es importante encontrar el equilibrio adecuado entre el paralelismo y la sobrecarga.

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(1 to 100000000, 16)
rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC Al utilizar RDD, tarda mucho menos.

// COMMAND ----------

// MAGIC %md
// MAGIC Para obtener el nivel de paralelismo:

// COMMAND ----------

spark.sparkContext.defaultParallelism

// COMMAND ----------

// MAGIC %md
// MAGIC Este código recupera el número de particiones. Se realizan los siguientes pasos:
// MAGIC 
// MAGIC 1. Se llama al método spark.conf.set para establecer el valor de la propiedad de configuración spark.sql.shuffle.partitions al nivel de paralelismo por defecto de la aplicación Spark. El nivel de paralelismo por defecto se obtiene llamando a spark.sparkContext.defaultParallelism.
// MAGIC 
// MAGIC 2. Se llama al método spark.conf.get para recuperar el valor de la propiedad de configuración spark.sql.shuffle.partitions.
// MAGIC 
// MAGIC El resultado de este código será el valor de la propiedad de configuración spark.sql.shuffle.partitions, que es el nivel de paralelismo por defecto de la aplicación Spark. Esta propiedad de configuración determina el número de particiones utilizadas para barajar datos en las operaciones SQL de Spark. Al establecer esta propiedad en el nivel de paralelismo predeterminado, te aseguras de que los datos se barajarán utilizando un nivel de paralelismo adecuado para los recursos disponibles en tu clúster.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

// MAGIC %md
// MAGIC El nivel de paralelismo por defecto de la aplicación Spark es de 8.