// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Spark Configs

// COMMAND ----------

// MAGIC %md
// MAGIC El método getAll devuelve un mapa de todas las propiedades de configuración y sus valores en la aplicación Spark. El valor devuelto será un Map[String, String].

// COMMAND ----------

val mconf = spark.conf.getAll

// COMMAND ----------

// MAGIC %md
// MAGIC Este código es un bucle for que itera sobre las claves del mapa mconf e imprime cada par clave-valor. El valor de cada par clave-valor se recupera usando el método apply del mapa (mconf(k)), y la clave y el valor se concatenan en una cadena usando interpolación de cadena (s"${k} -> ${mconf(k)}").

// COMMAND ----------

for (k <- mconf.keySet) {println(s"${k} -> ${mconf(k)}")}

// COMMAND ----------

// MAGIC %md
// MAGIC Este código está recuperando el valor de la propiedad de configuración "spark.shuffle.service.enabled" de la configuración de Spark. Este puede ser "true" o "false", indicando si el servicio Spark shuffle está habilitado o no.

// COMMAND ----------

spark.conf.get("spark.shuffle.service.enabled")

// COMMAND ----------

// MAGIC %md 
// MAGIC Spark shuffle es un proceso de Spark que redistribuye datos entre particiones en un entorno informático paralelo y distribuido. Implica la redistribución de datos de múltiples ejecutores para formar una única partición para su procesamiento, lo que a menudo es necesario cuando se unen datos de diferentes particiones o se agrupan datos basados en una clave específica. El proceso de shuffle puede afectar significativamente al rendimiento de una aplicación Spark, y la configuración del servicio shuffle es un factor crítico para optimizar el rendimiento de Spark. 

// COMMAND ----------

// MAGIC %md
// MAGIC La propiedad de configuración "spark.sql.files.maxPartitionBytes" establece el número máximo de bytes que utilizará una partición al leer un archivo en una fuente de datos SQL de Spark. Al leer un archivo, Spark lo dividirá en trozos y procesará cada trozo en paralelo como una partición independiente. El valor de esta propiedad de configuración determina el tamaño máximo de un trozo, y los valores más grandes pueden dar lugar a menos particiones más grandes, mientras que los valores más pequeños pueden dar lugar a más particiones más pequeñas. El valor será una representación de cadena de un valor entero.

// COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

// COMMAND ----------

// MAGIC %md
// MAGIC  La propiedad defaultParallelism del objeto SparkContext devuelve el nivel de paralelismo por defecto utilizado por Spark al ejecutar operaciones sobre RDDs (Resilient Distributed Datasets). Esta propiedad determina el número de tareas que se utilizarán en paralelo para procesar los datos de un RDD. El valor de esta propiedad se determina en función del gestor de cluster y del número de núcleos disponibles en el cluster. El valor será un entero que representa el nivel de paralelismo por defecto.

// COMMAND ----------

spark.sparkContext.defaultParallelism

// COMMAND ----------

// MAGIC %md
// MAGIC Un alto nivel de paralelismo puede mejorar el rendimiento al procesar más datos en paralelo. Sin embargo, también aumenta la sobrecarga de gestionar las tareas y coordinar la ejecución. Por otro lado, un nivel bajo de paralelismo puede resultar en una infrautilización de los recursos del cluster, lo que puede llevar a un rendimiento pobre.