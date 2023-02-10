// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Sort Merge Join
// MAGIC 
// MAGIC Este cuaderno muestra la operación _SortMergeJoin_ antes de la agrupación y después de la agrupación

// COMMAND ----------

// MAGIC %md
// MAGIC El Sort Merge Join es un tipo de operación de join en Spark que se utiliza para unir dos o más tablas o RDDs basadas en una o más columnas comunes. La operación funciona de la siguiente manera:
// MAGIC 
// MAGIC 1. Ordenación: Ambos RDDs o tablas se ordenan por las columnas en las que se basará el join.
// MAGIC 
// MAGIC 2. Merge: A continuación, se comparan los valores de las columnas en ambos RDDs o tablas y se realiza una combinación de las filas que tienen valores coincidentes.
// MAGIC 
// MAGIC 3. Join: Finalmente, se crea una nueva tabla o RDD que contiene los datos de ambas tablas combinadas.

// COMMAND ----------

// Definimos una función simple benchmark util.
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// Esta configuración desactiva la generación de código de toda la etapa, cambiando efectivamente la ruta de ejecución para que sea similar a Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Before Spark 2.x") {
  spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md
// MAGIC En Spark 3.0 puedes tener estos modos diferentes para `explain(mode)`: 'simple', 'extended', 'codegen', 'cost', 'formatted'

// COMMAND ----------

// MAGIC %md
// MAGIC Estamos uniendo un dataframe de 0 hasta 999.999.999 y otro de 0 a 999. A continuación, se realiza un join entre ambos dataframes en la columna id utilizando la función join. Finalmente, se utiliza la función selectExpr para contar el número de filas en el resultado del join y se usa la función explain("simple") para obtener una explicación simple del plan de ejecución.

// COMMAND ----------

spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain("simple")

// COMMAND ----------

// MAGIC %md
// MAGIC El código spark.conf.set("spark.sql.codegen.wholeStage", true) configura una propiedad de Spark para habilitar el "codegen" en todas las etapas de ejecución. El "codegen" es una técnica que permite a Spark generar código en tiempo de ejecución para optimizar la ejecución de ciertas operaciones.
// MAGIC 
// MAGIC El bloque de código benchmark("Spark 3.0preview2") { spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count() } realiza una operación de benchmarking en Spark, lo que significa que mide el tiempo de ejecución de una operación específica. En este caso, la operación es un join entre dos dataframes y la cuenta de las filas en el resultado del join.

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 3.0preview2") {
  spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md Configuramos:
// MAGIC 1. El código spark.conf.set("spark.sql.codegen.wholeStage", true) significa que Spark generará código en tiempo de ejecución para optimizar la ejecución de ciertas operaciones en todas las etapas de ejecución.
// MAGIC 
// MAGIC 2. El código spark.conf.set("spark.sql.join.preferSortMergeJoin", "true") significa que Spark preferirá realizar operaciones de join utilizando el algoritmo Sort-Merge Join en lugar de otros algoritmos de join.
// MAGIC 
// MAGIC 3. El código spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") significa que Spark no realizará ninguna operación de broadcast de forma automática, lo que puede afectar el rendimiento en algunos casos.
// MAGIC 
// MAGIC 4. El código spark.conf.set("spark.sql.defaultSizeInBytes", "100000") es el tamaño en bytes que Spark utiliza como límite para decidir si una operación de broadcast es adecuada o no.
// MAGIC 
// MAGIC 5. El código spark.conf.set("spark.sql.shuffle.partitions", 16) significa que Spark utilizará 16 particiones para las operaciones de shuffle.

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.defaultSizeInBytes", "100000")
spark.conf.set("spark.sql.shuffle.partitions", 16)

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un dataframe con datos de usuario.

// COMMAND ----------

import scala.util.Random

var states = scala.collection.mutable.Map[Int, String]()
var items = scala.collection.mutable.Map[Int, String]()
val rnd = new scala.util.Random(42)
// iniciamos states y items comprados
states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")
// creamos el dataframe
val usersDF = (0 to 1000000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5)))).toDF("uid", "login", "email", "user_state")
val ordersDF = (0 to 1000000).map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
                             .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

// COMMAND ----------

display(usersDF)

// COMMAND ----------

display(ordersDF)

// COMMAND ----------

val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

// COMMAND ----------

usersOrdersDF.show(false)

// COMMAND ----------

usersOrdersDF.cache()

// COMMAND ----------

display(usersOrdersDF)

// COMMAND ----------

usersOrdersDF.explain(true)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS UsersTbl

// COMMAND ----------

// MAGIC %md
// MAGIC bucketBy(8, "uid") agrupa los datos en 8 bucket, o particiones, basadas en la columna "uid".

// COMMAND ----------

import org.apache.spark.sql.functions.asc

usersDF
  .orderBy(asc("uid"))
  .write.format("parquet")
  .bucketBy(8, "uid")
  .saveAsTable("UsersTbl")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS OrdersTbl

// COMMAND ----------

ordersDF.orderBy(asc("users_id"))
  .write.format("parquet")
  .bucketBy(8, "users_id")
  .saveAsTable("OrdersTbl")       

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM OrdersTbl LIMIT 10

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM UsersTbl LIMIT 10

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE UsersTbl

// COMMAND ----------

val usersBucketDF = spark.table("UsersTbl")

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE OrdersTbl

// COMMAND ----------

val ordersBucketDF = spark.table("OrdersTbl")

// COMMAND ----------

// MAGIC %md
// MAGIC Unimos las dos tablas creadas anteriormente.

// COMMAND ----------

val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, $"users_id" === $"uid")

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos.

// COMMAND ----------

joinUsersOrdersBucketDF.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Explicamos.

// COMMAND ----------

joinUsersOrdersBucketDF.explain(true)