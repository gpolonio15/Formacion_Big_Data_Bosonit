// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Whole-stage code generation: Spark Tungsten engine
// MAGIC 
// MAGIC Este cuaderno demuestra la potencia de la generación de código de etapa completa, una técnica que combina el estado del arte de los compiladores modernos y las bases de datos MPP. Con el fin de comparar el rendimiento con Spark 1.6, desactivamos la generación de código de etapa completa en Spark 2.0, lo que resultaría en el uso de una ruta de código similar a la de Spark 1.6.
// MAGIC 
// MAGIC Para leer las entradas de blog complementarias, haga clic en lo siguiente:
// MAGIC - https://databricks.com/blog/2016/05/11/spark-2-0-technical-preview-easier-faster-and-smarter.html
// MAGIC - https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### Primero, definamos el benchmark setup

// COMMAND ----------

// Definimos una fución simple benchmark
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Este "cluster" tiene un solo nodo, con 3 núcleos asignados.

// COMMAND ----------

// MAGIC %md
// MAGIC ### ¿A qué velocidad puede Spark 1.6 sumar 1.000 millones de números?

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Spark 1.6") {
  spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### ¿A qué velocidad puede Spark 2.0 sumar 1.000 millones de números?

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 2.0") {
  spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
}

// COMMAND ----------

// MAGIC %md
// MAGIC Pasa de tardar 33 segundos en la versión 1.6 a 2 segundos en la versión 2.0.

// COMMAND ----------

// MAGIC %md
// MAGIC ### ¿A qué velocidad puede Spark 1.6 unir 1.000 millones de registros?

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Spark 1.6") {
  spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").count()
}

// COMMAND ----------

spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ¿A qué velocidad puede Spark 2.0 unir 1.000 millones de registros?

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 2.0") {
  spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md
// MAGIC Esta es aún mejor. De 120 segundos a 1,57 segundos para unir mil millones de números.

// COMMAND ----------

// MAGIC %md
// MAGIC ### ¿Por qué hacemos estas estúpidas evaluaciones comparativas?
// MAGIC 
// MAGIC Es una pregunta excelente. Aunque las pruebas parecen sencillas, en realidad miden muchas de las primitivas fundamentales del procesamiento real de datos. Por ejemplo, la agregación es un patrón muy común, y la unión de enteros es uno de los patrones más utilizados en un almacén de datos de esquema en estrella bien definido (por ejemplo, las uniones de tablas de hechos y tablas de dimensiones).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Otras operaciones primitivas
// MAGIC 
// MAGIC También hemos evaluado la eficacia de otras operaciones primitivas con la generación de código de etapa completa. La tabla siguiente resume el resultado:
// MAGIC 
// MAGIC La forma en que realizamos la evaluación comparativa consiste en medir el coste por fila, en nanosegundos.
// MAGIC 
// MAGIC 
// MAGIC |                       | Spark 1.6 | Spark 2.0 |
// MAGIC |:---------------------:|:---------:|:---------:|
// MAGIC |         filter        |   15 ns   |   1.1 ns  |
// MAGIC |     sum w/o group     |   14 ns   |   0.9 ns  |
// MAGIC |      sum w/ group     |   79 ns   |  10.7 ns  |
// MAGIC |       hash join       |   115 ns  |   4.0 ns  |
// MAGIC |  sort (8 bit entropy) |   620 ns  |   5.3 ns  |
// MAGIC | sort (64 bit entropy) |   620 ns  |   40 ns   |
// MAGIC |    sort-merge join    |   750 ns  |   700 ns  |
// MAGIC 
// MAGIC 
// MAGIC Para leer la entrada del blog correspondiente, haga clic aquí: https://databricks.com/blog/2016/05/11/spark-2-0-technical-preview-easier-faster-and-smarter.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### Understanding the execution plan
// MAGIC 
// MAGIC Comprender el plan de ejecución
// MAGIC 
// MAGIC La función explain se ha ampliado para la generación de código de etapa completa. Cuando un operador tiene un asterisco (*) a su alrededor, la generación de código de etapa completa está activada. En el siguiente caso, Range, Filter y los dos Aggregates se ejecutan con generación de código de etapa completa. Exchange no tiene generación de código de etapa completa porque está enviando datos a través de la red.
// MAGIC 
// MAGIC Este plan de consulta tiene dos "etapas" (divididas por Exchange). En la primera, tres operadores (Range, Filter, Aggregate) se agrupan en una única función. En la segunda etapa, sólo hay un operador (Aggregate).

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un DataFrame de Spark a partir de un rango de valores de 1000 elementos, luego filtra los datos en el DataFrame para incluir solo aquellos con un valor de columna "id" mayor que 100, y finalmente calcula la suma de los valores en la columna "id" del DataFrame filtrado. 

// COMMAND ----------

spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain(true)

// COMMAND ----------

spark.range(1000).filter("id > 100").selectExpr("sum(id)").show()
