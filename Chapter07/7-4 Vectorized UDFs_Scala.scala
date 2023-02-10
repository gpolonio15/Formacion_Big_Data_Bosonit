// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Funciones definidas por el usuario vectorizadas
// MAGIC 
// MAGIC Comparemos el rendimiento de las UDF, las UDF vectorizadas y los métodos incorporados.

// COMMAND ----------

// MAGIC %md
// MAGIC Generemos algunos dummy data, los guardaremos en cache().

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, pandas_udf
// MAGIC 
// MAGIC df = (spark
// MAGIC       .range(0, 10 * 1000 * 1000)
// MAGIC       .withColumn('id', (col('id') / 1000).cast('integer'))
// MAGIC       .withColumn('v', rand()))
// MAGIC 
// MAGIC df.cache()
// MAGIC df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Incrementamos la columna por uno.
// MAGIC 
// MAGIC Empecemos con un ejemplo sencillo de añadir uno a cada valor de nuestro DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC ### PySpark UDF

// COMMAND ----------

// MAGIC %md
// MAGIC Función para sumar a los valores de la columna v, el comando mágico "timeit" se utiliza para medir el tiempo de ejecución de la expresión que le sigue. La opción "-n1" especifica que la expresión debe ejecutarse una vez, y la opción "-r1" especifica que el resultado debe repetirse una vez.

// COMMAND ----------

// MAGIC %python
// MAGIC @udf("double")
// MAGIC def plus_one(v):
// MAGIC     return v + 1
// MAGIC 
// MAGIC %timeit -n1 -r1 df.withColumn('v', plus_one(df.v)).agg(count(col('v'))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Sintaxis alternativa (ahora también se puede utilizar en el espacio de nombres SQL)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import DoubleType
// MAGIC 
// MAGIC def plus_one(v):
// MAGIC     return v + 1
// MAGIC 
// MAGIC #
// MAGIC spark.udf.register("plus_one_udf", plus_one, DoubleType())
// MAGIC 
// MAGIC %timeit -n1 -r1 df.selectExpr("id", "plus_one_udf(v) as v").agg(count(col('v'))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Scala UDF
// MAGIC 
// MAGIC Tomo bastante tiempo añadir 1 a cada valor. Vamos a ver cuánto tiempo se tarda con un Scala UDF.

// COMMAND ----------

// MAGIC %python
// MAGIC df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val df = spark.table("df")
// MAGIC 
// MAGIC def plusOne: (Double => Double) = { v => v+1 }
// MAGIC val plus_one = udf(plusOne)

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora con Scala contamos el número de datos.

// COMMAND ----------

df.withColumn("v", plus_one($"v"))
  .agg(count(col("v")))
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobamos que no ha tardado nada.

// COMMAND ----------

// MAGIC %md
// MAGIC UDF de Scala era mucho más rápida. Sin embargo, a partir de Spark 2.3, hay UDFs vectorizados disponibles en Python para ayudar a acelerar el cálculo.
// MAGIC 
// MAGIC * [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
// MAGIC * Documentación](https://spark.apache.org/docs/latest/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow)
// MAGIC ![Benchmark](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)
// MAGIC 
// MAGIC Las UDFs vectorizadas utilizan Apache Arrow para acelerar el cálculo. Veamos cómo esto ayuda a mejorar nuestro tiempo de procesamiento.

// COMMAND ----------

// MAGIC %md
// MAGIC [Apache Arrow](https://arrow.apache.org/), es un formato de datos en columna en memoria que se utiliza en Spark para transferir datos de forma eficiente entre procesos JVM y Python. Más información [aquí](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).
// MAGIC 
// MAGIC Echemos un vistazo a cuánto tiempo se tarda en convertir un Spark DataFrame a Pandas con y sin Apache Arrow habilitado.

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "true")
// MAGIC 
// MAGIC %timeit -n1 -r1 df.toPandas()

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "false")
// MAGIC 
// MAGIC %timeit -n1 -r1 df.toPandas()

// COMMAND ----------

// MAGIC %md Con Apache Arrow habilitado tarda 5 segundos, sin estar habilitado 10 veces más en total 51 segundos.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Vectorized UDF

// COMMAND ----------

// MAGIC %python
// MAGIC @pandas_udf("double")
// MAGIC def vectorized_plus_one(v):
// MAGIC     return v + 1
// MAGIC 
// MAGIC %timeit -n1 -r1 df.withColumn("v", vectorized_plus_one(df.v)).agg(count(col("v"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Todavía no es tan bueno como el UDF de Scala, pero al menos es mejor que el UDF normal de Python.
// MAGIC 
// MAGIC Aquí hay alguna sintaxis alternativa para el UDF de Pandas.

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import pandas_udf
// MAGIC 
// MAGIC def vectorized_plus_one(v):
// MAGIC     return v + 1
// MAGIC 
// MAGIC vectorized_plus_one_udf = pandas_udf(vectorized_plus_one, "double")
// MAGIC 
// MAGIC %timeit -n1 -r1 df.withColumn("v", vectorized_plus_one_udf(df.v)).agg(count(col("v"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Built-in Method
// MAGIC 
// MAGIC Comparemos el rendimiento de las UDFs con el uso del método incorporado.

// COMMAND ----------

// MAGIC %md
// MAGIC Un método incorporado es una función predefinida que está disponible en un lenguaje de programación o en una biblioteca de software. Estos métodos ya han sido implementados y están listos para ser utilizados por los programadores sin tener que escribir el código desde cero. Los métodos incorporados son parte del lenguaje o biblioteca y ofrecen una funcionalidad común y útil que se puede reutilizar en diferentes proyectos de programación. En este caso lit, se utiliza para crear una nueva columna con un valor literal constante. La función lit toma un único argumento que es el valor a utilizar como constante en la nueva columna. Esto puede ser útil en situaciones en las que se desea añadir un valor constante a un DataFrame, por ejemplo para añadir un valor por defecto para una columna que falta, o para añadir una nueva columna con un valor estático para cada fila.

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import lit
// MAGIC 
// MAGIC %timeit -n1 -r1 df.withColumn("v", df.v + lit(1)).agg(count(col("v"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Computing subtract mean
// MAGIC 
// MAGIC Arriba, estábamos trabajando con tipos de retorno Scalar. Ahora podemos utilizar UDFs agrupados.

// COMMAND ----------

// MAGIC %md
// MAGIC ### PySpark UDF

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import Row
// MAGIC import pandas as pd
// MAGIC 
// MAGIC @udf(ArrayType(df.schema))
// MAGIC def subtract_mean(rows):
// MAGIC   vs = pd.Series([r.v for r in rows])
// MAGIC   vs = vs - vs.mean()
// MAGIC   return [Row(id=rows[i]["id"], v=float(vs[i])) for i in range(len(rows))]
// MAGIC   
// MAGIC %timeit -n1 -r1 (df.groupby("id").agg(collect_list(struct(df["id"], df["v"])).alias("rows")).withColumn("new_rows", subtract_mean(col("rows"))).withColumn("new_row", explode(col("new_rows"))).withColumn("id", col("new_row.id")).withColumn("v", col("new_row.v")).agg(count(col("v"))).show())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Vectorized UDF

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC def vectorized_subtract_mean(pdf: pd.Series) -> pd.Series:
// MAGIC 	return pdf.assign(v=pdf.v - pdf.v.mean())
// MAGIC 
// MAGIC %timeit -n1 -r1 df.groupby("id").applyInPandas(vectorized_subtract_mean, df.schema).agg(count(col("v"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Tarda menos el Vectorized UDF.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Diferencia entre Vectorized UDF y PySpark UDF

// COMMAND ----------

// MAGIC %md
// MAGIC Las diferencias entre Vectorized UDF y PySpark UDF son las siguientes:
// MAGIC 
// MAGIC Vectorized UDF: Son funciones de usuario que están optimizadas para trabajar con grandes cantidades de datos en una sola iteración, aprovechando la capacidad de procesamiento en paralelo de Spark. Estas funciones son escritas en lenguaje de bajo nivel, como Scala, y están integradas en Spark, lo que las hace más rápidas que las UDF escritas en Python.
// MAGIC 
// MAGIC PySpark UDF: Son funciones de usuario escritas en Python y ejecutadas en Spark. Aunque son más fáciles de escribir y mantener que las Vectorized UDF, son más lentas debido a la overhead de la interpretación de Python y la serialización/deserialización de los datos.
// MAGIC 
// MAGIC En resumen, se recomienda utilizar Vectorized UDF cuando la velocidad es un factor crítico, mientras que PySpark UDF es una buena opción cuando la facilidad de uso y la flexibilidad son más importantes.