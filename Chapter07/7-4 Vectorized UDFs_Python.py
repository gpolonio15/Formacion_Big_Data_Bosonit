# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Vectorized User Defined Functions
# MAGIC 
# MAGIC Comparemos el rendimiento de los UDF, los UDF vectorizados y los métodos incorporados.

# COMMAND ----------

# MAGIC %md
# MAGIC Generamos algunos dummy data.

# COMMAND ----------

# MAGIC %md
# MAGIC El código crea un Spark DataFrame utilizando el método range de SparkSession con un rango de 0 a 10 millones. La columna "id" se crea dividiendo la columna "id" por 1000 y convirtiéndola en un número entero. La columna "v" se crea utilizando un método generador de números aleatorios. El método caché se utiliza para almacenar los datos en memoria. Por último, el método count se utiliza para contar el número de filas del DataFrame.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, pandas_udf

df = (spark
      .range(0, 10 * 1000 * 1000)
      .withColumn("id", (col("id") / 1000).cast("integer"))
      .withColumn("v", rand()))

df.cache()
df.count()

# COMMAND ----------

df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incrementamos la columna por uno.
# MAGIC 
# MAGIC Empecemos con un ejemplo sencillo de añadir uno a cada valor de nuestro DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark UDF

# COMMAND ----------

# MAGIC %md
# MAGIC Función para sumar 1 los valores de la columna v.

# COMMAND ----------

@udf("double")
def plus_one(v):
    return v + 1

%timeit -n1 -r1 df.withColumn("v", plus_one(df.v)).agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sintaxis alternativa (ahora también se puede utilizar en el espacio de nombres SQL)

# COMMAND ----------

from pyspark.sql.types import DoubleType

def plus_one(v):
    return v + 1
  
spark.udf.register("plus_one_udf", plus_one, DoubleType())

%timeit -n1 -r1 df.selectExpr("id", "plus_one_udf(v) as v").agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scala UDF
# MAGIC 
# MAGIC  Vmos a ver cuánto tiempo se tarda con un Scala UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista temporal.

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %md 
# MAGIC Generamos la función para sumarle 1 a los datos de la columna v.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val df = spark.table("df")
# MAGIC 
# MAGIC def plusOne: (Double => Double) = { v => v+1 }
# MAGIC val plus_one = udf(plusOne)

# COMMAND ----------

# MAGIC %md
# MAGIC La aplicamos.

# COMMAND ----------

# MAGIC %scala
# MAGIC df.withColumn("v", plus_one($"v"))
# MAGIC   .agg(count(col("v")))
# MAGIC   .show()

# COMMAND ----------

df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC UDF de Scala es mucho más rápida. Sin embargo, a partir de Spark 2.3, hay UDFs vectorizados disponibles en Python para ayudar a acelerar el cálculo
# MAGIC 
# MAGIC * [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow)
# MAGIC 
# MAGIC ![Benchmark](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)
# MAGIC 
# MAGIC Las UDFs vectorizadas utilizan Apache Arrow para acelerar el cálculo. Veamos cómo esto ayuda a mejorar nuestro tiempo de procesamiento.

# COMMAND ----------

# MAGIC %md
# MAGIC [Apache Arrow](https://arrow.apache.org/), es un formato de datos en columna en memoria que se utiliza en Spark para transferir datos de forma eficiente entre procesos JVM y Python. Más información [aquí](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).
# MAGIC 
# MAGIC Echemos un vistazo a cuánto tiempo se tarda en convertir un Spark DataFrame a Pandas con y sin Apache Arrow habilitado.

# COMMAND ----------

# MAGIC %md
# MAGIC Apache activado.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

%timeit -n1 -r1 df.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC Apache desactivado.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

%timeit -n1 -r1 df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vectorized UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Las UDF vectorizadas se implementan utilizando Apache Arrow y aprovechan el formato de datos en columnas para realizar operaciones sobre múltiples valores a la vez, lo que resulta más rápido que procesar filas individuales.
# MAGIC 
# MAGIC En Spark, las UDF vectorizadas pueden crearse utilizando el método pyspark.sql.functions.udf, y la implementación de la UDF debe estar escrita en Python y utilizar la API de Apache Arrow. La salida de la UDF también debe tener la forma de una columna Arrow.

# COMMAND ----------

# MAGIC %md
# MAGIC Función con implementación de la UDF.

# COMMAND ----------

@pandas_udf("double")
def vectorized_plus_one(v):
    return v + 1

%timeit -n1 -r1 df.withColumn("v", vectorized_plus_one(df.v)).agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC No es tan bueno como el UDF de Scala, pero al menos es mejor que el UDF normal de Python.
# MAGIC 
# MAGIC Aquí hay alguna sintaxis alternativa para el UDF de Pandas.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf

def vectorized_plus_one(v):
    return v + 1

vectorized_plus_one_udf = pandas_udf(vectorized_plus_one, "double")

%timeit -n1 -r1 df.withColumn("v", vectorized_plus_one_udf(df.v)).agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built-in Method
# MAGIC 
# MAGIC Comparemos el rendimiento de las UDFs con el uso del método incorporado.

# COMMAND ----------

# MAGIC %md
# MAGIC Un método incorporado es una función predefinida y lista para usar que se incluye en un lenguaje o biblioteca de programación. Estos métodos están diseñados para realizar operaciones comunes y de uso frecuente, como la manipulación de cadenas, cálculos matemáticos o procesamiento de datos, y son proporcionados por el lenguaje o la biblioteca para mayor comodidad y facilidad de uso.
# MAGIC 
# MAGIC En Apache Spark, los métodos integrados, también conocidos como funciones integradas, son un conjunto de funciones que se pueden utilizar para realizar diversas operaciones de procesamiento de datos en Spark DataFrames, como agregaciones, filtrados o transformaciones. Spark proporciona un amplio conjunto de funciones integradas que se pueden utilizar directamente en las consultas SQL de Spark o en la API de PySpark.
# MAGIC 
# MAGIC Algunos ejemplos de métodos integrados en Spark son count, sum, avg, min, max, collect_list, explode, lower, upper, trim, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC  La función lit se utiliza para crear un valor literal que puede utilizarse en una expresión SQL de Spark. En este caso, lit(1) crea un valor literal de 1.

# COMMAND ----------

from pyspark.sql.functions import lit

%timeit -n1 -r1 df.withColumn("v", df.v + lit(1)).agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Computing subtract mean
# MAGIC 
# MAGIC Trabajemos con UDFs agrupados.

# COMMAND ----------

# MAGIC %md
# MAGIC Una UDF agrupada, también conocida como UDF agregada, es una función definida por el usuario en Apache Spark que opera sobre datos agrupados. Permite realizar agregaciones personalizadas sobre datos agrupados en un DataFrame de Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark UDF

# COMMAND ----------

from pyspark.sql import Row
import pandas as pd

@udf(ArrayType(df.schema))
def subtract_mean(rows):
  vs = pd.Series([r.v for r in rows])
  vs = vs - vs.mean()
  return [Row(id=rows[i]["id"], v=float(vs[i])) for i in range(len(rows))]
  
%timeit -n1 -r1 (df.groupby("id").agg(collect_list(struct(df["id"], df["v"])).alias("rows")).withColumn("new_rows", subtract_mean(col("rows"))).withColumn("new_row", explode(col("new_rows"))).withColumn("id", col("new_row.id")).withColumn("v", col("new_row.v")).agg(count(col("v"))).show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vectorized UDF

# COMMAND ----------

def vectorized_subtract_mean(pdf: pd.Series) -> pd.Series:
	return pdf.assign(v=pdf.v - pdf.v.mean())

%timeit -n1 -r1 df.groupby("id").applyInPandas(vectorized_subtract_mean, df.schema).agg(count(col("v"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Vuelve a tardar menos Vectorized UDF, que el PySpark UDF.