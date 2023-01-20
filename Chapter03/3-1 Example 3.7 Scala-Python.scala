// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Example 3.7 Scala-Python

// COMMAND ----------

// MAGIC %md
// MAGIC # Scala

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.spark.sql.functions.{col, expr, when, concat, lit}
// MAGIC 
// MAGIC val jsonFile = "/databricks-datasets/learning-spark-v2/blogs.json"
// MAGIC 
// MAGIC val schema = StructType(Array(StructField("Id", IntegerType, false),
// MAGIC   StructField("First", StringType, false),
// MAGIC   StructField("Last", StringType, false),
// MAGIC   StructField("Url", StringType, false),
// MAGIC   StructField("Published", StringType, false),
// MAGIC   StructField("Hits", IntegerType, false),
// MAGIC   StructField("Campaigns", ArrayType(StringType), false)))
// MAGIC 
// MAGIC val blogsDF = spark.read.schema(schema).json(jsonFile)
// MAGIC 
// MAGIC blogsDF.show(false)
// MAGIC // print the schemas
// MAGIC print(blogsDF.printSchema)
// MAGIC print(blogsDF.schema)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos una vista temporal en un DataFrame

// COMMAND ----------

// MAGIC %scala
// MAGIC blogsDF.createOrReplaceTempView("blogs")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Definición del esquema del DataFrame en una cadena de texto en formato SQL

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.table("blogs").schema.toDDL

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos una columna de Hits por 2

// COMMAND ----------

// MAGIC %scala
// MAGIC blogsDF.select(expr("Hits") * 2).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Seleccionamos la columna hits + id

// COMMAND ----------

// MAGIC %scala
// MAGIC blogsDF.select(expr("Hits") + expr("Id")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos una colunna que nos indica si hits es menor que 10000

// COMMAND ----------

// MAGIC %scala
// MAGIC blogsDF.withColumn("Big Hitters", (expr("Hits") > 10000)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos una colunna que concatena nombre, apellido e Id, y es llamado autorID.

// COMMAND ----------

// MAGIC %scala
// MAGIC blogsDF.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(expr("AuthorsId")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Python

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import col, expr, when, concat, lit

// COMMAND ----------

// MAGIC %md
// MAGIC #### Debemos definir el Schema.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # definimos el schema
// MAGIC """"
// MAGIC schema = (StructType([
// MAGIC    StructField("Id", IntegerType(), False),
// MAGIC    StructField("First", StringType(), False),
// MAGIC    StructField("Last", StringType(), False),
// MAGIC    StructField("Url", StringType(), False),
// MAGIC    StructField("Published", StringType(), False),
// MAGIC    StructField("Hits", IntegerType(), False),
// MAGIC    StructField("Campaigns", ArrayType(StringType()), False)]))
// MAGIC    """
// MAGIC 
// MAGIC #Definimos el ddl
// MAGIC ddl_schema = "`Id` INT,`First` STRING,`Last` STRING,`Url` STRING,`Published` STRING,`Hits` INT,`Campaigns` ARRAY<STRING>"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos los datos.

// COMMAND ----------

// MAGIC %python
// MAGIC # Creamos los daros
// MAGIC data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
// MAGIC        [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
// MAGIC        [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
// MAGIC        [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
// MAGIC        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
// MAGIC        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
// MAGIC       ]

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos el dataframe.

// COMMAND ----------

// MAGIC %python
// MAGIC # Utilizamos el schema para crear los datos.
// MAGIC blogs_df = spark.createDataFrame(data, ddl_schema)
// MAGIC # Mostramos el dataframe
// MAGIC blogs_df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creamos una vista temporal en un DataFrame

// COMMAND ----------

// MAGIC %python
// MAGIC blogs_df.createOrReplaceTempView("blogs")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Hacemos los mismos ejercicios que en scala

// COMMAND ----------

// MAGIC %python
// MAGIC blogs_df.select(expr("Hits") * 2).show(n=6)

// COMMAND ----------

// MAGIC %python
// MAGIC blogs_df.withColumn("Big Hitters", (expr("Hits") > 10000)).show()

// COMMAND ----------

// MAGIC %python
// MAGIC blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(expr("AuthorsId")).show(n=6)