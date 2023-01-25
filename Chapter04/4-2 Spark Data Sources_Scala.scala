// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Spark Data Sources
// MAGIC 
// MAGIC Este cuaderno nos muestra como usar Spark Data Sources Interface API para leer distintos formtos de archivo:
// MAGIC  * Parquet
// MAGIC  * JSON
// MAGIC  * CSV
// MAGIC  * Avro
// MAGIC  * ORC
// MAGIC  * Image
// MAGIC  * Binary
// MAGIC 
// MAGIC La lista completa de métodos DataSource está disponible [aquí].(https://docs.databricks.com/spark/latest/data-sources/index.html#id1)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Definir rutas para las distintas fuentes de datos

// COMMAND ----------

val parquetFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
val jsonFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val csvFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val orcFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
val avroFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
//Definimos el schema
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fuente de datos Parquet

// COMMAND ----------

val df = spark
  .read
  .format("parquet")
  .option("path", parquetFile)
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC Otra forma de leer estos mismos datos utilizando una variación de esta API

// COMMAND ----------

val df2 = spark.read.parquet(parquetFile)

// COMMAND ----------

df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Usando SQL
// MAGIC 
// MAGIC Creamos un _unmanaged_ temporary view

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC     USING parquet
// MAGIC     OPTIONS (
// MAGIC       path "/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet"
// MAGIC     )

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos SQL para consultar la tabla
// MAGIC 
// MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fuente de datos JSON 

// COMMAND ----------

val df = spark
  .read
  .format("json")
  .option("path", jsonFile)
  .load()

// COMMAND ----------

df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Usamos SQL
// MAGIC 
// MAGIC Crearemos una _unmanaged_ temporary view

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC     USING json
// MAGIC     OPTIONS (
// MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC     )

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos SQL para consultar la tabla
// MAGIC 
// MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fuente de datos CSV

// COMMAND ----------

val df  = spark
  .read
  .format("csv")
  .option("header", "true")
  .schema(schema)              //Cargar el schema, generado anteriormente
  .option("mode", "FAILFAST")  // Salir si hay algún error
  .option("nullValue", "")	  // Remplazar el valor nulo por ""
  .option("path", csvFile)
  .load()

// COMMAND ----------

df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Se guarda el DataFrame (df) como un archivo Parquet en el directorio "/tmp/data/parquet/df_parquet". El método .save() escribe el DataFrame en el directorio especificado como un archivo Parquet.

// COMMAND ----------

(df.write.format("parquet")
  .mode("overwrite")
  .option("path", "/tmp/data/parquet/df_parquet")
  .option("compression", "snappy")
  .save())

// COMMAND ----------

// MAGIC %md
// MAGIC Este comando está listando los archivos en el directorio "/tmp/data/parquet/df_parquet" en el Sistema de Archivos Databricks (DBFS). El %fs es el comando mágico en el cuaderno databricks para acceder al DBFS. El comando ls se usa para listar los archivos y directorios en un directorio dado.

// COMMAND ----------

// MAGIC %fs ls /tmp/data/parquet/df_parquet

// COMMAND ----------

// MAGIC %md
// MAGIC Volvemos a abrir el csv y leerlo como Dataframe.

// COMMAND ----------

val df2 = spark
  .read
  .option("header", "true")
  .option("mode", "FAILFAST") // Salir si hay errores
  .option("nullValue", "")    // Sustituir el valor nulo por ""
  .schema(schema)
  .csv(csvFile)

// COMMAND ----------

df2.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Usamos SQL
// MAGIC 
// MAGIC Crearemos una _unmanaged_ temporary view

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC     USING csv
// MAGIC     OPTIONS (
// MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
// MAGIC       header "true",
// MAGIC       inferSchema "true",
// MAGIC       mode "FAILFAST"
// MAGIC     )

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos SQL para consultar la tabla
// MAGIC 
// MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fuente de datos ORC 

// COMMAND ----------

val df = spark.read
  .format("orc")
  .option("path", orcFile)
  .load()

// COMMAND ----------

df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Usamos SQL
// MAGIC 
// MAGIC Crearemos un _unmanaged_ temporary view

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC     USING orc
// MAGIC     OPTIONS (
// MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
// MAGIC     )

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos SQL para consultar la tabla
// MAGIC 
// MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fuente de datos Avro 

// COMMAND ----------

val df = spark.read
  .format("avro")
  .option("path", avroFile)
  .load()

// COMMAND ----------

df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Usamos SQL
// MAGIC 
// MAGIC Crearemos una _unmanaged_ temporary view

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC     USING avro
// MAGIC     OPTIONS (
// MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
// MAGIC     )

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos SQL para consultar la tabla
// MAGIC 
// MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Image

// COMMAND ----------

// MAGIC %md
// MAGIC Soporte para cargar datos de imágenes como un DataFrame. El paquete permite leer imágenes de un directorio y crear un DataFrame en el que cada fila representa una imagen y las columnas contienen información como la ruta del archivo, la altura, la anchura y los propios datos de la imagen. 

// COMMAND ----------

import org.apache.spark.ml.source.image

val imageDir = "/databricks-datasets/cctvVideos/train_images/"
val imagesDF = spark.read.format("image").load(imageDir)

imagesDF.printSchema
imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(7, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Binary

// COMMAND ----------

// MAGIC %md
// MAGIC En Spark, "binario" se refiere normalmente a datos binarios, que son una secuencia de bytes que representan datos en un formato que no es legible por humanos.

// COMMAND ----------

val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val binaryFilesDF = spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .load(path)

binaryFilesDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Para ignorar cualquier descubrimiento de datos de particionamiento en un directorio, puede establecer `recursiveFileLookup` a `true`. La ".option("pathGlobFilter", "*.jpg")" se utiliza para filtrar los archivos que se leen, en este caso, sólo se leerán los archivos con la extensión ".jpg". 

// COMMAND ----------

val binaryFilesDF = spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .option("recursiveFileLookup", "true")
  .load(path)
binaryFilesDF.show(5)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Leer los AVRO, Parquet, JSON y CSV escritos en el cap3

// COMMAND ----------

// MAGIC %md
// MAGIC #### Parquet

// COMMAND ----------

// MAGIC %scala
// MAGIC //Parquet
// MAGIC val fileParquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileParquetDF.limit(20))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Json

// COMMAND ----------

// MAGIC %scala
// MAGIC //Json
// MAGIC val fileJsonDF = spark.read.format("json").load("/tmp/fireServiceJson/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileJsonDF.limit(20))

// COMMAND ----------

// MAGIC %md
// MAGIC El formato Json ordena las columnas por orden alfabético.

// COMMAND ----------

// MAGIC %md
// MAGIC #### CSV

// COMMAND ----------

// MAGIC %scala
// MAGIC //CSV
// MAGIC val fileCsvDF = spark.read.format("csv").option("header", "true").load("/tmp/fireServiceCSV/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileCsvDF.limit(20))

// COMMAND ----------

// MAGIC %md
// MAGIC #### AVRO

// COMMAND ----------

// MAGIC %scala
// MAGIC //AVRO
// MAGIC val fileAvroDF = spark.read.format("AVRO").load("/tmp/fireServiceAVRO/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileAvroDF.limit(20))