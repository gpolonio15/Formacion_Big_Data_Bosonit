# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Spark Data Sources Python
# MAGIC 
# MAGIC Este notebook shows muestra como usar Spark Data Sources Interface API para leer distintos formatos de apertura:
# MAGIC  * Parquet
# MAGIC  * JSON
# MAGIC  * CSV
# MAGIC  * Avro
# MAGIC  * ORC
# MAGIC  * Image
# MAGIC  * Binary
# MAGIC 
# MAGIC Una lista de disintos DataSource esta dosponible aquí [here](https://docs.databricks.com/spark/latest/data-sources/index.html#id1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definimos los paths para las disintas fuentes de datos.

# COMMAND ----------

parquet_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
json_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
csv_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
orc_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
avro_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
#Schema
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet Data Source

# COMMAND ----------

df = (spark
      .read
      .format("parquet")
      .option("path", parquet_file)
      .load())

# COMMAND ----------

# MAGIC %md
# MAGIC Otra forma de leer estos mismos datos utilizando una variación de esta API

# COMMAND ----------

df2 = spark.read.parquet(parquet_file)

# COMMAND ----------

df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usamos SQL
# MAGIC 
# MAGIC Creamos una  _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet"
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC Usamos SQL para consultar la tabla
# MAGIC 
# MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON Data Source

# COMMAND ----------

# MAGIC %md Cargamos el fuente de datos json

# COMMAND ----------

df = spark.read.format("json").option("path", json_file).load()

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos cargarlo directamente con spark.read.json

# COMMAND ----------

df2 = spark.read.json(json_file)

# COMMAND ----------

df2.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usamos SQL
# MAGIC 
# MAGIC Creamos una _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING json
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC Usamos SQL para abrir la tabla
# MAGIC 
# MAGIC Debe ser el mismo resultado que en el apartado superior.

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Data Source

# COMMAND ----------

df = (spark
      .read
	 .format("csv")
	 .option("header", "true")
	 .schema(schema)
	 .option("mode", "FAILFAST")  # Salida si existe algún error
	 .option("nullValue", "")	  # remplazar el valor nulo por "" “”
	 .option("path", csv_file)
	 .load())


# COMMAND ----------

df.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Se guarda el DataFrame (df) como un archivo Parquet en el directorio "/tmp/data/parquet/df_parquet". El método .save() escribe el DataFrame en el directorio especificado como un archivo Parquet.

# COMMAND ----------

(df.write.format("parquet")
  .mode("overwrite")
  .option("path", "/tmp/data/parquet/df_parquet")
  .option("compression", "snappy")
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC Este comando está listando los archivos en el directorio "/tmp/data/parquet/df_parquet" en el Sistema de Archivos Databricks (DBFS). El %fs es el comando mágico en el cuaderno databricks para acceder al DBFS. El comando ls se usa para listar los archivos y directorios en un directorio dado.

# COMMAND ----------

# MAGIC %fs ls /tmp/data/parquet/df_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC Volvemos a abrir el csv y leerlo como Dataframe.

# COMMAND ----------

df2 = (spark
       .read
       .option("header", "true")
       .option("mode", "FAILFAST")	 # exit if any errors
       .option("nullValue", "")
       .schema(schema)
       .csv(csv_file))

# COMMAND ----------

df2.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usamos SQL
# MAGIC 
# MAGIC Crearemos una _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING csv
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
# MAGIC       header "true",
# MAGIC       inferSchema "true",
# MAGIC       mode "FAILFAST"
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC Usamos SQL para consultar la tabla
# MAGIC 
# MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORC Data Source

# COMMAND ----------

df = (spark.read
      .format("orc")
      .option("path", orc_file)
      .load())

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usamos SQL
# MAGIC 
# MAGIC Crearemos un _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING orc
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizamos SQL para consultar la tabla
# MAGIC 
# MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avro Data Source

# COMMAND ----------

df = (spark.read
      .format("avro")
      .option("path", avro_file)
      .load())

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usamos SQL
# MAGIC 
# MAGIC Crearemos una _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING avro
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizamos SQL para consultar la tabla
# MAGIC 
# MAGIC El resultado debe ser el mismo que el leído en el DataFrame anterior

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Image

# COMMAND ----------

# MAGIC %md
# MAGIC Soporte para cargar datos de imágenes como un DataFrame. El paquete permite leer imágenes de un directorio y crear un DataFrame en el que cada fila representa una imagen y las columnas contienen información como la ruta del archivo, la altura, la anchura y los propios datos de la imagen. 

# COMMAND ----------

from pyspark.ml import image

image_dir = "/databricks-datasets/cctvVideos/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Binary

# COMMAND ----------

# MAGIC %md
# MAGIC En Spark, "binario" se refiere normalmente a datos binarios, que son una secuencia de bytes que representan datos en un formato que no es legible por humanos.

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
binary_files_df = (spark
                   .read
                   .format("binaryFile")
                   .option("pathGlobFilter", "*.jpg")
                   .load(path))
binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Para ignorar cualquier descubrimiento de datos de particionamiento en un directorio, puede establecer `recursiveFileLookup` a `true`. La ".option("pathGlobFilter", "*.jpg")" se utiliza para filtrar los archivos que se leen, en este caso, sólo se leerán los archivos con la extensión ".jpg". 

# COMMAND ----------

binary_files_df = (spark
                   .read
                   .format("binaryFile")
                   .option("pathGlobFilter", "*.jpg")
                   .option("recursiveFileLookup", "true")
                   .load(path))
binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer los AVRO, Parquet, JSON y CSV escritos en el cap3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet

# COMMAND ----------

#Parquet
fileParquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")

# COMMAND ----------

display(fileParquetDF.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Json

# COMMAND ----------

#Json
fileJsonDF = spark.read.format("json").load("/tmp/fireServiceJson/")

# COMMAND ----------

# display(fileJsonDF.limit(20))

# COMMAND ----------

# MAGIC %md En Json se ordenan las columnas alfabéticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV

# COMMAND ----------

#CSV
fileCsvDF = spark.read.format("csv").option("header", "true").load("/tmp/fireServiceCSV/")

# COMMAND ----------

display(fileCsvDF.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### AVRO

# COMMAND ----------

#AVRO
fileAvroDF = spark.read.format("AVRO").load("/tmp/fireServiceAVRO/")

# COMMAND ----------

display(fileAvroDF.limit(20))