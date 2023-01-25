# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Example 4.1_Python
# MAGIC 
# MAGIC Este cuaderno muestra el Ejemplo 4.1 del libro que muestra cómo utilizar SQL en un conjunto de datos US Flights Dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC Librerias necesarias de SQL

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Defina una UDF para convertir el formato de fecha en un formato legible.
# MAGIC 
# MAGIC *Nota: la fecha es una cadena a la que le falta el año, por lo que puede resultar difícil realizar consultas utilizando la función SQL `year()`.

# COMMAND ----------

def to_date_format_udf(d_str):
  l = [char for char in d_str]
  return "".join(l[0:2]) + "/" +  "".join(l[2:4]) + " " +"".join(l[4:6]) + ":" + "".join(l[6:])

to_date_format_udf("02190925")

# COMMAND ----------

# MAGIC %md
# MAGIC Registramos el UDF

# COMMAND ----------

spark.udf.register("to_date_format_udf", to_date_format_udf, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos nuestros datos de US departure flight data

# COMMAND ----------

df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
      .load())

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Testeamos nuestra UDF creada.

# COMMAND ----------

df.selectExpr("to_date_format_udf(date) as data_format").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Crear una vista temporal en la que podamos realizar consultas SQL

# COMMAND ----------

df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC Cache Table para facilitar las consultas

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE us_delay_flights_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC Convertir todo `date` a `date_fm` para que sea más elegible.
# MAGIC 
# MAGIC Nota: estamos utilizando UDF para convertirlo sobre la marcha.  

# COMMAND ----------

# MAGIC %md
# MAGIC Veamos el resultado.

# COMMAND ----------

spark.sql("SELECT *, date, to_date_format_udf(date) AS date_fm FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizamos spark.sql para contar el número de datos.

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM us_delay_flights_tbl").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1:
# MAGIC 
# MAGIC  Buscar todos los vuelos cuya distancia entre origen y destino sea superior a 1000 

# COMMAND ----------

spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Una consulta equivalente a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC Primer ejemplo utilizamos col en el where y desc antes de la columna para ordenar.

# COMMAND ----------

df.select("distance", "origin", "destination").where(col("distance") > 1000).orderBy(desc("distance")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC En el segundo ejemplo utilizamos $ en vez de col.

# COMMAND ----------

df.select("distance", "origin", "destination").where("distance > 1000").orderBy("distance", ascending=False).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC En el último ejemplo utilizamos $ en vez de col y en el order By $ y el nombre_de_la_columna.desc

# COMMAND ----------

df.select("distance", "origin", "destination").where("distance > 1000").orderBy(desc("distance")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2:
# MAGIC 
# MAGIC  Descubra todos los vuelos con retrasos de 2 horas entre San Francisco y Chicago  

# COMMAND ----------

spark.sql("""
SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC
""").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Una consulta equivalente a DataFrame

# COMMAND ----------

df.select("date","delay", "origin", "destination").where((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")).orderBy(desc("delay")).show(15)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3:
# MAGIC 
# MAGIC Una consulta más complicada en SQL, etiquetemos todos los vuelos de EE.UU. con origen en aeropuertos con _alto_, _medio_, _bajo_, _sin retrasos_, independientemente de los destinos.

# COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
                  WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
                  WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'No Delays'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Una consulta equivalente a DataFrame

# COMMAND ----------

df.selectExpr("delay", "origin", "destination",
              "CASE WHEN delay > 360 THEN 'Retraso muy grande' " + 
              "WHEN delay > 120 AND delay < 360 THEN 'Gran Retraso' " +
              "WHEN delay > 60 AND delay < 120 THEN 'Pequeño Retraso' " + 
              "WHEN delay > 0 and delay < 60 THEN 'Retraso Tolerable' " +
              "WHEN delay = 0 THEN 'Sin retraso' " +
              "ELSE 'Sin retraso' END AS Flight_Delays").orderBy("origin", desc("delay")).show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC También podriamos hacer:

# COMMAND ----------

df.select("delay", "origin", "destination").withColumn("Flight_Delays", 
                  when(col("delay") > 360, "Retraso muy grande").otherwise(
                  when(col("delay") > 120, "Gran Retraso").otherwise(
                  when(col("delay") > 60, "Pequeño Retraso").otherwise(
                  when(col("delay") > 0, "Retraso Tolerable").otherwise(
                  when(col("delay") == 0, "Sin retraso").otherwise("Sin retraso")))))).orderBy(col("origin"), desc("delay")).show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Algunas Consultas Secundarias

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferencia entre GlobalTempView vs TempView

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC En Apache Spark, las vistas temporales son una forma de simplificar las consultas complejas al crear una vista de un subconjunto de los datos y utilizar esa vista en lugar del conjunto completo de datos.
# MAGIC 
# MAGIC Hay dos tipos de vistas temporales en Apache Spark:
# MAGIC 
# MAGIC   1. Global Temp View: Una vista temporal global es visible en todas las sesiones y todos los usuarios de un Spark cluster. Es creada mediante el método "createGlobalTempView" y su nombre debe ser precedido por "global_temp."
# MAGIC 
# MAGIC   2. Temp View: Una vista temporal es visible solo en la sesión actual y solo para el usuario actual. Es creada mediante el método "createOrReplaceTempView" y no necesita ningún prefijo especial en su nombre.
# MAGIC 
# MAGIC En resumen la principal diferencia entre ambas es que GlobalTempView es visible para todas las sesiones y usuarios mientras que TempView solo es visible para la sesión actual.
# MAGIC 
# MAGIC Es importante mencionar que las vistas temporales solo existen mientras la sesión de spark está activa, una vez que se cierra la sesión o se desconecta.

# COMMAND ----------

# MAGIC %md
# MAGIC Seleccionamos los vuelos con origen e SFO.

# COMMAND ----------

df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista temporal global.

# COMMAND ----------

df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC Los seleccionamos todo de la GlobalTempView creada utilizando SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC Eliminamos la vista temporal creada.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS global_temp.us_origin_airport_JFK_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos otra selección de datos, esta vez con origen en 'JFK'

# COMMAND ----------

df2 = spark.sql("SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'")

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos una vista temporal global.

# COMMAND ----------

df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC Los seleccionamos todo de la GlobalTempView creada utilizando SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC Eliminamos la vista temporal creada.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC La línea de código que se está mostrando utiliza el método "listTables" del objeto "spark.catalog" de Apache Spark para mostrar las tablas en una base de datos específica creada anteriormente y llamada "global_temp" .

# COMMAND ----------

spark.catalog.listTables(dbName="global_temp")