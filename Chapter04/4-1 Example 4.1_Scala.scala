// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Ejemplo 4.1 Scala
// MAGIC 
// MAGIC Este cuaderno muestra el Ejemplo 4.1 del libro de cómo usar SQL en un en el dataset US Flights, utilizando scala.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC Defina una UDF para convertir el formato de fecha en un formato legible.
// MAGIC 
// MAGIC Nota: la fecha es una cadena a la que le falta el año, por lo que puede resultar difícil realizar consultas utilizando la función SQL `year()`.

// COMMAND ----------

def toDateFormatUDF(dStr:String) : String  = {
  return s"${dStr(0)}${dStr(1)}${'/'}${dStr(2)}${dStr(3)}${' '}${dStr(4)}${dStr(5)}${':'}${dStr(6)}${dStr(7)}"
}

// test  it
toDateFormatUDF("02190925")

// COMMAND ----------

// MAGIC %md
// MAGIC Registrar el paso a una fecha legible, la función se está registrando en el contexto de Apache Spark mediante el método "register" en el objeto "spark.udf". 

// COMMAND ----------

spark.udf.register("toDateFormatUDF", toDateFormatUDF(_:String):String)

// COMMAND ----------

// MAGIC %md
// MAGIC Leemos el US departure flight data

// COMMAND ----------

val df = spark
  .read
  .format("csv")
  .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
  .option("header", "true")
  .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
  .load()

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Esquema

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Probemos nuestra UDF

// COMMAND ----------

df.selectExpr("toDateFormatUDF(date) as data_format").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Crear una vista temporal en la que podamos realizar consultas SQL.

// COMMAND ----------

df.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC Cache Table para agilizar las consultas

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE us_delay_flights_tbl

// COMMAND ----------

// MAGIC %md
// MAGIC Convertir todo `date` a `date_fm` para que sea más elegible.

// COMMAND ----------

spark.sql("SELECT *, date, toDateFormatUDF(date) AS date_fm FROM us_delay_flights_tbl").show(10, false)

// COMMAND ----------

spark.sql("SELECT COUNT(*) FROM us_delay_flights_tbl").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 1:
// MAGIC 
// MAGIC  Buscar todos los vuelos cuya distancia entre origen y destino sea superior a 1000 

// COMMAND ----------

spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Una consulta equivalente a DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC Primer ejemplo utilizamos col en el where y desc antes de la columna para ordenar.

// COMMAND ----------

df.select("distance", "origin", "destination").where(col("distance") > 1000).orderBy(desc("distance")).show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC En el segundo ejemplo utilizamos $ en vez de col.

// COMMAND ----------

df.select("distance", "origin", "destination").where($"distance" > 1000).orderBy(desc("distance")).show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC En el último ejemplo utilizamos $ en vez de col y en el orderBy $ y el nombre_de_la_columna.desc

// COMMAND ----------

df.select("distance", "origin", "destination").where($"distance" > 1000).orderBy($"distance".desc).show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 2:
// MAGIC 
// MAGIC  Averiguar todos los vuelos con 2 horas de retraso entre San Francisco y Chicago   

// COMMAND ----------

spark.sql("""
SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC
""").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Una consulta equivalente a DataFrame

// COMMAND ----------

df.select("date","delay", "origin", "destination").where($"delay" > 120 && $"origin"==="SFO" && $"destination"==="ORD").orderBy($"delay".desc).show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 3:
// MAGIC 
// MAGIC Una consulta más complicada en SQL, etiquetemos todos los vuelos de EE.UU. con origen en aeropuertos con _alto_, _medio_, _bajo_, _sin retrasos_, independientemente de los destinos.

// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Retrago muy grande'
                  WHEN delay > 120 AND delay < 360 THEN  'Gran Retraso '
                  WHEN delay > 60 AND delay < 120 THEN  'Pequeño Retraso'
                  WHEN delay > 0 and delay < 60  THEN   'Retraso Tolerable'
                  WHEN delay = 0 THEN 'Sin retraso'
                  ELSE 'Sin retraso'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(20, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Una consulta equivalente en DataFrame

// COMMAND ----------

df.selectExpr("delay", "origin", "destination",
              "CASE WHEN delay > 360 THEN 'Retraso muy grande' " + 
              "WHEN delay > 120 AND delay < 360 THEN 'Gran Retraso' " +
              "WHEN delay > 60 AND delay < 120 THEN 'Pequeño Retraso' " + 
              "WHEN delay > 0 and delay < 60 THEN 'Retraso Tolerable' " +
              "WHEN delay = 0 THEN 'Sin retraso' " +
              "ELSE 'Sin retraso' END AS Flight_Delays").orderBy($"origin", desc("delay")).show(20, false)

// COMMAND ----------

// MAGIC %md También podriamos hacer, en DataFrame API

// COMMAND ----------

df.select("delay", "origin", "destination").withColumn("Flight_Delays", 
                  when($"delay" > 360, "Retraso muy grande").otherwise(
                  when($"delay"> 120, "Gran Retraso").otherwise(
                  when($"delay" > 60, "Pequeño Retraso").otherwise(
                  when($"delay" > 0, "Retraso Tolerable").otherwise(
                  when($"delay" === 0, "Sin retraso").otherwise("Sin retraso")))))).orderBy($"origin", desc("delay")).show(20, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Algunas preguntas complementarias

// COMMAND ----------

// MAGIC %md
// MAGIC ## Diferencia entre GlobalTempView vs TempView

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC En Apache Spark, las vistas temporales son una forma de simplificar las consultas complejas al crear una vista de un subconjunto de los datos y utilizar esa vista en lugar del conjunto completo de datos.
// MAGIC 
// MAGIC Hay dos tipos de vistas temporales en Apache Spark:
// MAGIC 
// MAGIC   1. Global Temp View: Una vista temporal global es visible en todas las sesiones y todos los usuarios de un Spark cluster. Es creada mediante el método "createGlobalTempView" y su nombre debe ser precedido por "global_temp."
// MAGIC 
// MAGIC   2. Temp View: Una vista temporal es visible solo en la sesión actual. Es creada mediante el método "createOrReplaceTempView" y no necesita ningún prefijo especial en su nombre.
// MAGIC 
// MAGIC En resumen la principal diferencia entre ambas es que GlobalTempView es visible para todas las sesiones y usuarios mientras que TempView solo es visible en esa sesión.
// MAGIC 
// MAGIC Es importante mencionar que las vistas temporales solo existen mientras la sesión de spark está activa, una vez que se cierra la sesión o se desconecta.

// COMMAND ----------

// MAGIC %md
// MAGIC Seleccionamos los vuelos con origen e SFO.

// COMMAND ----------

val df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una vista temporal global.

// COMMAND ----------

df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC Los seleccionamos todo de la GlobalTempView creada utilizando SQL.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC Eliminamos la vista temporal creada.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS global_temp.us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos otra selección de datos, esta vez con origen en 'JFK'

// COMMAND ----------

val df2 = spark.sql("SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'")

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una vista temporal global.

// COMMAND ----------

df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC Los seleccionamos todo de la GlobalTempView creada utilizando SQL.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC Eliminamos la vista temporal creada.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC La línea de código que se está mostrando utiliza el método "listTables" del objeto "spark.catalog" de Apache Spark para mostrar las tablas en una base de datos específica creada anteriormente y llamada "global_temp" .

// COMMAND ----------

display(spark.catalog.listTables(dbName="global_temp"))