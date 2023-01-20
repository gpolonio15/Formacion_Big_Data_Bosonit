// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # IoT Devices
// MAGIC 
// MAGIC Definir en Scala un case class que asignará un map a un Scala Dataset: _DeviceIoTData_.

// COMMAND ----------

// MAGIC %md
// MAGIC Definimos la clase DeviceIoTData

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long, 
    cca2: String, cca3: String, cn: String, device_id: Long, 
    device_name: String, humidity: Long, ip: String, latitude: Double,
    lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

// COMMAND ----------

// MAGIC %md
// MAGIC Definir un Scala case class que se pueda mapear a un Scala Dataset: _DeviceTempByCountry_

// COMMAND ----------

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

// COMMAND ----------

// MAGIC %md
// MAGIC Leer archivos JSON con información del dispositivo
// MAGIC 
// MAGIC 1. El DataFrameReader devolverá un DataFrame y lo convertirá en Dataset[DeviceIotData].
// MAGIC 2. DS es una colección de Dataset que mapean a Scala case class _DeviceIotData_.ta_

// COMMAND ----------

val ds = spark.read.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

// MAGIC %md
// MAGIC Mapas de esquema a cada campo y tipo en el objeto de clase case de Scala

// COMMAND ----------

ds.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Mostramos una parte del dataframe

// COMMAND ----------

ds.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Utilice Dataset API para filtrar la temperatura y la humedad. Observe el uso de la sintaxis `object.field` empleada con Dataset JVM,
// MAGIC similar al acceso a campos JavaBean. Esta sintaxis no sólo es legible, sino que también es segura desde el punto de vista de la compilación. 
// MAGIC 
// MAGIC Por ejemplo, si comparas d.temp > "30", obtendrás un error de compilación.

// COMMAND ----------

val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})

// COMMAND ----------

filterTempDS.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Utilice una consulta más complicada con funciones lambda con el conjunto de datos original DeviceIoTData. Fíjate en el prefijo de los nombres de columna `_1`, `_2`, etc.
// MAGIC Esta es la forma de Spark de manejar los nombres de columnas desconocidos devueltos por un conjunto de datos cuando se utilizan consultas con expresiones lambda. Simplemente les cambiamos el nombre y las lanzamos
// MAGIC a nuestra clase definida _DeviceTempByCountry_.

// COMMAND ----------

val dsTemp = ds
  .filter(d => {d.temp > 25}).map(d => (d.temp, d.device_name, d.device_id, d.cca3))
  .withColumnRenamed("_1", "temp")
  .withColumnRenamed("_2", "device_name")
  .withColumnRenamed("_3", "device_id")
  .withColumnRenamed("_4", "cca3").as[DeviceTempByCountry]

dsTemp.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Esta consulta devuelve un _Dataset[Row]_ ya que no tenemos una clase case correspondiente a la que convertir, por lo que se devuelve un objeto genérico `Row
// MAGIC genérico.

// COMMAND ----------

ds.select($"temp", $"device_name", $"device_id", $"humidity", $"cca3", $"cn").where("temp > 25").show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC En cambio, estos resultados se corresponden bien con nuestra clase de caso _DeviceTempByCountry_.

// COMMAND ----------

val dsTemp2 = ds.select($"temp", $"device_name", $"device_id", $"device_id", $"cca3").where("temp > 25").as[DeviceTempByCountry]

dsTemp.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Utilice el método first() para consultar el primer objeto _DeviceTempByCountry_.

// COMMAND ----------

val device = dsTemp.first()

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-1) ¿Cómo detectar dispositivos averiados con batería baja por debajo de un umbral?
// MAGIC 
// MAGIC Nota: los umbrales inferiores a 8 son candidatos potenciales

// COMMAND ----------

ds.select($"battery_level", $"c02_level", $"device_name").where($"battery_level" < 8).sort($"c02_level").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-2) ¿Cómo identificar a los países infractores con altos niveles de emisiones de CO2?
// MAGIC 
// MAGIC Nota: Cualquier nivel de C02 superior a 1300 es un infractor potencial de emisiones de C02
// MAGIC 
// MAGIC Filtrar c02_levels es mayor que 1300, ordenar en orden descendente en C02_level. Tenga en cuenta que esta API de lenguaje específico de dominio de alto nivel se lee como una consulta SQL

// COMMAND ----------

val newDS = ds
  .filter(d => {d.c02_level > 1300})
  .groupBy($"cn")
  .avg()
  .sort($"avg(c02_level)".desc)

newDS.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-3) ¿Podemos clasificar y agrupar los países en función de la temperatura media, el C02 y la humedad

// COMMAND ----------

ds.filter(d => {d.temp > 25 && d.humidity > 75})
  .select("temp", "humidity", "cn")
  .groupBy($"cn")
  .avg()
  .sort($"avg(temp)".desc, $"avg(humidity)".desc).as("avg_humidity").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-4) ¿Podemos calcular los valores mínimos y máximos de temperatura, C02 y humedad?

// COMMAND ----------

import org.apache.spark.sql.functions._ 

ds.select(min("temp"), max("temp"), min("humidity"), max("humidity"), min("c02_level"), max("c02_level"), min("battery_level"), max("battery_level")).show(10)