// Databricks notebook source
// MAGIC %md
// MAGIC # Web Server logs Analysis

// COMMAND ----------

// MAGIC %md
// MAGIC Tenemos un web server log el cuál mantiene un historial de las peticiones realizadas a la página. Este tipo de server logs tienen un formato standard (Common Log Format). 

// COMMAND ----------

// MAGIC %md
// MAGIC En nuestro caso tenemos el dataset de los web server logs de la NASA. Qué están compuestos por este tipo de registros:
// MAGIC 
// MAGIC 133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713

// COMMAND ----------

// MAGIC %md
// MAGIC Por lo que tenemos los siguientes campos:
// MAGIC 1. Host: 133.43.96.45
// MAGIC 
// MAGIC 2. User-identifier: en este dataset, todos estos campos estarán con un “-“.
// MAGIC 
// MAGIC 3. Userid: al igual que el anterior campo, también será obviado.
// MAGIC 
// MAGIC 4. Date: 01/Aug/1995:00:00:23 -0400, como podemos ver está en formato 
// MAGIC dd/MMM/yyyy:HH:mm:ss y el campo final “-0400” sería el timezone.
// MAGIC 
// MAGIC 5. Request Method: GET.
// MAGIC 
// MAGIC 6. Resource: /images/launch-logo.gif, sería el recurso al que se accede en esta 
// MAGIC petición.
// MAGIC 
// MAGIC 7. Protocol: HTTP/1.0.
// MAGIC 
// MAGIC 8. HTTP status code: 200.
// MAGIC 
// MAGIC 9. Size: 1713, y como ultimo campo tendríamos el tamaño del objeto recibido por el 
// MAGIC cliente en bytes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Comenzemos cargando los datos.

// COMMAND ----------

val Nasa_DF = spark.read.text("/FileStore/tables/NASA_access_log_Jul95.gz")

// COMMAND ----------

// MAGIC %md
// MAGIC Veamos propiedades del DataFrame creado:

// COMMAND ----------

Nasa_DF.printSchema()

// COMMAND ----------

Nasa_DF.show(5, truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC Lo convertimos a un marco de datos en un RDD 

// COMMAND ----------

val NasaDF_rdd = Nasa_DF.rdd 
NasaDF_rdd.getClass 

// COMMAND ----------

// MAGIC %md
// MAGIC ##  Ejemplo de como sacar columnas sueltas del dataframe completo.

// COMMAND ----------

// MAGIC %md
// MAGIC Echemos un vistazo a la cantidad total de registros con los que estamos trabajando en nuestro conjunto de datos.

// COMMAND ----------

print(NasaDF_rdd.count())

// COMMAND ----------

// MAGIC %md
// MAGIC #### Extracción de nombres del host

// COMMAND ----------

val sampleLogs = NasaDF_rdd.take(15).map(row => row.getAs[String]("value"))

// COMMAND ----------

import scala.util.matching.Regex

val hostPattern = "(^\\S+\\.[\\S+\\.]+\\S+)\\s".r
val hosts = sampleLogs.map { log =>
  hostPattern.findFirstIn(log).getOrElse("no match")
}


// COMMAND ----------

// MAGIC %md
// MAGIC Hemos clasificado bien el host.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Fechas.

// COMMAND ----------

// MAGIC %md
// MAGIC Primero pasar los meses a numéricos.

// COMMAND ----------


import scala.util.matching.Regex

val datePattern: Regex = """(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})""".r

//Meses a numéricos.
val dateMap: Map[String, String] = Map("Jan" -> "01", "Feb" -> "02", "Mar" -> "03", "Apr" -> "04", "May" -> "05", "Jun" -> "06", "Jul" -> "07", "Aug" -> "08", "Sep" -> "09", "Oct" -> "10", "Nov" -> "11", "Dec" -> "12")

val dates = sampleLogs.map { log =>
  datePattern.findFirstMatchIn(log) match {
    case Some(dateTimeMatch) =>
      val (day, month, year, hour, minute, second) = (dateTimeMatch.group(1), dateTimeMatch.group(2), dateTimeMatch.group(3),dateTimeMatch.group(4), dateTimeMatch.group(5), dateTimeMatch.group(6))
      val formattedMonth = dateMap(month)
      s"$day/$formattedMonth/$year $hour:$minute:$second"
    case None => "no match"
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### Una vez ya hemos visto como se pueden separar y modificar las fechas, hagamos un código completo para separar el array en las columnas que queremos.

// COMMAND ----------

val apache_regex = """(\S+) (\S+) (\S+) \[(\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\] "(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) (\S+) (\S+)" (\d{3}) (\S+)""".r

// COMMAND ----------

// MAGIC %md
// MAGIC A continuación definieremos la función se usa para parsear y formatear una cadena de fecha en el formato de registro de Apache a un nuevo formato de "AAAA-MM-DD HH:MM:SS".
// MAGIC 
// MAGIC En primer lugar, la función inicializa un mapa map_meses que asigna los nombres abreviados de los meses en el formato de registro de Apache a su representación numérica.
// MAGIC 
// MAGIC A continuación, la función fecha utiliza un método String.format para construir la cadena de salida deseada. La cadena de entrada fecha se divide en diferentes subcadenas en función de su posición y las subcadenas se utilizan para construir la cadena de salida en el formato deseado. La correspondencia entre los nombres abreviados de los meses y su representación numérica se realiza mediante el mapa map_meses.

// COMMAND ----------

// MAGIC %md
// MAGIC La fecha ha de ser con el siguiente formato: Año-Mes-Día horas.

// COMMAND ----------

val map_meses = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8, "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

def fecha(date: String): String = {
  "%1$s-%2$s-%3$s %4$s:%5$s:%6$s".format(
    date.substring(7, 11),
    map_meses(date.substring(3, 6)),
    date.substring(0, 2),
    date.substring(12, 14),
    date.substring(15, 17),
    date.substring(18, 20)
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC Se espera que la expresión regular apache_regex capture campos específicos de una línea de registro de Apache y la función intenta extraer estos campos y devolverlos en un objeto Spark Row.
// MAGIC 
// MAGIC La función toma un único argumento logLine que es una representación de cadena de una línea de registro. Utiliza un patrón para ver si la línea de registro coincide con la expresión regular apache_regex. Si la línea de registro coincide con la expresión, la función extrae los campos relevantes y devuelve un Array que contiene un objeto Row y el número 1.
// MAGIC 
// MAGIC Si la línea de registro no coincide con la expresión regular apache_regex, la función devuelve un array que contiene la cadena original de la línea de registro y el número 0.

// COMMAND ----------

import org.apache.spark.sql.Row

def parseLog(logLine: String) : Array[Any] = {
    return logLine match {
        case apache_regex(host, user_identifier, user_id, datetime, request_method, resource, protocol, http_status, content_size) => 
        Array(
            Row(
                host,
                user_identifier,
                user_id,
                fecha(datetime),
                request_method,
                resource,
                protocol,
                http_status,
                if(content_size == "-") "0" else content_size
            ), 1
        )
        case _ => Array(logLine, 0)
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC El método textFile se utiliza para leer los archivos de registro en un Spark RDD (Resilient Distributed Dataset). A continuación, se utiliza el método map para aplicar la función parseLog a cada línea de registro en el RDD, transformando cada línea de registro en una matriz de un objeto Spark Row y el número 1, o una cadena y el número 0, según lo definido por la función parseLog. Por último, se llama al método cache en el RDD para almacenar en caché los datos transformados en la memoria para un acceso más rápido en el futuro.

// COMMAND ----------

val path = "/FileStore/tables/NASA_access_log_Jul95.gz"
val logs = sc.textFile(path).map(parseLog).cache()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC El array columnas especifica los nombres de las columnas en el DataFrame, en el orden en que deben aparecer.
// MAGIC 
// MAGIC  El esquema se construye mapeando el array de columnas a objetos StructField, donde cada StructField representa una única columna en el DataFrame. Los argumentos de cada StructField son el nombre de la columna, el tipo de datos (en este caso StringType) y una bandera que indica si el campo es anulable.
// MAGIC 
// MAGIC Finalmente, se crea un Spark DataFrame llamado Nada_DF utilizando el método spark.createDataFrame. Los datos para el DataFrame se obtienen del RDD de registros filtrando el RDD para incluir sólo las líneas de registro que se analizaron correctamente (es decir, donde log(1) == 1), y luego mapeando el RDD para extraer sólo los objetos Row (es decir, log(0).asInstanceOf[Row]). El RDD resultante se utiliza como fuente de datos para el método createDataFrame, junto con el esquema previamente definido.

// COMMAND ----------

import org.apache.spark.sql.types._
val columna = Array("host", "user_identifier", "user_id", "datetime", "request_method", "resource", "protocol", "http_status", "content_size")
val schema = StructType(
  columna.map(col => StructField(col, StringType, false))
)

// Creamos el dataframe.
val Nasa_DF = spark.createDataFrame(logs.filter(log => log(1) == 1).map(log => log(0).asInstanceOf[Row]), schema)

// COMMAND ----------

Nasa_DF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Utilizamos el método withColumn para crear una nueva columna en el DataFrame llamada "datetime". La nueva columna se crea convirtiendo la columna "datetime" de "Nasa_DF" en una marca de tiempo utilizando la función to_timestamp.
// MAGIC 
// MAGIC La siguiente línea crea una nueva columna llamada "http_status" convirtiendo la columna "http_status" de "Nasa_DF" en un entero mediante el método cast.
// MAGIC 
// MAGIC La última línea crea una nueva columna llamada "content_size" convirtiendo la columna "content_size" de "Nasa_DF" en un entero mediante el método cast.
// MAGIC 
// MAGIC Por último, se llama al método cache() en el DataFrame para almacenar los datos en memoria caché, de modo que sea más rápido acceder a los datos para operaciones posteriores.

// COMMAND ----------

// MAGIC %md
// MAGIC Importante poner el Datetime: Año-Mes-Día y hora. Si no cuando quieras utilizar to_timestamp, te saldrán todos los valores en Null.

// COMMAND ----------

import org.apache.spark.sql.functions._
val Access_Nasa= ( Nasa_DF
  .withColumn("datetime", to_timestamp($"datetime"))
  .withColumn("http_status", $"http_status".cast("int"))
  .withColumn("content_size", $"content_size".cast("int"))
)
Access_Nasa.cache()
Access_Nasa.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Esquema de los datos:

// COMMAND ----------

Access_Nasa.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC # Consultas a realizar

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

// COMMAND ----------

Access_Nasa.select("protocol").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC En Spark SQL.

// COMMAND ----------

Access_Nasa.createOrReplaceTempView("nasa_logs")

// COMMAND ----------

spark.sql("SELECT DISTINCT protocol FROM nasa_logs").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Los distintos protocolos posibles para utilizar son: 
// MAGIC  1. HTTP/*
// MAGIC 2. HTTP/V1.0
// MAGIC 3. HTTP/1.0
// MAGIC 4. STS-69</a><p>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.

// COMMAND ----------

Access_Nasa.select("http_status")
 .groupBy("http_status")
 .agg(count("http_status").alias("Total_http_status"))
 .orderBy(desc("Total_http_status"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Spark SQL

// COMMAND ----------

spark.sql("SELECT http_status, COUNT(http_status) AS Total_http_status FROM nasa_logs GROUP BY http_status ORDER BY Total_http_status DESC").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Los códigos de estado más comunes en la web son 200, 304, 302 y 404.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. ¿Y los métodos de petición (verbos) más utilizados?

// COMMAND ----------

Access_Nasa.select("request_method")
 .groupBy("request_method")
 .agg(count("request_method").alias("Total_request_method"))
 .orderBy(desc("Total_request_method"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Spark SQL.

// COMMAND ----------

spark.sql("SELECT request_method, COUNT(request_method) AS Total_request_method FROM nasa_logs GROUP BY request_method ORDER BY Total_request_method DESC").show()

// COMMAND ----------

// MAGIC %md
// MAGIC El método de petición (verbos) más utilizado es GET.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

// COMMAND ----------

Access_Nasa.select("resource","content_size")
 .orderBy(desc("content_size"))
 .show(1,false)

// COMMAND ----------

// MAGIC %md
// MAGIC En spark SQL.

// COMMAND ----------

spark.sql("SELECT resource, content_size FROM nasa_logs ORDER BY content_size DESC LIMIT 1").show(false)

// COMMAND ----------

// MAGIC 
// MAGIC %md
// MAGIC El recurso que tuvo la mayor transferencia de bytes es /shuttle/countdown/video/livevideo.jpeg

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.

// COMMAND ----------

Access_Nasa.select("resource")
  .groupBy("resource")
  .agg(count("resource").alias("Total_resource"))
  .orderBy(desc("Total_resource"))
  .show(1,false)

// COMMAND ----------

// MAGIC %md
// MAGIC Spark SQL.

// COMMAND ----------

spark.sql("SELECT resource, COUNT(resource) AS Total_resource FROM nasa_logs GROUP BY resource ORDER BY Total_resource DESC LIMIT 1").show(false)

// COMMAND ----------

// MAGIC  %md
// MAGIC     El recurso de nuestra web que más tráfico recibe es /images/NASA-logosmall.gif

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. ¿Qué días la web recibió más tráfico?

// COMMAND ----------

Access_Nasa.select(date_trunc("day",$"datetime").alias("trafic_day"))
 .groupBy("trafic_day")
 .agg(count("trafic_day").alias("Total_trafic"))
 .orderBy(desc("Total_trafic"))
 .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Spark Scala.

// COMMAND ----------

spark.sql("SELECT date_trunc('day', datetime) AS trafic_day, count(date_trunc('day', datetime)) as Total_trafic FROM Nasa_logs GROUP BY trafic_day ORDER BY Total_trafic DESC").show()

// COMMAND ----------

// MAGIC %md Los días que más tráfico recibio nuestra web fueron el 13 de Julio, 6 y 5 de ese mismo mes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7.¿Cuáles son los hosts son los más frecuentes?

// COMMAND ----------

Access_Nasa.select("host")
 .groupBy("host")
 .agg(count("host").alias("Total_host"))
 .orderBy(desc("Total_host"))
 .show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC En spark SQL.

// COMMAND ----------

spark.sql("SELECT host, COUNT(host) AS Total_host FROM nasa_logs GROUP BY host ORDER BY Total_host DESC").show()

// COMMAND ----------

// MAGIC %md Los hots mas frecuentes son los de piwebay.prodigy.com

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8.¿A qué horas se produce el mayor número de tráfico en la web?

// COMMAND ----------

 Access_Nasa.select(hour($"datetime").alias("trafic_hour"))
 .groupBy("trafic_hour")
 .agg(count("trafic_hour").alias("Total_trafic"))
 .orderBy(desc("Total_trafic"))
 .show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC En spark SQL.

// COMMAND ----------

spark.sql("SELECT hour(datetime) AS trafic_hour, COUNT(hour(datetime)) AS Total_trafic FROM nasa_logs GROUP BY trafic_hour ORDER BY Total_trafic DESC").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC El medio día son las horas con más tráfico para nuestra web.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9.¿Cuál es el número de errores 404 que ha habido cada día?

// COMMAND ----------

Access_Nasa.where($"http_status" === 404)
  .select(date_format($"datetime", "yyyy-MM-dd").alias("date"), $"http_status")
  .groupBy("date", "http_status")
  .agg(count("date").alias("total"))
  .orderBy(desc("total"))
  .show(false)

// COMMAND ----------

spark.sql("SELECT date_format(datetime, 'yyyy-MM-dd') AS date, http_status, COUNT(date_format(datetime, 'yyyy-MM-dd')) AS Total FROM nasa_logs WHERE http_status=404 GROUP BY date, http_status ORDER BY Total DESC").show()

// COMMAND ----------

// MAGIC %md El error 404 se situa entorno a una media de 400 por día, llegando al máximo el 19 de julio con 636.