# Databricks notebook source
# MAGIC %md
# MAGIC # Web Server logs Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Tenemos un web server log el cuál mantiene un historial de las peticiones realizadas a la página. Este tipo de server logs tienen un formato standard (Common Log Format). 

# COMMAND ----------

# MAGIC %md
# MAGIC En nuestro caso tenemos el dataset de los web server logs de la NASA. Qué están compuestos por este tipo de registros:
# MAGIC 
# MAGIC 133.43.96.45 - - [01/Aug/1995:00:00:23 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713

# COMMAND ----------

# MAGIC %md
# MAGIC Por lo que tenemos los siguientes campos:
# MAGIC 1. Host: 133.43.96.45
# MAGIC 
# MAGIC 2. User-identifier: en este dataset, todos estos campos estarán con un “-“.
# MAGIC 
# MAGIC 3. Userid: al igual que el anterior campo, también será obviado.
# MAGIC 
# MAGIC 4. Date: 01/Aug/1995:00:00:23 -0400, como podemos ver está en formato 
# MAGIC dd/MMM/yyyy:HH:mm:ss y el campo final “-0400” sería el timezone.
# MAGIC 
# MAGIC 5. Request Method: GET.
# MAGIC 
# MAGIC 6. Resource: /images/launch-logo.gif, sería el recurso al que se accede en esta 
# MAGIC petición.
# MAGIC 
# MAGIC 7. Protocol: HTTP/1.0.
# MAGIC 
# MAGIC 8. HTTP status code: 200.
# MAGIC 
# MAGIC 9. Size: 1713, y como ultimo campo tendríamos el tamaño del objeto recibido por el 
# MAGIC cliente en bytes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definimos el regex completo.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC  1. (\S+): Este grupo de captura se usa para capturar la dirección IP del cliente que realizó la solicitud.
# MAGIC 
# MAGIC 2. (\S+): Este grupo de captura se usa para capturar el identificador de cliente.
# MAGIC 
# MAGIC 3. (\S+): Este grupo de captura se usa para capturar el usuario remoto.
# MAGIC 
# MAGIC 4. (\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4}): captura la fecha y hora de la solicitud en formato "dd/MMM/yyyy:HH:mm:ss -xxxx".
# MAGIC 
# MAGIC 5. "(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH): captura el método HTTP utilizado en la solicitud.
# MAGIC 
# MAGIC 6. (\S+): captura la ruta solicitada. 
# MAGIC 
# MAGIC 7. (\S+): captura la versión del protocolo HTTP utilizada. 
# MAGIC 
# MAGIC 8. (\d{3}): captura el código de estado HTTP devuelto en la respuesta.
# MAGIC 
# MAGIC 9. (\S+): captura el tamaño de la respuesta en bytes.

# COMMAND ----------

import re

apache_regex = r'(\S+) (\S+) (\S+) \[(\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\] "(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) (\S+) (\S+)" (\d{3}) (\S+)'


# COMMAND ----------

meses = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}

def fecha(date):
    return "{0}-{1}-{2} {3}:{4}:{5}".format(
        date[7:11],
        meses[date[3:6]],
        date[0:2],
        date[12:14],
        date[15:17],
        date[18:20]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Este código toma una fecha en formato de cadena como entrada y devuelve la misma fecha en formato de cadena con el año, mes, día, hora, minuto y segundo separados por guiones y dos puntos. Por ejemplo, si se proporciona la cadena "15Feb2023 10:30:45", la función devolverá la cadena "2023-2-15 10:30:45".

# COMMAND ----------

from pyspark.sql import Row

def parseLog(logLine):
    import re
    
    apache_regex = r'(\S+) (\S+) (\S+) \[(\d{2}\/[A-Za-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\] "(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) (\S+) (\S+)" (\d{3}) (\S+)'
    match = re.match(apache_regex, logLine)
    if match:
        host = match.group(1)
        user_identifier = match.group(2)
        user_id = match.group(3)
        datetime = match.group(4)
        request_method = match.group(5)
        resource = match.group(6)
        protocol = match.group(7)
        http_status = match.group(8)
        content_size = match.group(9)
        content_size = "0" if content_size == "-" else content_size
        return [Row(host=host, user_identifier=user_identifier, user_id=user_id, datetime=fecha(datetime), request_method=request_method, resource=resource, protocol=protocol, http_status=http_status, content_size=content_size), 1]
    else:
        return [logLine, 0]


# COMMAND ----------

# MAGIC %md
# MAGIC La función primero compara la cadena logLine de entrada con la expresión regular utilizando re.match. Si hay coincidencia, extrae los grupos de captura y construye un objeto Row con los valores extraídos. Si el valor de content_size es "-", lo sustituye por "0". La función devuelve entonces una tupla con el objeto Row construido y un valor de 1. Si no hay coincidencia, la función devuelve una tupla con la cadena logLine original y un valor de 0.

# COMMAND ----------

path = "/FileStore/tables/NASA_access_log_Jul95.gz"
logs = sc.textFile(path).map(parseLog).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC El método textFile se utiliza para leer los archivos de registro en un Spark RDD (Resilient Distributed Dataset). A continuación, se utiliza el método map para aplicar la función parseLog a cada línea de registro en el RDD, transformando cada línea de registro en una matriz de un objeto Spark Row y el número 1, o una cadena y el número 0, según lo definido por la función parseLog. Por último, se llama al método cache en el RDD para almacenar en caché los datos transformados en la memoria para un acceso más rápido en el futuro.

# COMMAND ----------

from pyspark.sql.types import *

columna = ["host", "user_identifier", "user_id", "datetime", "request_method", "resource", "protocol", "http_status", "content_size"]
schema = StructType([StructField(col, StringType(), False) for col in columna])

# Creamos el dataframe.
Nasa_DF = spark.createDataFrame(logs.filter(lambda log: log[1] == 1).map(lambda log: log[0]), schema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC El primer paso es definir un esquema para el DataFrame, que es un objeto StructType que especifica los tipos de datos y los nombres de las columnas para el DataFrame. 
# MAGIC 
# MAGIC A continuación, el código filtra los registros para conservar sólo aquellos en los que el segundo elemento (es decir, log[1]) es igual a 1. Los registros resultantes se mapean para extraer el primer elemento (es decir, log[0]), que contiene los datos del registro en forma de objeto Row. 

# COMMAND ----------

Nasa_DF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Se han creado las columnas correctamente.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

Access_Nasa = (Nasa_DF
    .withColumn("datetime", to_timestamp(col("datetime")))
    .withColumn("http_status", col("http_status").cast("int"))
    .withColumn("content_size", col("content_size").cast("int"))
)

Access_Nasa.cache()
Access_Nasa.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Esquema de los datos:

# COMMAND ----------

Access_Nasa.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Importante poner el Datetime: Año-Mes-Día y hora. Si no cuando quieras utilizar to_timestamp, te saldrán todos los valores en Null. El http_status a entero al igual que el content size.

# COMMAND ----------

# MAGIC %md
# MAGIC # Consultas a realizar

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

# COMMAND ----------

Access_Nasa.select("protocol").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC En Spark SQL.

# COMMAND ----------

Access_Nasa.createOrReplaceTempView("nasa_logs")

# COMMAND ----------

spark.sql("SELECT DISTINCT protocol FROM nasa_logs").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Los distintos protocolos posibles para utilizar son: 
# MAGIC  1. HTTP/*
# MAGIC 2. HTTP/V1.0
# MAGIC 3. HTTP/1.0
# MAGIC 4. STS-69</a><p>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.

# COMMAND ----------

from pyspark.sql.functions import count, desc

Access_Nasa.select("http_status") \
          .groupBy("http_status") \
          .agg(count("http_status").alias("Total_http_status")) \
          .orderBy(desc("Total_http_status")) \
          .show()

# COMMAND ----------

# MAGIC %md
# MAGIC En Spark SQL.

# COMMAND ----------

spark.sql("SELECT http_status, COUNT (http_status) AS Total_http_status FROM nasa_logs GROUP BY http_status ORDER BY Total_http_status DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Los códigos de estado más comunes en la web son 200, 304, 302 y 404.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. ¿Y los métodos de petición (verbos) más utilizados?

# COMMAND ----------

Access_Nasa.select("request_method")\
            .groupBy("request_method")\
            .agg(count("request_method").alias("Total_request_method"))\
            .orderBy(desc("Total_request_method"))\
            .show()

# COMMAND ----------

spark.sql("SELECT request_method, COUNT (request_method) AS Total_request_method FROM nasa_logs  GROUP BY request_method ORDER BY Total_request_method DESC").show()

# COMMAND ----------

# MAGIC %md
# MAGIC El método de petición (verbos) más utilizado es GET.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

# COMMAND ----------

Access_Nasa.select("resource","content_size")\
             .orderBy(desc("content_size"))\
             .show(1, False)

# COMMAND ----------

spark.sql("SELECT resource,content_size FROM nasa_logs ORDER BY content_size DESC LIMIT 1").show(1,False)

# COMMAND ----------

# MAGIC %md
# MAGIC El recurso que tuvo la mayor transferencia de bytes es /shuttle/countdown/video/livevideo.jpeg

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.

# COMMAND ----------

Access_Nasa.select("resource")\
          .groupBy("resource")\
          .agg(count("resource").alias("Total_resource"))\
          .orderBy(desc("Total_resource"))\
          .show(1,False)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL.

# COMMAND ----------

spark.sql("SELECT resource, COUNT (resource) AS Total_resource FROM nasa_logs GROUP BY resource ORDER BY Total_resource DESC LIMIT 1").show(1, False)

# COMMAND ----------

# MAGIC  %md
# MAGIC     El recurso de nuestra web que más tráfico recibe es /images/NASA-logosmall.gif

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. ¿Qué días la web recibió más tráfico?

# COMMAND ----------

from pyspark.sql.functions import date_trunc

Access_Nasa.select(date_trunc("day", col("datetime")).alias("trafic_day")) \
            .groupBy("trafic_day") \
            .agg(count("trafic_day").alias("Total_trafic")) \
            .orderBy(desc("Total_trafic")) \
            .show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL.

# COMMAND ----------

spark.sql("SELECT date_trunc('day', datetime) AS trafic_day, count(date_trunc('day', datetime)) as Total_trafic FROM Nasa_logs GROUP BY trafic_day ORDER BY Total_trafic DESC LIMIT 5").show()

# COMMAND ----------

# MAGIC %md Los días que más tráfico recibio nuestra web fueron el 13 de Julio, 6 y 5 de ese mismo mes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.¿Cuáles son los hosts son los más frecuentes?

# COMMAND ----------

Access_Nasa.select("host") \
           .groupBy("host") \
           .agg(count("host").alias("Total_host")) \
           .orderBy(desc("Total_host")) \
           .show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL.

# COMMAND ----------

spark.sql("SELECT host, COUNT(host) AS Total_host FROM nasa_logs GROUP BY host ORDER BY Total_host DESC").show(5,False)

# COMMAND ----------

# MAGIC %md Los hots mas frecuentes son los de piwebay.prodigy.com

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.¿A qué horas se produce el mayor número de tráfico en la web?

# COMMAND ----------

from pyspark.sql.functions import hour

Access_Nasa.select(hour(col("datetime")).alias("trafic_hour")) \
           .groupBy("trafic_hour") \
           .agg(count("trafic_hour").alias("Total_trafic")) \
           .orderBy(desc("Total_trafic")) \
           .show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL.

# COMMAND ----------

spark.sql("SELECT hour(datetime) AS trafic_hour, COUNT(hour(datetime)) AS Total_trafic FROM nasa_logs GROUP BY trafic_hour ORDER BY Total_trafic DESC").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC El medio día son las horas con más tráfico para nuestra web.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.¿Cuál es el número de errores 404 que ha habido cada día?

# COMMAND ----------

from pyspark.sql.functions import date_format


Access_Nasa.where(col("http_status") == 404)\
          .select(date_format("datetime", "yyyy-MM-dd").alias("date"), col("http_status"))\
          .groupBy("date", "http_status")\
          .agg(count("date").alias("total"))\
          .orderBy(desc("total"))\
          .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL.

# COMMAND ----------

spark.sql("SELECT date_format(datetime, 'yyyy-MM-dd') AS date,http_status, COUNT(date_format(datetime, 'yyyy-MM-dd')) AS Total  FROM nasa_logs WHERE http_status==404 GROUP BY date, http_status ORDER BY Total DESC").show()

# COMMAND ----------

# MAGIC %md El error 404 se situa entorno a una media de 400 por día, llegando al máximo el 19 de julio con 636.