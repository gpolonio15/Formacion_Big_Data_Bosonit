// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # San Francisco Fire Calls
// MAGIC 
// MAGIC Este cuaderno es el ejemplo completo del Capítulo 3, que muestra cómo utilizar DataFrame y Spark SQL para patrones y operaciones comunes de análisis de datos en el conjunto de datos [San Francisco Fire Department Calls ](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3) dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC Ubicación en la que se almacena el conjunto de datos de llamadas de incendios del Departamento de Bomberos de San Francisco.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv

// COMMAND ----------

// MAGIC %md
// MAGIC Definir la ubicación del conjunto de datos público 

// COMMAND ----------

// MAGIC %md
// MAGIC Cargamos el csv.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types._ 
// MAGIC import org.apache.spark.sql.functions._ 
// MAGIC 
// MAGIC val sfFireFile = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC Inspeccionar el aspecto de los datos antes de definir el schema

// COMMAND ----------

// MAGIC %fs head databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv

// COMMAND ----------

// MAGIC %md
// MAGIC Definir nuestro esquema ya que el fichero tiene 4 millones de registros. Inferir el esquema es costoso para archivos grandes.

// COMMAND ----------

// MAGIC %scala
// MAGIC val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
// MAGIC   StructField("UnitID", StringType, true),
// MAGIC   StructField("IncidentNumber", IntegerType, true),
// MAGIC   StructField("CallType", StringType, true),                  
// MAGIC   StructField("CallDate", StringType, true),      
// MAGIC   StructField("WatchDate", StringType, true),
// MAGIC   StructField("CallFinalDisposition", StringType, true),
// MAGIC   StructField("AvailableDtTm", StringType, true),
// MAGIC   StructField("Address", StringType, true),       
// MAGIC   StructField("City", StringType, true),       
// MAGIC   StructField("Zipcode", IntegerType, true),       
// MAGIC   StructField("Battalion", StringType, true),                 
// MAGIC   StructField("StationArea", StringType, true),       
// MAGIC   StructField("Box", StringType, true),       
// MAGIC   StructField("OriginalPriority", StringType, true),       
// MAGIC   StructField("Priority", StringType, true),       
// MAGIC   StructField("FinalPriority", IntegerType, true),       
// MAGIC   StructField("ALSUnit", BooleanType, true),       
// MAGIC   StructField("CallTypeGroup", StringType, true),
// MAGIC   StructField("NumAlarms", IntegerType, true),
// MAGIC   StructField("UnitType", StringType, true),
// MAGIC   StructField("UnitSequenceInCallDispatch", IntegerType, true),
// MAGIC   StructField("FirePreventionDistrict", StringType, true),
// MAGIC   StructField("SupervisorDistrict", StringType, true),
// MAGIC   StructField("Neighborhood", StringType, true),
// MAGIC   StructField("Location", StringType, true),
// MAGIC   StructField("RowID", StringType, true),
// MAGIC   StructField("Delay", FloatType, true)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Creamos una estructura de datos en Spark

// COMMAND ----------

// MAGIC %scala
// MAGIC val fireDF = spark
// MAGIC   .read
// MAGIC   .schema(fireSchema)
// MAGIC   .option("header", "true")
// MAGIC   .csv(sfFireFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Guarda en cache el DataFrame ya que realizaremos algunas operaciones sobre él.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireDF.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC Número de datos

// COMMAND ----------

// MAGIC %scala
// MAGIC fireDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Imprimos el esquema.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Seleccionamos 7 columnas del conjunto.

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fireDF.limit(7))

// COMMAND ----------

// MAGIC %md
// MAGIC Filtrar los tipos de llamada "Medical Incident".
// MAGIC 
// MAGIC Tenga en cuenta que los métodos filter() y where() del DataFrame son similares.

// COMMAND ----------

// MAGIC %scala
// MAGIC val MIFireDF = fireDF
// MAGIC   .select("IncidentNumber", "AvailableDtTm", "CallType") 
// MAGIC   .where(col("CallType") === "Medical Incident")
// MAGIC 
// MAGIC MIFireDF.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-1) ¿Cuántos tipos distintos de llamadas se hicieron al Cuerpo de Bomberos?
// MAGIC 
// MAGIC Para estar seguros, no contemos las llamadas "nulas" en esa columna.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireDF.select("CallType").where(col("CallType").isNotNull).distinct().count()

// COMMAND ----------

// MAGIC %md
// MAGIC Se hicieron 32 tipos de llamadas.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-2) ¿Cuáles son los nombres de los distintos tipos de llamadas al Cuerpo de Bomberos?
// MAGIC 
// MAGIC Estos son los distintos tipos de llamadas al Cuerpo de Bomberos

// COMMAND ----------

// MAGIC %scala
// MAGIC fireDF.select("CallType").where(col("CallType").isNotNull).distinct().show(32, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-3) ¿Averiguar todos los tiempos de respuesta o retraso superiores a 5 minutos?
// MAGIC     1.Cambiar el nombre de la columna Delay - > ReponseDelayedinMins
// MAGIC     2.Devuelve un nuevo DataFrame
// MAGIC     3.Averigua todas las llamadas en las que el tiempo de respuesta al lugar del incendio se retrasó más de 5 minutos.

// COMMAND ----------

// MAGIC %scala
// MAGIC val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
// MAGIC newFireDF.select("Address","ResponseDelayedinMins").where($"ResponseDelayedinMins" > 5).show(30, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Hagamos un poco de ETL:
// MAGIC 
// MAGIC 1. Transformar las fechas de cadena a tipo de datos Spark Timestamp para que podamos hacer algunas consultas basadas en el tiempo más tarde.
// MAGIC 2. Devuelve una consulta transformada
// MAGIC 3. Almacena en caché el nuevo DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC Transformamos las fechas.

// COMMAND ----------

// MAGIC %scala
// MAGIC val fireTSDF = newFireDF
// MAGIC   .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate") 
// MAGIC   .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate") 
// MAGIC   .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.cache()
// MAGIC fireTSDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Compruebe las columnas transformadas con el tipo Spark Timestamp

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-4) ¿Cuáles son los tipos de llamada más comunes?
// MAGIC 
// MAGIC Enuméralas en orden descendente

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF
// MAGIC   .select("CallType")
// MAGIC   .where($"CallType".isNotNull)
// MAGIC   .groupBy("CallType")
// MAGIC   .count()
// MAGIC   .orderBy(desc("count"))
// MAGIC   .show(32, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-4a) ¿En qué códigos postales se produjeron la mayoría de las llamadas?
// MAGIC 
// MAGIC Investiguemos en qué códigos postales de San Francisco se produjeron más llamadas de bomberos y de qué tipo fueron.
// MAGIC 
// MAGIC 1. Filtrar por tipo de llamada
// MAGIC 2. Agrupar por tipo de llamada y código postal
// MAGIC 3. Contarlas y mostrarlas en orden descendente

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF
// MAGIC   .select("CallType", "ZipCode")
// MAGIC   .where(col("CallType").isNotNull)
// MAGIC   .groupBy("CallType", "Zipcode")
// MAGIC   .count()
// MAGIC   .orderBy(desc("count"))
// MAGIC   .show(40, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Parece que la mayoría de llamadas son por Medical Incident y en los Zipcode  94102,94103,94110 y 94109

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-4b) ¿Qué barrios de San Francisco se encuentran en los códigos postales 94102 y 94103?
// MAGIC 
// MAGIC Averigüemos los barrios asociados a estos dos códigos postales. Con toda probabilidad, estos son algunos de los 
// MAGIC vecindarios con altos reportes de crímenes.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.select("Neighborhood", "Zipcode").where($"Zipcode" === 94102 || $"Zipcode" === 94103).distinct().show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-5) ¿Cuál fue la suma de todas las llamadas, la media, el mínimo y el máximo de los tiempos de respuesta de las llamadas?
// MAGIC 
// MAGIC Utilicemos las funciones SQL integradas en Spark para calcular la suma, la media, el mínimo y el máximo de algunas columnas:
// MAGIC 
// MAGIC * Número de Alarmas Totales
// MAGIC * Cuál fue el tiempo mínimo y máximo de respuesta antes de que los bomberos llegaran al lugar de la llamada.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-6a) ¿Cuántos años distintos de datos hay en el archivo CSV?
// MAGIC 
// MAGIC Podemos utilizar la función `year()` de SQL Spark del tipo de datos de la columna Timestamp IncidentDate.

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC En total, tenemos llamadas de bomberos de los años 2000-2018

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-6b) ¿En qué semana del año de 2018 hubo más llamadas por incendios?

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.filter(year($"IncidentDate") === 2018).groupBy(weekofyear($"IncidentDate")).count().orderBy(desc("count")).show(45)

// COMMAND ----------

// MAGIC %md
// MAGIC La semana 1 puede ser debido ser debido a la celebración de año nuevo y la del 25 debido al 4 de julio.

// COMMAND ----------

// MAGIC %md
// MAGIC #### P-7) ¿Qué barrios de San Francisco tuvieron el peor tiempo de respuesta en 2018?

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.select("Neighborhood", "ResponseDelayedinMins").filter(year($"IncidentDate") === 2018).orderBy(desc("ResponseDelayedinMins")).show(1000, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Debe de haber un problema, debido a que es demasiado tiempo.

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-8a) ¿Cómo podemos utilizar archivos Parquet o tablas SQL para almacenar datos y volver a leerlos?

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")

// COMMAND ----------

// MAGIC %fs ls /tmp/fireServiceParquet/

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-8b) ¿Cómo podemos utilizar una tabla Parquet SQL para almacenar datos y volver a leerlos?

// COMMAND ----------

// MAGIC %scala
// MAGIC fireTSDF.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls_2")

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE FireServiceCalls_2

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM FireServiceCalls_2 LIMIT 20

// COMMAND ----------

// MAGIC %md
// MAGIC ##### P-8c) ¿Cómo se pueden leer datos de un archivo Parquet?
// MAGIC Tenga en cuenta que no tenemos que especificar el esquema aquí, ya que se almacena como parte de los metadatos Parquet

// COMMAND ----------

// MAGIC %scala
// MAGIC val fileParquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileParquetDF.limit(20))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Ejercicios complementarios.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1) Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.

// COMMAND ----------

// MAGIC %scala
// MAGIC // Lo leemos
// MAGIC val mnmFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

// COMMAND ----------

// MAGIC %scala
// MAGIC // Lo leemos en spark
// MAGIC val mnmDF = spark
// MAGIC   .read
// MAGIC   .format("csv")
// MAGIC   .option("header", "true")
// MAGIC   .option("inferSchema", "true")
// MAGIC   .load(mnmFile)
// MAGIC 
// MAGIC display(mnmDF)

// COMMAND ----------

// MAGIC %scala
// MAGIC // Leemos directamente el esquema, ya que hemos utilizado option("inferSchema", "true")
// MAGIC mnmDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2) Cuando se define un schema al definir un campo por ejemplo StructField('Delay', FloatType(), True) ¿qué significa el último parámetro Boolean?

// COMMAND ----------

// MAGIC %md 
// MAGIC El último parámetro booleano en la definición de un campo en un esquema de Spark (como en el ejemplo dado: StructField('Delay', FloatType(), True)) indica si el campo especificado es obligatorio o no. Si es True, el campo es obligatorio y debe tener un valor válido en cada fila de los datos. Si es False, el campo es opcional y puede ser nulo en algunas filas de los datos.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3) Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?

// COMMAND ----------

// MAGIC %md
// MAGIC En Spark, un Dataset es una colección de objetos con un esquema definido que se extiende de un DataFrame. Un DataFrame es una colección de datos distribuidos con un esquema definido. A nivel de código, la principal diferencia entre un Dataset y un DataFrame es el tipo de objetos que contienen.
// MAGIC 
// MAGIC Un DataFrame es una representación de los datos en formato tabular con columnas y filas, donde las columnas son de tipo org.apache.spark.sql.Column y las filas son de tipo org.apache.spark.sql.Row. Se puede acceder a los valores de las columnas mediante el nombre de la columna como una propiedad de la fila.
// MAGIC 
// MAGIC Por otro lado, un Dataset es una colección de objetos de un tipo específico, donde cada objeto representa una fila en los datos. Cada objeto tiene propiedades que corresponden a las columnas en el esquema del DataFrame. El acceso a los valores de las columnas se realiza mediante las propiedades de objeto.
// MAGIC 
// MAGIC En resumen, los DataFrames son una representación de los datos en formato tabular, y los Datasets son una colección de objetos de un tipo específico con un esquema definido, con mejores características de rendimiento y seguridad tipada.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4) Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar los datos en los formatos:
// MAGIC     JSON
// MAGIC     CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
// MAGIC     AVRO

// COMMAND ----------

// MAGIC %scala
// MAGIC // JSON
// MAGIC fireTSDF.write.format("json").mode("overwrite").save("/tmp/fireServiceJson/")

// COMMAND ----------

// MAGIC %fs ls /tmp/fireServiceJson/

// COMMAND ----------

// MAGIC %scala
// MAGIC // CSV
// MAGIC fireTSDF.write.format("csv").mode("overwrite").option("header", "true").save("/tmp/fireServiceCSV/")

// COMMAND ----------

// MAGIC %fs ls /tmp/fireServiceCSV/

// COMMAND ----------

// MAGIC %scala
// MAGIC // AVRO
// MAGIC import org.apache.spark.sql.avro._
// MAGIC fireTSDF.write.format("avro").mode("overwrite").save("/tmp/fireServiceAVRO/")

// COMMAND ----------

// MAGIC %fs ls /tmp/fireServiceAVRO/

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5) Revisar al guardar los ficheros (p.e. json, csv, etc) el número de ficheros creados, revisar su contenido para comprender (constatar) como se guardan. 

// COMMAND ----------

// MAGIC %md
// MAGIC Abramos cada archivo guardado.

// COMMAND ----------

// MAGIC %scala
// MAGIC //Json
// MAGIC val fileJsonDF = spark.read.format("json").load("/tmp/fireServiceJson/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileJsonDF.limit(20))

// COMMAND ----------

// MAGIC %scala
// MAGIC //CSV
// MAGIC val fileCsvDF = spark.read.format("csv").option("header", "true").load("/tmp/fireServiceCSV/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileCsvDF.limit(20))

// COMMAND ----------

// MAGIC %scala
// MAGIC //AVRO
// MAGIC val fileAvroDF = spark.read.format("AVRO").load("/tmp/fireServiceAVRO/")

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fileAvroDF.limit(20))

// COMMAND ----------

// MAGIC %md
// MAGIC #####  ¿A qué se debe que hayan más de un fichero?

// COMMAND ----------

// MAGIC %md
// MAGIC Spark divide el archivo en varios partes y los guarda en diferentes archivos. Esto se debe a que Spark utiliza un sistema de distribución de archivos llamado Hadoop Distributed File System (HDFS) para almacenar y procesar grandes cantidades de datos. Al dividir el archivo en varias partes, Spark puede procesar y almacenar los datos de manera eficiente en un cluster de computadoras.

// COMMAND ----------

// MAGIC %md
// MAGIC #####  ¿Cómo obtener el número de particiones de un DataFrame?

// COMMAND ----------

// MAGIC %md
// MAGIC Puedes obtener el número de particiones de un DataFrame en Spark utilizando el método "rdd.getNumPartitions()" del DataFrame. Este método devuelve un entero que representa el número de particiones del DataFrame.

// COMMAND ----------

// MAGIC %scala
// MAGIC //Avro
// MAGIC val numPartitions = fileAvroDF.rdd.partitions.size
// MAGIC print("Número de particiones del DataFrame: ", numPartitions)

// COMMAND ----------

// MAGIC %scala
// MAGIC //Csv
// MAGIC val numPartitions = fileCsvDF.rdd.partitions.size
// MAGIC print("Número de particiones del DataFrame: ", numPartitions)

// COMMAND ----------

// MAGIC %scala
// MAGIC //Json
// MAGIC val numPartitions = fileJsonDF.rdd.partitions.size
// MAGIC print("Número de particiones del DataFrame: ", numPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC El que menos particiones tiene es el formato avro, seguido de csv, pero sin mucha diferencia. El que más el formato Json con 25 particiones.

// COMMAND ----------

// MAGIC  %md
// MAGIC ##### ¿Qué formas existen para modificar el número de particiones de un DataFrame?

// COMMAND ----------

// MAGIC %md
// MAGIC Existen varias formas de modificar el número de particiones de un DataFrame en Spark, algunas de las cuales son:
// MAGIC 
// MAGIC 1. Usando el método repartition(): Este método permite especificar el número deseado de particiones. Por ejemplo, para dividir un DataFrame en 10 particiones, se puede usar el siguiente código: df = df.repartition(10)
// MAGIC 
// MAGIC 2. Usando el método coalesce(): Este método permite combinar particiones existentes en un número específico. Por ejemplo, para combinar 5 particiones en una sola, se puede usar el siguiente código: df = df.coalesce(1)
// MAGIC 
// MAGIC 3. Usando el método partitionBy(): Este método permite dividir el DataFrame en particiones basadas en el valor de una columna específica. Por ejemplo, para dividir un DataFrame en particiones basadas en el valor de la columna "category", se puede usar el siguiente código: df = df.repartition(df['category'])

// COMMAND ----------

// MAGIC  %md
// MAGIC ##### Llevar a cabo el ejemplo modificando el número de particiones a 1 y revisar de nuevo el/los ficheros guardados.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.avro._
// MAGIC fireTSDF.coalesce(1).write.format("avro").mode("overwrite").save("/tmp/fireServiceAVRO/")

// COMMAND ----------

// MAGIC %scala
// MAGIC val fileAvroDF_1 = spark.read.format("AVRO").load("/tmp/fireServiceAVRO/")

// COMMAND ----------

// MAGIC %scala
// MAGIC //Avro
// MAGIC val numPartitions = fileAvroDF_1.rdd.partitions.size
// MAGIC print("Número de particiones del DataFrame: ", numPartitions)