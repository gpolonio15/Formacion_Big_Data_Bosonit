// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # File Formats
// MAGIC 
// MAGIC En este cuaderno, veremos cómo los diferentes formatos de archivo afectan al rendimiento de tu trabajo Spark.
// MAGIC Spark Summit 2016: [Why You Should Care about Data Layout in the Filesystem](https://databricks.com/session/why-you-should-care-about-data-layout-in-the-filesystem)

// COMMAND ----------

// MAGIC %md
// MAGIC Leamos un archivo delimitado por dos puntos.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt

// COMMAND ----------

// MAGIC %fs head --maxBytes=1000 /databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt

// COMMAND ----------

// MAGIC %md
// MAGIC Separado por ":"

// COMMAND ----------

val csvDF = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ¿Son correctos estos tipos de datos? Todos ellos son tipos de cadena.
// MAGIC 
// MAGIC Tenemos que decirle a Spark que infiera el esquema.

// COMMAND ----------

val csvDF = spark
  .read
  .option("header", "true")
  .option("sep", ":")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt")

// COMMAND ----------

// MAGIC %md
// MAGIC Nos ha llevado mucho tiempo averiguar el esquema de este archivo
// MAGIC 
// MAGIC Ahora intentemos lo mismo con archivos comprimidos (formatos Gzip y Snappy).
// MAGIC 
// MAGIC Observa que el archivo gzip es el más compacto - veremos si es el más rápido para operar.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt

// COMMAND ----------

// MAGIC %md
// MAGIC Un archivo GZIP es un formato de archivo comprimido que utiliza el algoritmo de compresión GZIP para reducir el tamaño de archivos grandes. El formato GZIP se utiliza ampliamente para comprimir archivos de texto de gran tamaño, como archivos de registro, y para distribuir paquetes de software a través de Internet.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt.gz

// COMMAND ----------

// MAGIC %md
// MAGIC Snappy es un algoritmo de compresión rápido y eficiente ampliamente utilizado en Apache Spark y otros sistemas de big data. Está diseñado para ofrecer altas velocidades de compresión y descompresión, lo que lo hace muy adecuado para su uso en cadenas de procesamiento de big data.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt.snappy

// COMMAND ----------

// MAGIC %md
// MAGIC Leer en el archivo de formato de compresión Gzip.

// COMMAND ----------

val csvDFgz = spark
  .read
  .option("header", "true")
  .option("sep", ":")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt.gz")

// COMMAND ----------

// MAGIC %md
// MAGIC Aunque el formato sin comprimir ocupaba más espacio que el formato Gzip, su funcionamiento era bastante más rápido que el de este último.

// COMMAND ----------

val csvDFsnappy = spark
  .read
  .option("header", "true")
  .option("sep", ":")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt.snappy")

// COMMAND ----------

// MAGIC %md
// MAGIC Espera, pensé que Snappy se suponía que era divisible - ¿por qué sólo una ranura de lectura en el archivo?
// MAGIC 
// MAGIC Los archivos CSV normales que se comprimen con el formato Snappy no se pueden dividir. Si quieres trabajar con formatos no basados en columnas, deberías usar `bzip2` (Snappy es genial para Parquet, que veremos más adelante).

// COMMAND ----------

// MAGIC %md
// MAGIC BZIP2 es un algoritmo de compresión de archivos que se utiliza habitualmente para comprimir archivos de texto de gran tamaño, como archivos de registro y paquetes de software. Proporciona mayores ratios de compresión que otros algoritmos habituales, como GZIP, lo que lo hace muy adecuado para su uso en cadenas de procesamiento de big data en las que el espacio en disco es un problema.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-with-header-10m.csv.bzip

// COMMAND ----------

// MAGIC %md
// MAGIC En realidad, el archivo bzip ocupa menos espacio que el archivo snappy o gzip. Vamos a leerlo.

// COMMAND ----------

val csvBzip = spark
  .read
  .option("header", "true")
  .option("sep", ":")
  .option("inferSchema", "true")
  .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.csv.bzip")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Profundicemos en los esquemas de compresión y en `inferSchema`...
// MAGIC 
// MAGIC ¿Cómo podemos evitar este paso de inferencia de esquemas?

// COMMAND ----------

csvDF.schema.json

// COMMAND ----------

import org.apache.spark.sql.types._
dbutils.fs.put("/tmp/myschema.json", csvDF.schema.json, true)

val schema_json = dbutils.fs.head("/tmp/myschema.json", Integer.MAX_VALUE)
val knownSchema = DataType.fromJson(schema_json).asInstanceOf[StructType]

// COMMAND ----------

val csvDFgz = spark
  .read
  .option("header", "true")
  .option("sep", ":")
  .schema(knownSchema)
  .csv("/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt.gz")

// COMMAND ----------

// MAGIC %md
// MAGIC Mucho mejor, ¡lo cargamos en menos de un segundo!
// MAGIC 
// MAGIC Ahora comparemos este archivo CSV con Parquet.

// COMMAND ----------

// MAGIC %md
// MAGIC Parquet.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/learning-spark-v2/people/people-10m.parquet/

// COMMAND ----------

// MAGIC %md
// MAGIC Se utiliza una comprensión de lista para recorrer los objetos FileInfo y extraer la propiedad size de cada objeto cuyo nombre termine en ".parquet". La lista de tamaños resultante se almacena en la variable size.

// COMMAND ----------

// MAGIC %python
// MAGIC size = [i.size for i in dbutils.fs.ls("/databricks-datasets/learning-spark-v2/people/people-10m.parquet/") if i.name.endswith(".parquet")]
// MAGIC __builtin__.sum(size)

// COMMAND ----------

// MAGIC %md
// MAGIC Además de que el archivo Parquet ocupa menos de la mitad del espacio necesario para almacenar el archivo de texto sin comprimir, también codifica los nombres de las columnas y sus tipos de datos asociados.
// MAGIC 
// MAGIC ***BONUS*** - ¿Por qué pasamos de 1 archivo CSV a 8 archivos Parquet?

// COMMAND ----------

// MAGIC %md
// MAGIC Leemos el archivo parquet

// COMMAND ----------

val parquetDF = spark.read.parquet("/databricks-datasets/learning-spark-v2/people/people-10m.parquet/")

// COMMAND ----------

// MAGIC %md
// MAGIC Por último, es mucho más rápido operar con ficheros Parquet que con ficheros CSV (especialmente cuando filtramos o seleccionamos un subconjunto de columnas). 
// MAGIC 
// MAGIC Fíjate en la diferencia de tiempos `%timeit` es una función incorporada en Python, así que vamos a crear vistas temporales para acceder a los datos en Python.

// COMMAND ----------

parquetDF.createOrReplaceTempView("parquetDF")
csvDF.createOrReplaceTempView("csvDF")
csvDFgz.createOrReplaceTempView("csvDFgz")

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("parquetDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %md
// MAGIC Como utilizamos Databricks, las siguientes llamadas a este archivo Parquet serán más rápidas gracias al almacenamiento automático en caché.

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("parquetDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %md
// MAGIC En parquet llegamos a tardar solo 1.62 segundos en seleccionar el género y salario, cuando este es mayor a 10000.

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("csvDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("csvDFgz").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %md
// MAGIC Sin embargo cuando trabajamos en csv este tarda 22 segundos.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Comparación
// MAGIC | Type    | <span style="white-space:nowrap">Inference Type</span> | <span style="white-space:nowrap">Inference Speed</span> | Reason                                          | <span style="white-space:nowrap">Should Supply Schema?</span> |
// MAGIC |---------|--------------------------------------------------------|---------------------------------------------------------|----------------------------------------------------|:--------------:|
// MAGIC | <b>CSV</b>     | <span style="white-space:nowrap">Full-Data-Read</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
// MAGIC | <b>Parquet</b> | <span style="white-space:nowrap">Metadata-Read</span>  | <span style="white-space:nowrap">Fast/Medium</span>     | <span style="white-space:nowrap">Number of Partitions</span> | No (most cases)             |
// MAGIC | <b>Tables</b>  | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">Predefined</span> | n/a            |
// MAGIC | <b>JSON</b>    | <span style="white-space:nowrap">Full-Read-Data</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
// MAGIC | <b>Text</b>    | <span style="white-space:nowrap">Dictated</span>       | <span style="white-space:nowrap">Zero</span>            | <span style="white-space:nowrap">Only 1 Column</span>   | Never          |
// MAGIC | <b>JDBC</b>    | <span style="white-space:nowrap">DB-Read</span>        | <span style="white-space:nowrap">Fast</span>            | <span style="white-space:nowrap">DB Schema</span>  | No             |

// COMMAND ----------

// MAGIC %md
// MAGIC ##Reading CSV
// MAGIC - `spark.read.csv(..)`
// MAGIC - Hay un gran número de opciones a la hora de leer ficheros CSV, incluyendo cabeceras, separador de columnas, escapes, etc.
// MAGIC - Podemos permitir que Spark infiera el esquema a costa de leer primero todo el fichero.
// MAGIC - Los ficheros CSV grandes deberían tener siempre un esquema predefinido.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Parquet
// MAGIC - `spark.read.parquet(..)`
// MAGIC - Los archivos parquet son el formato de archivo preferido para big-data.
// MAGIC - Es un formato de archivo columnar.
// MAGIC - Es un formato de archivo divisible.
// MAGIC - Ofrece muchas ventajas de rendimiento con respecto a otros formatos, incluido el pushdown de predicados.
// MAGIC - A diferencia de CSV, el esquema se lee, no se infiere.
// MAGIC - La lectura del esquema a partir de los metadatos de Parquet puede ser extremadamente eficiente.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Tables
// MAGIC - `spark.read.table(..)`
// MAGIC - La plataforma Databricks nos permite registrar una gran variedad de fuentes de datos como tablas a través de la interfaz de usuario de Databricks.
// MAGIC - Cualquier `DataFrame` (de CSV, Parquet, lo que sea) puede ser registrado como una vista temporal.
// MAGIC - Las Tablas/Vistas pueden ser cargadas a través del `DataFrameReader` para producir un `DataFrame`.
// MAGIC - Las tablas/vistas pueden utilizarse directamente en sentencias SQL.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading JSON
// MAGIC - `spark.read.json(..)`
// MAGIC - JSON representa tipos de datos complejos a diferencia del formato plano de CSV.
// MAGIC - Tiene muchas de las mismas limitaciones que CSV (necesidad de leer todo el archivo para deducir el esquema).
// MAGIC - Al igual que CSV, tiene muchas opciones que permiten controlar los formatos de fecha, los escapes, JSON de una o varias líneas, etc.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Text
// MAGIC - `spark.read.text(..)`
// MAGIC - Lee una línea de texto como una sola columna llamada `value`.
// MAGIC - Es la base de formatos de archivo más complejos, como los archivos de texto de ancho fijo.

// COMMAND ----------

// MAGIC %md
// MAGIC - `spark.read.jdbc(..)`
// MAGIC - Requiere una conexión a la base de datos por partición.
// MAGIC - Puede saturar la base de datos.
// MAGIC - Requiere la especificación de un stride para equilibrar correctamente las particiones.