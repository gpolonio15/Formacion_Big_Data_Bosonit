// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC ## Chapter 5: Spark SQL y DataFrames: Interacción con fuentes de datos externas
// MAGIC Este cuaderno contiene ejemplos de código para el *Capítulo 5: Spark SQL y DataFrames: Interactuando con Fuentes de Datos Externas*.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Funciones definidas por el usuario
// MAGIC Aunque Apache Spark tiene múltiple functiones predefinidas, la flexibilidad de Spark permite a los ingenieros y científicos de datos definir sus propias funciones (es decir, funciones definidas por el usuario o UDFs).  

// COMMAND ----------

// Creamos una función para obtener un número al cubo
val cubed = (s: Long) => {
  s * s * s
}

// Registramos UDF
spark.udf.register("cubed", cubed)

// Creamos una temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

// COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Acelerando y Distribuyendo UDFs de PySpark con UDFs de Pandas
// MAGIC Uno de los problemas que prevalecían anteriormente con el uso de PySpark UDFs era que tenía un rendimiento más lento que Scala UDFs.  Esto se debía a que las UDFs de PySpark requerían movimiento de datos entre la JVM y Python, lo cual era bastante costoso.   Para resolver este problema, en Apache Spark 2.3 se introdujeron las UDFs de pandas (también conocidas como UDFs vectorizadas). Es un UDF que utiliza Apache Arrow para transferir datos y utiliza pandas para trabajar con los datos. Se puede definir una UDF de pandas usando la palabra clave pandas_udf como decorador o para envolver la propia función.   Una vez que los datos están en formato Apache Arrow, ya no hay necesidad de serializar/picklear los datos puesto que ya están en un formato consumible por el proceso Python.  En lugar de operar sobre entradas individuales fila a fila, se está operando sobre una serie o dataframe de pandas (es decir, ejecución vectorizada).

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.functions import col, pandas_udf
// MAGIC from pyspark.sql.types import LongType
// MAGIC 
// MAGIC # Declaramos la función cubed.
// MAGIC def cubed(a: pd.Series) -> pd.Series:
// MAGIC     return a * a * a
// MAGIC 
// MAGIC # Creamos el pandas UDF para la función cubed
// MAGIC cubed_udf = pandas_udf(cubed, returnType=LongType())

// COMMAND ----------

// MAGIC %md
// MAGIC ### Usamos pandas Dataframe

// COMMAND ----------

// MAGIC %python
// MAGIC # Creamos una serie con Pandas
// MAGIC x = pd.Series([1, 2, 3])
// MAGIC 
// MAGIC # La función para un pandas_udf ejecutado con datos locales de Pandas
// MAGIC print(cubed(x))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Uso de Spark DataFrame

// COMMAND ----------

// MAGIC %python
// MAGIC # Creamos un Spark DataFrame
// MAGIC df = spark.range(1, 4)
// MAGIC 
// MAGIC #  Ejecutar la función como una UDF vectorizada de Spark
// MAGIC df.select("id", cubed(col("id"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Funciones de Orden Superior en DataFrames y Spark SQL
// MAGIC 
// MAGIC Dado que los tipos de datos complejos son una amalgama de tipos de datos simples, resulta tentador manipular los tipos de datos complejos directamente. Como se indica en el post *Introducción de nuevas funciones incorporadas y de orden superior para tipos de datos complejos en Apache Spark 2.4*, normalmente hay dos soluciones para la manipulación de tipos de datos complejos.
// MAGIC   1. Desglosar la estructura anidada en filas individuales, aplicar alguna función y, a continuación, volver a crear la estructura anidada como se indica en el fragmento de código siguiente (véase la opción 1)  
// MAGIC   2. Creación de una función definida por el usuario (UDF), opción 2.

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

// Creamos un Array
val arrayData = Seq(
  Row(1, List(1, 2, 3)),
  Row(2, List(2, 3, 4)),
  Row(3, List(3, 4, 5))
)

// Creamos el schema
val arraySchema = new StructType()
  .add("id", IntegerType)
  .add("values", ArrayType(IntegerType))


// Creamos el DataFrame
val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
df.createOrReplaceTempView("table")
df.printSchema()
df.show()

// COMMAND ----------

// Otra forma de poner el Schema.
val arraySchema_1 = StructType(List(
  StructField("id", IntegerType, false),
  StructField("list", ArrayType(IntegerType), false)
))

val df_1 = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema_1)
df_1.createOrReplaceTempView("table_1")
df_1.printSchema()
df_1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Opción 1: Explotar y recopilar
// MAGIC La consulta proporcionada utiliza la función SQL de Spark para seleccionar datos de un DataFrame. La consulta se divide en tres partes:
// MAGIC 
// MAGIC   1. La primera parte utiliza la función "explode" para descomponer una columna "values" que contiene una matriz en varias filas. Cada fila tendrá un valor único de la matriz en la columna "value".
// MAGIC 
// MAGIC   2. La segunda parte utiliza la función "collect_list" para agrupar los valores descompuestos por el ID y crear una nueva columna "newValues" que contiene una lista de los valores descompuestos más 1.
// MAGIC 
// MAGIC   3. La tercera parte utiliza la clausula "GROUP BY" para agrupar las filas por ID.

// COMMAND ----------

spark.sql("""
SELECT id, collect_list(value + 1) AS newValues
  FROM  (SELECT id, explode(values) AS value
        FROM table) x
 GROUP BY id
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Opción 2: Función definida por el usuario

// COMMAND ----------

// Creamos la UDF
def addOne(values: Seq[Int]): Seq[Int] = {
    values.map(value => value + 1)
}

// Registramos la UDF
val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[Int]): Seq[Int])

// Query data
spark.sql("SELECT id, plusOneInt(values) AS values FROM table").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Funciones de orden superior
// MAGIC Además de las funciones incorporadas mencionadas anteriormente, existen funciones de orden superior que toman funciones lambda anónimas como argumentos. 

// COMMAND ----------

// In Scala
// Creamos un DataFrame con dos filas de dos arrays (tempc1, tempc2)
val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsius")
tC.createOrReplaceTempView("tC")

// Mostramos el DataFrame
tC.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transform
// MAGIC 
// MAGIC `transform(array<T>, function<T, U>): array<U>`
// MAGIC 
// MAGIC La función transform produce un array aplicando una función a cada elemento de un array de entrada (similar a una función map).

// COMMAND ----------

// Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Filter
// MAGIC 
// MAGIC `filter(array<T>, función<T, booleana>): array<T>`
// MAGIC 
// MAGIC La función filtro produce un array donde la función booleana es verdadera.

// COMMAND ----------

// Filter temperatures > 38C for array of temperatures
spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Exists
// MAGIC 
// MAGIC `exists(array<T>, function<T, V, Boolean>): Boolean`
// MAGIC 
// MAGIC La función existe devuelve verdadero si la función booleana es válida para cualquier elemento de la matriz de entrada.

// COMMAND ----------

// Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT celsius, exists(celsius, t -> t = 38) as threshold
FROM tC
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Reduce
// MAGIC 
// MAGIC `reduce(array<T>, B, function<B, T, B>, function<B, R>)`
// MAGIC 
// MAGIC La función reducir reduce los elementos de la matriz a un único valor fusionando los elementos en un búfer B mediante la función<B, T, B> y aplicando una función de acabado<B, R> en el búfer final.

// COMMAND ----------

// Calculate average temperature and convert to F
spark.sql("""
SELECT celsius, 
       reduce(
          celsius, 
          0, 
          (t, acc) -> t + acc, 
          acc -> (acc div size(celsius) * 9 div 5) + 32
        ) as avgFahrenheit 
  FROM tC
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Aplica la función "reducir" a la columna "celsius". La función toma tres argumentos: el valor inicial (0), una función lambda que calcula la suma de los elementos y otra función lambda que convierte la suma de Celsius a Fahrenheit.
// MAGIC 
// MAGIC El resultado de la función "reducir" se asigna a una nueva columna "avgFahrenheit".

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrames y Operadores Relacionales Comunes de Spark SQL
// MAGIC 
// MAGIC La potencia de Spark SQL es que contiene muchas Operaciones DataFrame (también conocidas como Operaciones Untyped Dataset). 
// MAGIC 
// MAGIC Para ver la lista completa, consulte [Spark SQL, Funciones incorporadas](https://spark.apache.org/docs/latest/api/sql/index.html).
// MAGIC 
// MAGIC En la siguiente sección, nos centraremos en los siguientes operadores relacionales comunes:
// MAGIC * Uniones y uniones
// MAGIC * Ventanas
// MAGIC * Modificaciones

// COMMAND ----------

import org.apache.spark.sql.functions._

// Cargamos los File Paths
val delaysPath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

// Leemos el airport-codes-na.txt, pasandolo a csv.
val airports = spark
  .read
  .options(
    Map(
      "header" -> "true", 
      "inferSchema" ->  "true", 
      "sep" -> "\t"))
  .csv(airportsPath)

// Creamos la vista temporal
airports.createOrReplaceTempView("airports_na")

// Obtenemos departuredelays.csv
val delays = spark
  .read
  .option("header","true")
  .csv(delaysPath)
  .withColumn("delay", expr("CAST(delay as INT) as delay"))
  .withColumn("distance", expr("CAST(distance as INT) as distance"))

// Creamos la vista temporal
delays.createOrReplaceTempView("departureDelays")

// Creamos una pequeña tabla temporal
val foo = delays
  .filter(
    expr("""
         origin == 'SEA' AND 
         destination == 'SFO' AND 
         date like '01010%' AND delay > 0
         """))

foo.createOrReplaceTempView("foo")

// COMMAND ----------

spark.sql("SELECT * FROM airports_na").show(20)

// COMMAND ----------

spark.sql("SELECT * FROM departureDelays").show(20)

// COMMAND ----------

spark.sql("SELECT * FROM foo").show(20)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Unions

// COMMAND ----------

// MAGIC %md
// MAGIC En Spark, la operación de unión se utiliza para combinar las filas de dos o más DataFrames en un único DataFrame. El DataFrame resultante contendrá todas las filas de los DataFrames de entrada, eliminando las filas duplicadas. La operación de unión se puede realizar llamando al método .union() en un DataFrame y pasando uno o más DataFrames como argumentos.
// MAGIC 
// MAGIC Es importante tener en cuenta que la operación de unión requiere que los DataFrames de entrada tengan el mismo esquema (es decir, el mismo número de columnas y los mismos tipos de columna). Si los esquemas de los DataFrames de entrada son diferentes, se lanzará una excepción.

// COMMAND ----------

// Union de la tabla delays con foo
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC En spark SQL.

// COMMAND ----------

spark.sql("""
SELECT * 
FROM bar 
WHERE origin = 'SEA' 
   AND destination = 'SFO' 
   AND date LIKE '01010%' 
   AND delay > 0
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC En Dataframe API

// COMMAND ----------

bar.filter($"origin" === "SEA").filter($"destination" === "SFO").filter($"date".like("01010%")).filter($"delay" > 0).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Joins
// MAGIC Por defecto, es un `inner join`.  También existen las opciones `inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti`.
// MAGIC 
// MAGIC Más información disponible en:
// MAGIC * [PySpark Join](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=join)

// COMMAND ----------

// MAGIC %md
// MAGIC Un inner join es un tipo de operación de unión en bases de datos relacionales que devuelve solo las filas que tienen al menos una coincidencia en ambas tablas.

// COMMAND ----------

// Unir a Departure Delays data (foo) creada antes con flight info
foo.join(
  airports.as('air), 
  $"air.IATA" === $"origin"
).select("City", "State", "date", "delay", "distance", "destination").show()

// COMMAND ----------

// MAGIC %md
// MAGIC En SQL

// COMMAND ----------

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
  FROM foo f
  JOIN airports_na a
    ON a.IATA = f.origin
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Windowing Functions
// MAGIC 
// MAGIC Gran referencia: [Introducción a las funciones de ventana en Spark SQL](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)
// MAGIC 
// MAGIC > En su núcleo, una función de ventana calcula un valor de retorno para cada fila de entrada de una tabla basada en un grupo de filas, llamado Marco. Cada fila de entrada puede tener asociado un marco único. Esta característica de las funciones ventana las hace más potentes que otras funciones y permite a los usuarios expresar de forma concisa diversas tareas de procesamiento de datos que son difíciles (si no imposibles) de expresar sin funciones ventana.

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS departureDelaysWindow_3")
spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
spark.sql("""
CREATE TABLE departureDelaysWindow_3 AS
SELECT origin, destination, sum(delay) as TotalDelays 
  FROM departureDelays 
 WHERE origin IN ('SEA', 'SFO', 'JFK') 
   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
 GROUP BY origin, destination
""")

spark.sql("""SELECT * FROM departureDelaysWindow_3""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos la tabla en Dataframe API

// COMMAND ----------

val departureDelaysWindow_3 = spark.table("departureDelays")
  .filter($"origin".isin("SEA", "SFO", "JFK") && $"destination".isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL"))
  .groupBy("origin", "destination")
  .agg(sum("delay") as "TotalDelays")
departureDelaysWindow_3.createOrReplaceTempView("departureDelaysWindow_3")
departureDelaysWindow_3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ¿Cuáles son los tres principales destinos de los retrasos totales por ciudad de origen de SEA, SFO y JFK?

// COMMAND ----------

// MAGIC %md En la tabla creada ya tengo los aeropuertos de origen 'SEA', 'SFO', 'JFK'

// COMMAND ----------

spark.sql("""
SELECT origin, destination, sum(TotalDelays) as TotalDelays_Sum
 FROM departureDelaysWindow_3
GROUP BY origin, destination
ORDER BY TotalDelays_Sum DESC
LIMIT 10
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC En Dataframe API.

// COMMAND ----------

departureDelaysWindow_3.select($"origin", $"destination", $"TotalDelays")
                      .groupBy("origin", "destination")
                      .agg(sum("TotalDelays").as("TotalDelays"))
                      .orderBy($"TotalDelays".desc).show(10,false)

// COMMAND ----------

// MAGIC %md
// MAGIC Esta es una consulta escrita en Spark SQL que selecciona el origen, el destino, los retrasos totales y el rango de los vuelos de una tabla llamada "departureDelaysWindow_1". La consulta calcula primero el rango de cada vuelo en función de sus retrasos totales, particionado por el origen y ordenado por los retrasos totales en orden descendente. A continuación, filtra para los resultados de vuelos hasta rango 3.

// COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank 
  FROM ( 
     SELECT origin, destination, TotalDelays, dense_rank() 
       OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
       FROM departureDelaysWindow_3
  ) t 
 WHERE rank <= 3
""").show()

// COMMAND ----------

// MAGIC %md En Dataframe API

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

departureDelaysWindow_3.select($"origin", $"destination", $"TotalDelays",
    dense_rank().over(Window.partitionBy($"origin").orderBy($"TotalDelays".desc)).alias("rank"))
    .filter($"rank" <= 3).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Este código seleccionará las columnas "origen", "destino", "TotalDelays" y "rango" del DataFrame "departureDelaysWindow_3".
// MAGIC 
// MAGIC La columna "rango" se calcula utilizando la función dense_rank(), que asigna un rango único a cada fila dentro de una partición (en este caso, particionada por "origen") basándose en el orden especificado por la columna "TotalDelays" en orden descendente.
// MAGIC 
// MAGIC A continuación, el código filtra el DataFrame para incluir sólo las filas en las que el valor de "rank" es menor o igual a 3. Por último, muestra el DataFrame filtrado.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Modifications
// MAGIC 
// MAGIC Otra operación común de DataFrame es realizar modificaciones en el DataFrame. Recordemos que los RDDs subyacentes son inmutables (es decir, no cambian) para asegurar que existe un linaje de datos para las operaciones de Spark. Por lo tanto, aunque los propios DataFrames son inmutables, puede modificarlos mediante operaciones que creen un nuevo DataFrame diferente con columnas diferentes, por ejemplo: 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Añadiendo nuevas Columns

// COMMAND ----------

val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
foo2.show()

// COMMAND ----------

spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC En Dataframe API

// COMMAND ----------

import org.apache.spark.sql.functions._
val foo2 = foo.withColumn("status", when(col("delay") <= 10, "On-time").otherwise("Delayed"))
foo2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Eliminando Columnas

// COMMAND ----------

val foo3 = foo2.drop("delay")
foo3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Renombrando Columnas

// COMMAND ----------

val foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pivoting
// MAGIC Great reference [SQL Pivot: Converting Rows to Columns](https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html)

// COMMAND ----------

spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Este código utiliza la API SQL de Spark para ejecutar una consulta en un DataFrame. La consulta selecciona las columnas "destination", "month" y "delay" de un DataFrame llamado "departureDelays" donde la columna "origin" es igual a "SEA". La columna "mes" se crea utilizando la función SUBSTRING para extraer los dos primeros caracteres de la columna "fecha" y convirtiendo el resultado en un número entero mediante la función CAST. El resultado final se muestra utilizando el método show() con un límite de 10 filas.

// COMMAND ----------

spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
  FROM departureDelays WHERE origin = 'SEA' 
) 
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB, 3 MAR)
)
ORDER BY destination
""").show()

// COMMAND ----------

spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
  FROM departureDelays WHERE origin = 'SEA' 
) 
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Este código utiliza la API SQL de Spark para ejecutar una consulta en un DataFrame. La consulta selecciona primero las columnas "destino", "mes" y "retraso" de un DataFrame llamado "departureDelays" donde la columna "origen" es igual a "SEA". La columna "mes" se crea utilizando la función SUBSTRING para extraer los dos primeros caracteres de la columna "fecha" y convirtiendo el resultado en un número entero mediante la función CAST.
// MAGIC 
// MAGIC A continuación, el código utiliza una subconsulta para pivotar los datos por mes, que se especifica en la cláusula IN de la sentencia PIVOT, en este caso, 1 ENE, 2 FEB, 3 MAR, y la columna pivotante es la columna "mes". La sentencia PIVOT calcula el retraso medio y el retraso máximo para cada destino y mes. La función AVG se utiliza para calcular el retraso medio y la función MAX para calcular el retraso máximo. La función CAST(AVG(delay) AS DECIMAL(4, 2)) se utiliza para convertir los resultados en un decimal con 4 dígitos y 2 decimales.
// MAGIC 
// MAGIC Por último, la consulta utiliza la cláusula ORDER BY para ordenar los resultados por la columna "destino" en orden ascendente. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Rollup
// MAGIC Refer to [What is the difference between cube, rollup and groupBy operators?](https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators)

// COMMAND ----------

// MAGIC %md
// MAGIC GroupBy es la función de agregación más básica. Agrupa los datos por una o más columnas especificadas y realiza funciones de agregación como recuento, suma y promedio en las columnas restantes. Por ejemplo, puede utilizar groupBy("origen") para agrupar los datos por la columna "origen" y, a continuación, calcular el retraso medio de cada grupo.
// MAGIC 
// MAGIC Rollup es similar a groupBy, pero crea subtotales adicionales para cada grupo. Por ejemplo, si utiliza rollup("origen", "destino"), creará un grupo para cada combinación de "origen" y "destino", y también creará subtotales sólo para "origen" y para todo el conjunto de datos.
// MAGIC 
// MAGIC Cube es similar a rollup, pero crea subtotales para todas las combinaciones posibles de columnas, no sólo para las columnas especificadas. Por ejemplo, si utiliza cube("origen", "destino"), creará un grupo para cada combinación de "origen" y "destino", y también creará subtotales para cada columna individual, así como para todo el conjunto de datos.
// MAGIC 
// MAGIC En resumen, groupBy se utiliza para agrupar datos por una o más columnas especificadas, rollup se utiliza para crear subtotales adicionales para cada grupo, y cube se utiliza para crear subtotales para todas las combinaciones posibles de columnas.

// COMMAND ----------

// MAGIC %md
// MAGIC #Ejercicios.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pros y Cons utilizar UDFs

// COMMAND ----------

// MAGIC %md
// MAGIC Pros de utilizar UDF (Funciones Definidas por el Usuario):
// MAGIC 
// MAGIC   1. Flexibilidad: Permiten a los usuarios definir funciones personalizadas para su uso en consultas y procesamiento de datos.
// MAGIC 
// MAGIC   2. Reutilización: Las UDF pueden ser reutilizadas en diferentes consultas y ahorrar tiempo en la escritura de código.
// MAGIC   3. Mejora la legibilidad: Las UDF pueden hacer que las consultas sean más claras y legibles, ya que encapsulan lógicas complejas.
// MAGIC 
// MAGIC Contras de utilizar UDF:
// MAGIC 
// MAGIC   1. Rendimiento: Las UDF pueden ser más lentas que las funciones incorporadas en el sistema, ya que se ejecutan en un contexto separado.
// MAGIC 
// MAGIC   2. Portabilidad: Las UDF pueden no ser compatibles con todas las bases de datos y requerir modificaciones para su uso en sistemas diferentes.
// MAGIC 
// MAGIC   3. Debugging: Debugear una UDF puede ser más difícil que debugear una consulta, ya que las UDF tienen su propio contexto y control de flujo.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Instalar MySQL, descargar driver y cargar datos de BBDD de empleado.

// COMMAND ----------

// MAGIC %md
// MAGIC Encontramos problemas a la hora de cargar el parámetro ".option("driver", "com.mysql.cj.jdbc.Driver") con Databricks Community Edition, este nos indica que la dirección no se encuentra. Aunque este desacargado su conector ("mysql-connector-j-8.0.32") y se intentará cargar con Spark con ("spark-shell --jars ~/jars/mysql-connector-j-8.0.32.jar"). El error parece estar relacionado con la ubicación del archivo jar que se está intentando cargar, este se encuentra en la carpeta Windows (C:). Seguramente sea debido a nuestra versión Databricks Community Edition.
// MAGIC 
// MAGIC Como solución se ha instalado el IDE IntelliJ, para probar que los códigos expuestos proximamente funcionen.

// COMMAND ----------

// MAGIC %python
// MAGIC spark.jars

// COMMAND ----------

// MAGIC %md
// MAGIC El error "AttributeError: 'SparkSession' object has no attribute 'jars'" nos indica que el objeto SparkSession no tiene el atributo "jars". Esto significa que la propiedad "jars" no está disponible en la versión Community de Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1) Cargar con spark datos de empleados y departamentos

// COMMAND ----------

// MAGIC %md
// MAGIC Para conectarse a una base de datos MySQL desde Databricks, siga los siguientes pasos:
// MAGIC 
// MAGIC   1. Crear una cadena de conexión JDBC en MySQL y obtener los detalles de inicio de sesión y la dirección del servidor.
// MAGIC 
// MAGIC   2. Configurar un servidor de firewall para permitir la conexión desde la dirección IP de la cuenta de Databricks.
// MAGIC 
// MAGIC   3. Agregar un recurso de biblioteca JDBC en Databricks.
// MAGIC 
// MAGIC   4. Indicar la tabla de MySQL que queremos que cargar en Databricks, esta se enceuntra en la base de datos [employees](https://dev.mysql.com/doc/employee/en/).

// COMMAND ----------

// MAGIC %md
// MAGIC Como son varias tablas, crearemos una función que abra las distinas tablas de la base de datos employees.

// COMMAND ----------

def load_mysql_table(table, db="employees", user="root", password="test"):
    return (spark
           .read
           .format("jdbc")
           .option("url", "jdbc:mysql://localhost:3306/"+db)
           .option("driver", "com.mysql.cj.jdbc.Driver")
           .option("dbtable", table)
           .option("user", user)
           .option("password", password)
           .load())

// COMMAND ----------

val employeesDF = load_mysql_table("employees")
val departamentsDF = load_mysql_table("departments")
val salariesDF = load_mysql_table("salaries")
val titlesDF = load_mysql_table("titles")
val deptEmpDF = load_mysql_table("dept_emp")
val deptManagerDF = load_mysql_table("dept_manager")

// COMMAND ----------

// MAGIC %md
// MAGIC Aunque si queremos abrir cada tabla de una en una, sería así:

// COMMAND ----------

val employees_table = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost/3306/?user=root/employees")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "employees")
  .option("user", "root")
  .option("password", "Test")
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos las temporaryview para trabajar en SQL.

// COMMAND ----------

employeesDF.createOrReplaceTempView("employees")
departamentsDF.createOrReplaceTempView("departaments")
salariesDF.createOrReplaceTempView("salaries")
titlesDF.createOrReplaceTempView("titles")
deptEmpDF.createOrReplaceTempView("deptEmp")
deptManagerDF.createOrReplaceTempView("deptManager")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2)Mediante Joins mostrar toda la información de los empleados además de su título y salario.

// COMMAND ----------

// MAGIC %md En Dataframe API.

// COMMAND ----------

val infoEmploye = (employeesDF.as("emp")
  .join(salariesDF.as("sal"), $"sal.emp_no" === $"emp.emp_no")
  .join(titlesDF.as("tit"), $"tit.emp_no" === $"emp.emp_no"))
infoEmploye.show()

// COMMAND ----------

// MAGIC %md
// MAGIC En SQL.

// COMMAND ----------

spark.sql("""SELECT emp.*, sal.*, tit.* 
  FROM employees emp
  JOIN salaries sal
    ON emp.emp_no = sal.emp_no
  JOIN titles tit
    ON emp.emp_no = tit.emp_no
""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3) Diferencia entre Rank y dense_rank (operaciones de ventana) 

// COMMAND ----------

// MAGIC %md
// MAGIC Rank y dense_rank son dos funciones de operación de ventana en SQL que se utilizan para asignar un número de clasificación a las filas de un resultado de consulta.
// MAGIC 
// MAGIC La diferencia entre ambos es la forma en que manejan los empates:
// MAGIC 
// MAGIC   1. Rank: Asigna un número de clasificación único a cada fila y salta un número si hay empates. Por ejemplo, si hay dos filas con el mismo valor, se les asignará el mismo número de clasificación y el siguiente número será incrementado en dos.
// MAGIC 
// MAGIC   2. Dense_rank: Asigna un número de clasificación único a cada fila pero no salta ningún número en caso de empates. Por ejemplo, si hay dos filas con el mismo valor, se les asignará el mismo número de clasificación y el siguiente número será incrementado en uno.
// MAGIC 
// MAGIC En resumen, Rank se usa para hacer una clasificación normal, mientras que dense_rank se usa para hacer una clasificación más compacta, sin saltos en los números de clasificación. Un ejemplo:
// MAGIC Esto es:
// MAGIC amount|rank|dense_rank|
// MAGIC |-|-|-|
// MAGIC 50|1|1
// MAGIC 43|2|2
// MAGIC 21|3|3
// MAGIC 21|3|4
// MAGIC 15|5|5

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4) Utilizando operaciones de ventana obtener el salario, posición (cargo) y departamento actual de cada empleado, es decir, el último o más reciente

// COMMAND ----------

val salaryWindow = Window.partitionBy("emp.emp_no").orderBy("sal.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
val titleWindow = Window.partitionBy("emp.emp_no").orderBy("tit.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
val deptWindow = Window.partitionBy("emp.emp_no").orderBy("deptEmp.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

val LastStatsEmpleado = (employeesDF.as("emp")
  .join(salariesDF.as("sal"), $"sal.emp_no" === $"emp.emp_no")
  .join(titlesDF.as("tit"), $"tit.emp_no" === $"emp.emp_no")
  .join(deptEmpDF.as("deptEmp"), $"deptEmp.emp_no" === $"emp.emp_no")
  .withColumn("last_salary", last("sal.salary").over(salaryWindow))
  .withColumn("last_title", last("tit.title").over(titleWindow))
  .withColumn("last_dept", last("deptEmp.dept_no").over(titleWindow))
  .join(departamentsDF.as("dept"), $"dept.dept_no" === $"deptEmp.dept_no")
  .select("emp.emp_no", "last_salary", "last_title", "last_dept")
  .distinct()
  .orderBy("emp_no")
)
LastStatsEmpleado.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 1. Creamos las funciones Window:
// MAGIC          
// MAGIC       "salaryWindow" es una función Window que particiona los datos por "emp.emp_no" y los ordena por "sal.to_date". El rango de la ventana se establece entre los valores mínimo y máximo.
// MAGIC       
// MAGIC       "titleWindow" es una función Window  que divide los datos por "emp.emp_no" y los ordena por "tit.to_date". El rango de la ventana se establece entre los valores mínimo y máximo.
// MAGIC       
// MAGIC       "deptWindow" es una función Window que divide los datos por "emp.emp_no" y los ordena por "deptEmp.to_date". El rango de la ventana se establece entre los valores mínimo y máximo.
// MAGIC 
// MAGIC   2. Unir "employeesDF" con "salariesDF" y "titlesDF" en la columna "emp_no".
// MAGIC 
// MAGIC   3. Unir el resultado con "deptEmpDF" en la columna "emp_no".
// MAGIC 
// MAGIC 4. Utilizar la función "over" y la función agregada "last" para obtener el último valor de "salary", "title" y "dept_no".
// MAGIC 
// MAGIC 5. Unir el resultado con "departmentsDF" en la columna "dept_no".
// MAGIC 
// MAGIC 6. Seleccionar las columnas "emp_no", "last_salary", "last_title", "last_dept".
// MAGIC 
// MAGIC 7. Eliminar duplicados y ordenar el resultado por la columna "emp_no".

// COMMAND ----------

// MAGIC %md
// MAGIC Spark SQL.

// COMMAND ----------

val LastStatsEmpleado = spark.sql("""
  SELECT DISTINCT emp.emp_no, last_salary, last_title, last_dept
  FROM employees emp
  JOIN salaries sal ON sal.emp_no = emp.emp_no
  JOIN titles tit ON tit.emp_no = emp.emp_no
  JOIN deptEmp ON deptEmp.emp_no = emp.emp_no
  JOIN (
    SELECT emp_no, last(salary) over (partition by emp_no ORDER BY to_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_salary,
           last(title) over (partition by emp_no ORDER BY to_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_title,
           last(dept_no) over (partition by emp_no ORDER BY to_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_dept
    FROM salaries sal
    JOIN titles tit ON tit.emp_no = sal.emp_no
    JOIN deptEmp ON deptEmp.emp_no = sal.emp_no
  ) last_data ON last_data.emp_no = emp.emp_no
  JOIN departments dept ON dept.dept_no = deptEmp.dept_no
  ORDER BY emp.emp_no
""")

LastStatsEmpleado.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Esta consulta Spark SQL une varias tablas (empleados, salarios, títulos, deptEmp y departamentos) para obtener información distinta sobre cada empleado como el último salario, el último título y el último departamento de cada empleado que se calculan calculan utilizando funciones de Window (last() over (partition by emp_no ORDER BY to_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) y luego se unen con la tabla de empleados. Los resultados se ordenan por número de empleado (emp_no).
