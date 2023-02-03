# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC ## Chapter 5: Spark SQL y DataFrames: Interacción con fuentes de datos externas
# MAGIC Este cuaderno contiene ejemplos de código para el *Capítulo 5: Spark SQL y DataFrames: Interactuando con Fuentes de Datos Externas*.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funciones definidas por el usuario
# MAGIC Aunque Apache Spark tiene múltiple funciones, la flexibilidad de Spark permite a los ingenieros y científicos de datos definir sus propias funciones (es decir, funciones definidas por el usuario o UDFs).   

# COMMAND ----------

from pyspark.sql.types import LongType

# Create cubed function
def cubed(s):
  return s * s * s

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acelerando y Distribuyendo UDFs de PySpark con UDFs de Pandas
# MAGIC Uno de los problemas que prevalecían anteriormente con el uso de PySpark UDFs era que tenía un rendimiento más lento que Scala UDFs.  Esto se debía a que las UDFs de PySpark requerían movimiento de datos entre la JVM y Python, lo cual era bastante costoso.   Para resolver este problema, en Apache Spark 2.3 se introdujeron las UDFs de pandas (también conocidas como UDFs vectorizadas). Es un UDF que utiliza Apache Arrow para transferir datos y utiliza pandas para trabajar con los datos. Se puede definir una UDF de pandas usando la palabra clave pandas_udf como decorador o para envolver la propia función.   Una vez que los datos están en formato Apache Arrow, ya no hay necesidad de serializar/picklear los datos puesto que ya están en un formato consumible por el proceso Python.  En lugar de operar sobre entradas individuales fila a fila, se está operando sobre una serie o dataframe de pandas (es decir, ejecución vectorizada).

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usamos pandas Dataframe

# COMMAND ----------

# Create a Pandas series
x = pd.Series([1, 2, 3])

# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usiamos Spark DataFrame

# COMMAND ----------

# Create a Spark DataFrame
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones de Orden Superior en DataFrames y Spark SQL
# MAGIC 
# MAGIC Dado que los tipos de datos complejos son una amalgama de tipos de datos simples, resulta tentador manipular los tipos de datos complejos directamente. Como se indica en el post *Introducción de nuevas funciones incorporadas y de orden superior para tipos de datos complejos en Apache Spark 2.4*, normalmente hay dos soluciones para la manipulación de tipos de datos complejos.
# MAGIC   1. Desglosar la estructura anidada en filas individuales, aplicar alguna función y, a continuación, volver a crear la estructura anidada como se indica en el fragmento de código siguiente (véase la opción 1)  
# MAGIC   2. Creación de una función definida por el usuario (UDF), opcion 2

# COMMAND ----------

# Creamos un array dataset
arrayData = [[1, (1, 2, 3)], [2, (2, 3, 4)], [3, (3, 4, 5)]]

# Creamos el schema
from pyspark.sql.types import *
arraySchema = (StructType([
      StructField("id", IntegerType(), True), 
      StructField("values", ArrayType(IntegerType()), True)
      ]))

# Creamos el DataFrame
df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
df.createOrReplaceTempView("table")
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Opción 1: Explotar y recopilar
# MAGIC La consulta proporcionada utiliza la función SQL de Spark para seleccionar datos de un DataFrame. La consulta se divide en tres partes:
# MAGIC 
# MAGIC   1. La primera parte utiliza la función "explode" para descomponer una columna "values" que contiene una matriz en varias filas. Cada fila tendrá un valor único de la matriz en la columna "value".
# MAGIC 
# MAGIC   2. La segunda parte utiliza la función "collect_list" para agrupar los valores descompuestos por el ID y crear una nueva columna "newValues" que contiene una lista de los valores descompuestos más 1.
# MAGIC 
# MAGIC   3. La tercera parte utiliza la clausula "GROUP BY" para agrupar las filas por ID. 

# COMMAND ----------

spark.sql("""
SELECT id, collect_list(value + 1) AS newValues
  FROM  (SELECT id, explode(values) AS value
        FROM table) x
 GROUP BY id
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Opción 2: Función definida por el usuario

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType

# Creamos el UDF
def addOne(values):
  return [value + 1 for value in values]

# Registremos el UDF
spark.udf.register("plusOneIntPy", addOne, ArrayType(IntegerType()))  

# Query data
spark.sql("SELECT id, plusOneIntPy(values) AS values FROM table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funciones de orden superior
# MAGIC Además de las funciones incorporadas mencionadas anteriormente, existen funciones de orden superior que toman funciones lambda anónimas como argumentos. 

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

# Enseñaos los datos del DataFrame
t_c.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform
# MAGIC 
# MAGIC `transform(array<T>, function<T, U>): array<U>`
# MAGIC 
# MAGIC La función transform produce un array aplicando una función a cada elemento de un array de entrada (similar a una función map).

# COMMAND ----------

# Calcular los Fahrenheit desde los Celsius en un array de temperaturas
spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter
# MAGIC 
# MAGIC `filter(array<T>, función<T, booleana>): array<T>`
# MAGIC 
# MAGIC La función filtro produce un array donde la función booleana es verdadera.

# COMMAND ----------

# Filtramos las  temperaturas > 38C desde el array de temperaturas
spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exists
# MAGIC 
# MAGIC `exists(array<T>, function<T, V, Boolean>): Boolean`
# MAGIC 
# MAGIC La función existe devuelve verdadero si la función booleana es válida para cualquier elemento de la matriz de entrada.

# COMMAND ----------

# Seleccionamos si hay una temperatura de 38C en el array
spark.sql("""
SELECT celsius, exists(celsius, t -> t = 38) as threshold
FROM tC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reduce
# MAGIC 
# MAGIC `reduce(array<T>, B, function<B, T, B>, function<B, R>)`
# MAGIC 
# MAGIC La función reducir reduce los elementos de la matriz a un único valor fusionando los elementos en un búfer B mediante la función<B, T, B> y aplicando una función de acabado<B, R> en el búfer final.

# COMMAND ----------

# Calculamos la media de temperatura y la convertimoos a Fahrenheit
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

# COMMAND ----------

# MAGIC %md
# MAGIC Aplica la función "reducir" a la columna "celsius". La función toma tres argumentos: el valor inicial (0), una función lambda que calcula la suma de los elementos y otra función lambda que convierte la suma de Celsius a Fahrenheit.
# MAGIC 
# MAGIC El resultado de la función "reducir" se asigna a una nueva columna "avgFahrenheit".

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrames y Operadores Relacionales Comunes de Spark SQL
# MAGIC 
# MAGIC La potencia de Spark SQL es que contiene muchas Operaciones DataFrame (también conocidas como Operaciones Untyped Dataset). 
# MAGIC 
# MAGIC Para ver la lista completa, consulte [Spark SQL, Funciones incorporadas](https://spark.apache.org/docs/latest/api/sql/index.html).
# MAGIC 
# MAGIC En la siguiente sección, nos centraremos en los siguientes operadores relacionales comunes:
# MAGIC * Uniones y uniones
# MAGIC * Ventanas
# MAGIC * Modificaciones

# COMMAND ----------

from pyspark.sql.functions import expr

# Cargamos los File Paths
delays_path = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airports_path = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# Obtenemos el Airpot Dataset
airports = spark.read.options(header="true", inferSchema="true", sep="\t").csv(airports_path)
airports.createOrReplaceTempView("airports_na")

# Obtain departure Delays data
delays = spark.read.options(header="true").csv(delays_path)
delays = (delays
          .withColumn("delay", expr("CAST(delay as INT) as delay"))
          .withColumn("distance", expr("CAST(distance as INT) as distance")))

# Creamos la vista tempora
delays.createOrReplaceTempView("departureDelays")

# Creamos una pequeña tabla temporal
foo = delays.filter(expr("""
            origin == 'SEA' AND 
            destination == 'SFO' AND 
            date like '01010%' AND 
            delay > 0"""))

foo.createOrReplaceTempView("foo")

# COMMAND ----------

spark.sql("SELECT * FROM airports_na").show(20)

# COMMAND ----------

spark.sql("SELECT * FROM departureDelays").show(20)

# COMMAND ----------

spark.sql("SELECT * FROM foo").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unions

# COMMAND ----------

# MAGIC %md
# MAGIC En Spark, la operación de unión se utiliza para combinar las filas de dos o más DataFrames en un único DataFrame. El DataFrame resultante contendrá todas las filas de los DataFrames de entrada, eliminando las filas duplicadas. La operación de unión se puede realizar llamando al método .union() en un DataFrame y pasando uno o más DataFrames como argumentos.
# MAGIC 
# MAGIC Es importante tener en cuenta que la operación de unión requiere que los DataFrames de entrada tengan el mismo esquema (es decir, el mismo número de columnas y los mismos tipos de columna). Si los esquemas de los DataFrames de entrada son diferentes, se lanzará una excepción.

# COMMAND ----------

# Union de la tabla delays con foo
bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC En spark SQL.

# COMMAND ----------

spark.sql("""
SELECT * 
FROM bar 
WHERE origin = 'SEA' 
   AND destination = 'SFO' 
   AND date LIKE '01010%' 
   AND delay > 0
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC En Dataframe API

# COMMAND ----------

bar.filter(col("origin")== "SEA").filter(col("destination") == "SFO").filter(col("date").like("01010%")).filter(col("delay")> 0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins
# MAGIC Por defecto, es un `inner join`.  También existen las opciones `inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti`.
# MAGIC 
# MAGIC Más información disponible en:
# MAGIC * [PySpark Join](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=join)

# COMMAND ----------

# Unir a Departure Delays data (foo) creada antes con flight info
foo.join(
  airports, 
  airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL

# COMMAND ----------

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
  FROM foo f
  JOIN airports_na a
    ON a.IATA = f.origin
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windowing Functions
# MAGIC 
# MAGIC Gran referencia: [Introducción a las funciones de ventana en Spark SQL](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)
# MAGIC 
# MAGIC > En su núcleo, una función de ventana calcula un valor de retorno para cada fila de entrada de una tabla basada en un grupo de filas, llamado Marco. Cada fila de entrada puede tener asociado un marco único. Esta característica de las funciones ventana las hace más potentes que otras funciones y permite a los usuarios expresar de forma concisa diversas tareas de procesamiento de datos que son difíciles (si no imposibles) de expresar sin funciones ventana.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS departureDelaysWindow_py")
spark.sql("""
CREATE TABLE departureDelaysWindow_py AS
SELECT origin, destination, sum(delay) as TotalDelays 
  FROM departureDelays 
 WHERE origin IN ('SEA', 'SFO', 'JFK') 
   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
 GROUP BY origin, destination
""")

spark.sql("""SELECT * FROM departureDelaysWindow_py""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ¿Cuáles son los tres principales destinos de los retrasos totales por ciudad de origen de SEA, SFO y JFK?

# COMMAND ----------

# MAGIC %md En la tabla creada ya tengo los aeropuertos de origen 'SEA', 'SFO', 'JFK'

# COMMAND ----------

# MAGIC %md En este caso solo desde el origen 'SEA'

# COMMAND ----------

spark.sql("""
SELECT origin, destination, sum(TotalDelays) as TotalDelays
 FROM departureDelaysWindow_py
WHERE origin = 'SEA'
GROUP BY origin, destination
ORDER BY sum(TotalDelays) DESC
LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Esta es una consulta escrita en Spark SQL que selecciona el origen, el destino, los retrasos totales y el rango de los vuelos de una tabla llamada "departureDelaysWindow_1". La consulta calcula primero el rango de cada vuelo en función de sus retrasos totales, particionado por el origen y ordenado por los retrasos totales en orden descendente. A continuación, filtra para los resultados de vuelos hasta rango 3.

# COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank 
  FROM ( 
     SELECT origin, destination, TotalDelays, dense_rank() 
       OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
       FROM departureDelaysWindow_py
  ) t 
 WHERE rank <= 3
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modifications
# MAGIC 
# MAGIC Otra operación común de DataFrame es realizar modificaciones en el DataFrame. Recordemos que los RDDs subyacentes son inmutables (es decir, no cambian) para asegurar que existe un linaje de datos para las operaciones de Spark. Por lo tanto, aunque los propios DataFrames son inmutables, puede modificarlos mediante operaciones que creen un nuevo DataFrame diferente con columnas diferentes, por ejemplo: 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Añadiendo nuevas Columns

# COMMAND ----------

foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
foo2.show()

# COMMAND ----------

spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC En Dataframe API

# COMMAND ----------

from pyspark.sql.functions import when, col

foo2 = foo.withColumn("status", when(col("delay") <= 10, "On-time").otherwise("Delayed"))
foo2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminando Columnas

# COMMAND ----------

foo3 = foo2.drop("delay")
foo3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renombrando Columnas

# COMMAND ----------

foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivoting
# MAGIC Great reference [SQL Pivot: Converting Rows to Columns](https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html)

# COMMAND ----------

spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Este código utiliza la API SQL de Spark para ejecutar una consulta en un DataFrame. La consulta selecciona las columnas "destination", "month" y "delay" de un DataFrame llamado "departureDelays" donde la columna "origin" es igual a "SEA". La columna "mes" se crea utilizando la función SUBSTRING para extraer los dos primeros caracteres de la columna "fecha" y convirtiendo el resultado en un número entero mediante la función CAST. El resultado final se muestra utilizando el método show() con un límite de 10 filas.

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC Este código utiliza la API SQL de Spark para ejecutar una consulta en un DataFrame. La consulta selecciona primero las columnas "destino", "mes" y "retraso" de un DataFrame llamado "departureDelays" donde la columna "origen" es igual a "SEA". La columna "mes" se crea utilizando la función SUBSTRING para extraer los dos primeros caracteres de la columna "fecha" y convirtiendo el resultado en un número entero mediante la función CAST.
# MAGIC 
# MAGIC A continuación, el código utiliza una subconsulta para pivotar los datos por mes, que se especifica en la cláusula IN de la sentencia PIVOT, en este caso, 1 ENE, 2 FEB, 3 MAR, y la columna pivotante es la columna "mes". La sentencia PIVOT calcula el retraso medio y el retraso máximo para cada destino y mes. La función AVG se utiliza para calcular el retraso medio y la función MAX para calcular el retraso máximo. La función CAST(AVG(delay) AS DECIMAL(4, 2)) se utiliza para convertir los resultados en un decimal con 4 dígitos y 2 decimales.
# MAGIC 
# MAGIC Por último, la consulta utiliza la cláusula ORDER BY para ordenar los resultados por la columna "destino" en orden ascendente. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rollup
# MAGIC Refer to [What is the difference between cube, rollup and groupBy operators?](https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators)

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy es la función de agregación más básica. Agrupa los datos por una o más columnas especificadas y realiza funciones de agregación como recuento, suma y promedio en las columnas restantes. Por ejemplo, puede utilizar groupBy("origen") para agrupar los datos por la columna "origen" y, a continuación, calcular el retraso medio de cada grupo.
# MAGIC 
# MAGIC Rollup es similar a groupBy, pero crea subtotales adicionales para cada grupo. Por ejemplo, si utiliza rollup("origen", "destino"), creará un grupo para cada combinación de "origen" y "destino", y también creará subtotales sólo para "origen" y para todo el conjunto de datos.
# MAGIC 
# MAGIC Cube es similar a rollup, pero crea subtotales para todas las combinaciones posibles de columnas, no sólo para las columnas especificadas. Por ejemplo, si utiliza cube("origen", "destino"), creará un grupo para cada combinación de "origen" y "destino", y también creará subtotales para cada columna individual, así como para todo el conjunto de datos.
# MAGIC 
# MAGIC En resumen, groupBy se utiliza para agrupar datos por una o más columnas especificadas, rollup se utiliza para crear subtotales adicionales para cada grupo, y cube se utiliza para crear subtotales para todas las combinaciones posibles de columnas.

# COMMAND ----------

# MAGIC %md
# MAGIC #Ejercicios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pros y Cons utilizar UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC Pros de utilizar UDFs (User-Defined Functions) en Python con PySpark:
# MAGIC 
# MAGIC   1. Flexibilidad: UDFs permiten utilizar cualquier función Python en una operación Spark, lo que brinda mayor flexibilidad para realizar operaciones más complejas que no están disponibles en las funciones incorporadas de Spark.
# MAGIC   
# MAGIC   2. Reutilización de código: Las UDFs permiten reutilizar funciones existentes en Python en diferentes operaciones Spark, lo que ayuda a reducir la duplicación de código.
# MAGIC 
# MAGIC Cons de utilizar UDFs en Python con PySpark:
# MAGIC 
# MAGIC   1. Desempeño: Las UDFs pueden ser más lentas que las funciones incorporadas de Spark debido a la sobrecarga adicional de llamar a una función externa.
# MAGIC   
# MAGIC   2. Debugging: Puede ser más difícil depurar errores en UDFs en comparación con funciones incorporadas de Spark.
# MAGIC   
# MAGIC   3. Portabilidad: Las UDFs escritas en Python pueden no ser compatibles con otras implementaciones de Spark que no sean PySpark.
# MAGIC   
# MAGIC    4. Serialización: Las UDFs pueden requerir una mayor cantidad de overhead de serialización y deserialización debido a la necesidad de transmitir objetos Python a través de la red.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Cargar con spark datos de empleados y departamentos

# COMMAND ----------

# MAGIC %md
# MAGIC Para conectarse a una base de datos MySQL desde Databricks, siga los siguientes pasos:
# MAGIC 
# MAGIC   1. Crear una cadena de conexión JDBC en MySQL y obtener los detalles de inicio de sesión y la dirección del servidor.
# MAGIC 
# MAGIC   2. Configurar un servidor de firewall para permitir la conexión desde la dirección IP de la cuenta de Databricks.
# MAGIC 
# MAGIC   3. Agregar un recurso de biblioteca JDBC en Databricks.
# MAGIC 
# MAGIC   4. Indicar la tabla de MySQL que queremos que cargar en Databricks, esta se enceuntra en la base de datos [employees](https://dev.mysql.com/doc/employee/en/).

# COMMAND ----------

# MAGIC %md
# MAGIC Como son varias tablas, crearemos una función que abra las distinas tablas de la base de datos employees.

# COMMAND ----------

def load_mysql_table(table, db= "employees", user="root", password= "Test"):
    return (spark.read
           .format("jdbc")
           .option("url", "jdbc:mysql://localhost:3306/"+db)
           .option("driver", "com.mysql.cj.jdbc.Driver")
           .option("dbtable", table)
           .option("user", user)
           .option("password", password)
           .load())

# COMMAND ----------

employeesDF = load_mysql_table("employees")
departamentsDF = load_mysql_table("departments")
salariesDF = load_mysql_table("salaries")
titulesDF = load_mysql_table("titles")
deptEmpDF = load_mysql_table("dept_emp")
deptManagerDF = load_mysql_table("dept_manager")

# COMMAND ----------

# MAGIC %md
# MAGIC Aunque si queremos abrir cada tabla de una en una, sería así:

# COMMAND ----------

employees_table = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost/3306/?user=root/employees")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "employees")
  .option("user", "root")
  .option("password", "Test")
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC Creamos las temporaryview para trabajar en SQL.

# COMMAND ----------

employeesDF.createOrReplaceTempView("employees")
departamentsDF.createOrReplaceTempView("departaments")
salariesDF.createOrReplaceTempView("salaries")
titlesDF.createOrReplaceTempView("titles")
deptEmpDF.createOrReplaceTempView("deptEmp")
deptManagerDF.createOrReplaceTempView("deptManager")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2)Mediante Joins mostrar toda la información de los empleados además de su título y salario.

# COMMAND ----------

# MAGIC %md En Dataframe API.

# COMMAND ----------

infoEmploye = (employeesDF
  .join(salariesDF, salariesDF.emp_no == employeesDF.emp_no)
  .join(titlesDF, titlesDF.emp_no == employeesDF.emp_no)
)
infoEmploye.show()

# COMMAND ----------

# MAGIC %md
# MAGIC En SQL.

# COMMAND ----------

spark.sql("""SELECT emp.*, sal.*, tit.* 
  FROM employees emp
  JOIN salaries sal
    ON emp.emp_no = sal.emp_no
  JOIN titles tit
    ON emp.emp_no = tit.emp_no
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Diferencia entre Rank y dense_rank (operaciones de ventana) 

# COMMAND ----------

# MAGIC %md
# MAGIC Rank y dense_rank son dos funciones de operación de ventana en SQL que se utilizan para asignar un número de clasificación a las filas de un resultado de consulta.
# MAGIC 
# MAGIC La diferencia entre ambos es la forma en que manejan los empates:
# MAGIC 
# MAGIC   1. Rank: Asigna un número de clasificación único a cada fila y salta un número si hay empates. Por ejemplo, si hay dos filas con el mismo valor, se les asignará el mismo número de clasificación y el siguiente número será incrementado en dos.
# MAGIC 
# MAGIC   2. Dense_rank: Asigna un número de clasificación único a cada fila pero no salta ningún número en caso de empates. Por ejemplo, si hay dos filas con el mismo valor, se les asignará el mismo número de clasificación y el siguiente número será incrementado en uno.
# MAGIC 
# MAGIC En resumen, Rank se usa para hacer una clasificación normal, mientras que dense_rank se usa para hacer una clasificación más compacta, sin saltos en los números de clasificación. Un ejemplo:
# MAGIC Esto es:
# MAGIC amount|rank|dense_rank|
# MAGIC |-|-|-|
# MAGIC 50|1|1
# MAGIC 43|2|2
# MAGIC 21|3|3
# MAGIC 21|3|4
# MAGIC 15|5|5

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Utilizando operaciones de ventana obtener el salario, posición (cargo) y departamento actual de cada empleado, es decir, el último o más reciente

# COMMAND ----------

from pyspark.sql.functions import last

salaryWindow = Window.partitionBy("emp.emp_no").orderBy("sal.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
titleWindow = Window.partitionBy("emp.emp_no").orderBy("tit.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
deptWindow = Window.partitionBy("emp.emp_no").orderBy("deptEmp.to_date").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

LastStatsEmpleado = (employeesDF.alias("emp")
  .join(salariesDF.alias("sal"), salariosDF.emp_no == empleadosDF.emp_no)
  .join(titlesDF.alias("tit"), titulosDF.emp_no == empleadosDF.emp_no)
  .join(deptEmpDF.alias("deptEmp"), deptEmpDF.emp_no == empleadosDF.emp_no)
  .withColumn("last_salary", last("sal.salary").over(salaryWindow))
  .withColumn("last_title", last("tit.title").over(titleWindow))
  .withColumn("last_dept", last("deptEmp.dept_no").over(titleWindow))
  .join(departamentsDF.alias("dept"), departamentosDF.dept_no == deptEmpDF.dept_no)
  .select("emp.emp_no", "last_salary", "last_title", "last_dept")
  .distinct()
  .orderBy("emp_no")
)
LastStatsEmpleado.show()

# COMMAND ----------

# MAGIC %md
# MAGIC El objetivo principal de la consulta es obtener información sobre los empleados, incluyendo su último salario, título y departamento. Para hacer esto, se realizan varios "joins" para combinar datos de varias tablas y luego se utilizan operaciones de ventana para calcular los últimos valores de salario, título y departamento para cada empleado. Los "withColumn"se utilizan para agregar columnas adicionales al resultado de la consulta.
# MAGIC 
# MAGIC Por último, se utiliza el método "select" para seleccionar las columnas relevantes y el método "distinct" para eliminar duplicados. Finalmente, se utiliza el método "orderBy" para ordenar el resultado por "emp_no".
# MAGIC 
# MAGIC El resultado final es un conjunto de datos que contiene información sobre los empleados, incluyendo su número de empleado, su último salario, título y departamento.

# COMMAND ----------

# MAGIC %md
# MAGIC En spark SQL.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Esta consulta Spark SQL une varias tablas (empleados, salarios, títulos, deptEmp y departamentos) para obtener información distinta sobre cada empleado como el último salario, el último título y el último departamento de cada empleado que se calculan calculan utilizando funciones de Window (last() over (partition by emp_no ORDER BY to_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) y luego se unen con la tabla de empleados. Los resultados se ordenan por número de empleado (emp_no).