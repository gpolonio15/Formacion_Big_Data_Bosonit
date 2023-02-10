// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Map & MapPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un DataFrame con dos columnas: número y sus cuadrados

// COMMAND ----------

val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id").repartition(16)

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos algunas funciones usando  _map()_ y _mapPartition()_

// COMMAND ----------

// MAGIC %md La funcion con map(): Este código define una función Scala "func" que recibe un valor Long "v" como argumento. La función establece una conexión con una base de datos ubicada en "/tmp/sqrt.txt", calcula la raíz cuadrada del valor de entrada "v", escribe el resultado en la base de datos, cierra la conexión y devuelve el valor de la raíz cuadrada. Se utiliza "System.lineSeparator()" para escribir un carácter de nueva línea en la base de datos.
// MAGIC 
// MAGIC La función con mapPartition():Este código define una función Scala "funcMapPartitions" que toma dos argumentos, un "conn" de tipo FileWriter y un Long "v". La función calcula la raíz cuadrada del valor de entrada "v", escribe el resultado en el archivo representado por el objeto "conn" utilizando el método ".write", escribe un carácter de nueva línea utilizando "System.lineSeparator()" y devuelve el valor de la raíz cuadrada.
// MAGIC 
// MAGIC Función curried():Este código define una función Scala "benchmark" que recibe una cadena "name" como primer argumento y una función "f" como segundo argumento. La función "benchmark" es una función "curried", lo que significa que puede aplicarse parcialmente antes de proporcionar su segundo argumento.
// MAGIC 
// MAGIC La función "benchmark" calcula el tiempo necesario para ejecutar la función "f". Para ello, registra el tiempo del sistema en nanosegundos antes y después de la ejecución de "f" y, a continuación, calcula la diferencia. 

// COMMAND ----------

import spark.implicits._
import scala.math.sqrt
import java.io.FileWriter

// simulate a connection to a FS
def getConnection(f:String): FileWriter = {
  new FileWriter(f, true)
}

// Función con map()
def func(v: Long) = {
  // Conexión a la DB
  val conn = getConnection("/tmp/sqrt.txt")
  val sr = sqrt(v)
  // Escribimos valor en la DB
  conn.write(sr.toString())
  conn.write(System.lineSeparator()) 
  conn.close()
  sr
}

// Usamos funciñon mapPartition
def funcMapPartitions(conn:FileWriter, v: Long) = {
  val sr = sqrt(v)
  conn.write(sr.toString())
  conn.write(System.lineSeparator())
  sr
}
// Función curried para comparar el tiempo del código.
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Benchmark Map function

// COMMAND ----------

// MAGIC %md
// MAGIC Este código está ejecutando un benchmark en el DataFrame "df" de Spark. El benchmark recibe el nombre de "map function" y la función que se está evaluando es una operación de mapeo aplicada al DataFrame "df". La operación de mapeo transforma cada fila del DataFrame "df" llamando a la función "func" con el valor de la segunda columna de cada fila, representado por "r.getLong(1)".
// MAGIC 
// MAGIC El tiempo necesario para realizar la operación de asignación y mostrar los 10 primeros elementos se calculará e imprimirá en la consola mediante la función "benchmark". El mensaje que se mostrará será del tipo "Time taken in map function: [tiempo en segundos] segundos"

// COMMAND ----------

benchmark("map function") {
  df.map(r => (func(r.getLong(1)))).show(10)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Benchmark MapPartition function

// COMMAND ----------

// MAGIC %md 
// MAGIC Este código está ejecutando un benchmark en el DataFrame de Spark "df". El benchmark recibe el nombre de "mapPartition function" y la función que se está evaluando es una operación mapPartitions aplicada al DataFrame "df". La operación mapPartitions transforma cada partición del DataFrame "df" en una nueva partición.
// MAGIC 
// MAGIC En esta implementación concreta, la función "mapPartitions" crea una única conexión con el archivo de base de datos "/tmp/sqrt.txt" para cada partición. A continuación, mapea cada fila de la partición, llamando a la función "funcMapPartitions" con la conexión y el valor de la segunda columna de cada fila. Los resultados se recogen en una lista y se cierra la conexión. La lista resultante se convierte de nuevo en un iterador y se utiliza para crear un nuevo DataFrame.
// MAGIC 
// MAGIC El tiempo empleado en realizar la operación mapPartitions, convertir el resultado en un nuevo DataFrame y mostrar los 10 primeros elementos se calculará e imprimirá en la consola mediante la función "benchmark". 

// COMMAND ----------

benchmark("mapPartition function") {
  
val newDF = df.mapPartitions(iterator => {
        val conn = getConnection("/tmp/sqrt.txt")
        val result = iterator.map(data=>{funcMapPartitions(conn, data.getLong(1))}).toList
        conn.close()
        result.iterator
      }
  ).toDF().show(10)
}

// COMMAND ----------

// MAGIC %md
// MAGIC El tiempo de aplicación de la función no es muy diferente entre map() y mappartition().

// COMMAND ----------

// MAGIC %md
// MAGIC La diferencia entre las operaciones "map" y "mapPartitions" en Apache Spark se puede describir de la siguiente manera:
// MAGIC 
// MAGIC "map": "map" es una operación de transformación que aplica una función a cada fila individual de un DataFrame. La función se ejecuta en paralelo en cada fila, por lo que cada fila se puede procesar de forma independiente. El resultado de "map" es un nuevo DataFrame con una fila por cada fila del DataFrame original.
// MAGIC 
// MAGIC "mapPartitions": "mapPartitions" es una operación de transformación que aplica una función a cada partición de un DataFrame. La función se ejecuta en cada partición como un todo, por lo que la función puede aprovechar el hecho de que las filas de una partición están juntas. El resultado de "mapPartitions" es un nuevo DataFrame con una fila por cada fila del DataFrame original.
// MAGIC 
// MAGIC Entonces, ¿cuándo usar "map" y cuándo usar "mapPartitions"? "map" es útil cuando la función que se aplica a cada fila es computacionalmente barata, o cuando no necesita hacer uso del hecho de que las filas están juntas en una partición. "mapPartitions" es útil cuando la función que se aplica a las filas es costosa desde el punto de vista computacional, y cuando puede aprovechar el hecho de que las filas están juntas en una partición para optimizar su procesamiento.
// MAGIC 
// MAGIC En resumen, "map" sirve para procesar cada fila individualmente, mientras que "mapPartitions" sirve para procesar todas las filas de una partición a la vez.