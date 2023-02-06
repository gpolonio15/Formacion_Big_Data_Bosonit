// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Example 6.3_Scala

// COMMAND ----------

// MAGIC %md
// MAGIC Generamos los datos de muestra

// COMMAND ----------

import scala.util.Random._

// Nuestra case class para el Dataset
case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)
// creamos 1000 instancias de la scala Usage class 
// Esto genera data en el fly
val data = for (i <- 0 to 1000) 
// i: un valor entero, pasado como argumento al método
//"usuario-" + r.alphanumeric.take(5).mkString(""): una cadena, concatenada a partir de la cadena "user-" y el resultado de r.alphanumeric.take(5).mkString(""). r.alphanumeric es probablemente una colección de caracteres alfanuméricos aleatorios, y .take(5) toma los 5 primeros elementos de la misma. .mkString("") concatena todos los elementos de la colección, separados por una cadena vacía.
// r.nextInt(1000): un valor entero aleatorio entre 0 (inclusive) y 1000 (exclusive), generado por el método nextInt en r.
yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))
// Creamos un dataset del Usage tipos de data
val dsUsage = spark.createDataset(data)
dsUsage.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Queremos los valores con un usage mayor que 900, ordenados por usage tambíen en orden descendente.

// COMMAND ----------

import org.apache.spark.sql.functions._

dsUsage
  .filter(d => d.usage > 900)
  .orderBy(desc("usage"))
  .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Closure

// COMMAND ----------

// MAGIC %md
// MAGIC Una función closure es un objeto función que tiene acceso a las variables de su ámbito léxico, incluso cuando la función es invocada fuera de su ámbito original. En otras palabras, una closure es una función que "recuerda" los valores de las variables de su entorno, incluso después de que haya finalizado la ejecución del código que definió.
// MAGIC 
// MAGIC Los closures se utilizan a menudo en los lenguajes de programación funcionales para implementar funciones de orden superior, que son funciones que toman otras funciones como argumentos o devuelven funciones como resultados. Dado que los closures "recuerdan" los valores de las variables de su entorno, pueden utilizarse para capturar el estado del programa en un momento determinado y pasar ese estado a otra función.

// COMMAND ----------

def filterValue(f: Int) = (v: Int) => v > f 

// COMMAND ----------

// MAGIC %md
// MAGIC Funciones High order

// COMMAND ----------

def filterValue2(v: Int, f: Int): Boolean =  v > f 

// COMMAND ----------

// MAGIC %md
// MAGIC El código define una función filterWithUsage que toma un único argumento u de tipo Usage y devuelve un valor booleano que indica si el campo usage del objeto Usage es mayor que 900.
// MAGIC 
// MAGIC El método filter es llamado en el objeto dsUsage, pasando la función filterWithUsage como argumento. Esto filtrará el conjunto de datos dsUsage para que sólo incluya objetos Usage para los que filterWithUsage devuelva true.
// MAGIC 
// MAGIC Por último, se ejecuta el método orderBy en el conjunto de datos filtrado, pasando desc("usage") como argumento. Esto ordena el conjunto de datos filtrado en orden descendente según el campo usage.

// COMMAND ----------

def filterWithUsage(u: Usage) = u.usage > 900

dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(10)

// COMMAND ----------

// Utilizar una expresión lambda if-then-else y calcular un valor
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 }).show(6, false)
// Definir una función a computan en usage
def computeCostUsage(usage: Int): Double = {
  if (usage > 750) usage * 0.15 else usage * 0.50
}
// Usar la función como argumento para map. 
dsUsage.map(u => {computeCostUsage(u.usage)}).show(6, false)

// COMMAND ----------

// MAGIC %md
// MAGIC La funcion creada separada.

// COMMAND ----------

def computeCostUsage(usage: Int): Double = {
  if (usage > 750) usage * 0.15 else usage * 0.50
}

// COMMAND ----------

// MAGIC %md
// MAGIC Aplicamos la función.

// COMMAND ----------

dsUsage.map(u => {computeCostUsage(u.usage)}).show(6, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Sin crear función.

// COMMAND ----------

dsUsage.map(u => { if (u.usage > 750) u.usage * .15 else u.usage * .50 }).show(6, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una clase y una función.

// COMMAND ----------

case class UsageCosts(uid: Int, uname:String, usage: Int, cost: Double)

def computeUserCostUsage(u: Usage): UsageCosts = {
  val v = if (u.usage > 750) u.usage  * 0.15 else u.usage  * 0.50
    UsageCosts(u.uid, u.uname,u.usage, v)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos la función creada.

// COMMAND ----------

dsUsage.map(u => {computeUserCostUsage(u)}).show(6)
