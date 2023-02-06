// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Datasets: The DataFrame Query Language vs. Lambdas
// MAGIC 
// MAGIC La API de conjuntos de datos permite utilizar tanto el lenguaje de consulta DataFrame como transformaciones lambda similares a RDD.

// COMMAND ----------

// MAGIC %md
// MAGIC Este código define un caso de clase Persona en Scala con 8 campos: id, firstName, middleName, lastName, gender, birthDate, ssn, y salary.
// MAGIC 
// MAGIC A continuación, el código utiliza Apache Spark para leer un archivo de texto separado por delimitadores (con el delimitador :) y almacenar los datos en un objeto Spark Dataset[Person] llamado personDS.

// COMMAND ----------

case class Person(id: Integer, firstName: String, middleName: String, lastName: String, gender: String, birthDate: String, ssn: String, salary: String)

val personDS = spark
  .read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ":")
  .csv("dbfs:/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt")
  .as[Person]

personDS.cache().count

// COMMAND ----------

// MAGIC %md
// MAGIC Contamos el número de personas distintas que tienen como nombre Nell.

// COMMAND ----------

// DataFrame consulta DSL
println(personDS.filter($"firstName" === "Nell").distinct().count)

// COMMAND ----------

// Dataset, con lambda
println(personDS.filter(x => x.firstName == "Nell").distinct().count)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Efecto Tungsten Encoders' en la optimización Catalyst.
// MAGIC 
// MAGIC El lenguaje específico de dominio (DSL) utilizado por DataFrames y DataSets permite manipular los datos sin tener que deserializarlos del formato de Tungsten. 
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/tuning/dsl-lambda.png" alt="Lambda serialization overhead"/><br/>
// MAGIC 
// MAGIC La ventaja de esto es que evitamos cualquier *serialización / deserialización* sobrecarga. <br/>
// MAGIC 
// MAGIC Los conjuntos de datos ofrecen a los usuarios la posibilidad de manipular los datos mediante lambdas, lo que puede ser muy potente, especialmente con datos semiestructurados. El **desventaja** de las lambdas es que no pueden trabajar directamente con el formato de Tungsten, por lo que es necesaria la deserialización, añadiendo una sobrecarga al proceso.
// MAGIC 
// MAGIC Evitar saltos frecuentes entre DSL y cierres significaría que la *serialización / deserialización* hacia y desde el formato Tungsteno se reduciría, lo que supondría una ganancia de rendimiento.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC - Ventajas del uso de lambdas:
// MAGIC     - Buenas para datos semiestructurados
// MAGIC     - Muy potentes
// MAGIC - Desventajas:
// MAGIC     - Catalyst no puede interpretar las lambdas hasta el tiempo de ejecución. 
// MAGIC     - Las lambdas son opacas para Catalyst. Como no sabe lo que hace una lambda, no puede moverla a otra parte del procesamiento.
// MAGIC     - Saltar entre lambdas y la API de consulta DataFrame puede afectar al rendimiento.
// MAGIC     - Trabajar con lambdas significa que tenemos que `deserializar` desde el formato de Tungsten a un objeto y luego volver a serializar al formato de Tungsten cuando la lambda haya terminado.
// MAGIC     
// MAGIC Si _tienes_ que usar lambdas, encadenarlas puede ayudar.

// COMMAND ----------

// defina el año hace 40 años para la siguiente consulta
import java.util.Calendar

val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40

personDS
  .filter(x => x.birthDate.split("-")(0).toInt > earliestYear) // Por debajo de los 40.
  .filter($"salary" > 80000) // Todos los que ganan más de 80.000
  .filter(x => x.lastName.startsWith("J")) // El apellido empieza por J
  .filter($"firstName".startsWith("D")) // El nombre empieza por D
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC Dataframe API.

// COMMAND ----------

import org.apache.spark.sql.functions._

personDS
  .filter(year($"birthDate") > earliestYear) // Por debajo de los 40.
  .filter($"salary" > 80000) // Todos los que ganan más de 80.000
  .filter($"lastName".startsWith("J")) // El apellido empieza por J
  .filter($"firstName".startsWith("D")) // El nombre empieza por D
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Es mucho más rapido usando Dataframe API.