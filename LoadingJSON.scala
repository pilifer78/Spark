// Databricks notebook source
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

// COMMAND ----------

val input=sc.textFile("/mnt/databricks/employees.json")

// COMMAND ----------

case class Employee(name: String, salary:Double)
{
  val result = input.flatMap(record => {
try {
Some(mapper.readValue(record, classOf[Person]))
} catch {
case e: Exception => None
}})
}

// COMMAND ----------

//saving json
  result.filter(p=>P.employee).map(mapper.writeValuesAsString(_)).saveAsTextFile(outputfile)
