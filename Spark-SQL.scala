// Databricks notebook source
//Creating a Spark Session
import org.apache.spark.sql.SparkSession
val sparkSession = SparkSession.builder
  .master("local")
  .appName("my-spark-app")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()

// COMMAND ----------

val jsonData=spark.read.json("/mnt/databricks/people.json")

// COMMAND ----------

display(jsonData)

// COMMAND ----------

display(spark.sql("select * from people"))

// COMMAND ----------

// To get a list of tables in the current database
val tables = spark.catalog.listTables()
display(tables)
