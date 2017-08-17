// Databricks notebook source
display(dbutils.fs.ls("/mnt/databricks"))

// COMMAND ----------

import org.apache.spark.SparkFiles
//val file=sc.addFile("/rute") //not apply in that case
val inFile=sc.textFile("/mnt/databricks/spam.data")

// COMMAND ----------

//convert the line to set of numbers in string format and then convert each of the number to a double
val nums=inFile.map(line=>line.split(' ').map(_.toDouble))
inFile.first()


// COMMAND ----------

nums.first()
