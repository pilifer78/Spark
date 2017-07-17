// Databricks notebook source
sc

// COMMAND ----------

val myRDD=sc.textFile("/mnt/databricks/README.md")

// COMMAND ----------



// COMMAND ----------

myRDD.count()

// COMMAND ----------

myRDD.collect()

// COMMAND ----------

val logFiles=sc.textFile("/mnt/databricks/README.md")

// COMMAND ----------

val logdomaincom=logFiles.filter(line=>line.contains("spark"))

// COMMAND ----------

logdomaincom.take(10)

// COMMAND ----------

val logdomaincom=logFiles.filter(line=>line.contains("spark")).count()

// COMMAND ----------

val logdomaincom=logFiles.map(line=>line.split(' ')).count()

// COMMAND ----------

val logdomaincom=logFiles.map(line=>line.split(' ')(0)).take(5)

// COMMAND ----------

logdomaincom.take(10).foreach(println)

// COMMAND ----------

logFiles.saveAsTextFile("/mnt/databricks/outFile.txt")
