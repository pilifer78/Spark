// Databricks notebook source
val file=sc.textFile("/mnt/databricks/databricks/wikipedia_e_20140913.11.tsv")

// COMMAND ----------

file.first()

// COMMAND ----------

file.take(1)

// COMMAND ----------

val seed=(100*math.random).toInt

// COMMAND ----------

val sample=file.sample(false,1/10,seed)

// COMMAND ----------

sample.saveAsTextFile("/mnt/databricks/databricks/sampleOut")

// COMMAND ----------

val parsed=file.sample(false,1/10,seed).map(x=>x.split(" ")).map(x=>(x(1),x(2).toInt))

// COMMAND ----------

val reduced=parsed.reduceByKey(_+_)

// COMMAND ----------

val countThenTitle=reduced.map(x=>(x._2, x._1))

// COMMAND ----------

countThenTitle.sortByKey(false).take(10)

// COMMAND ----------

sc.version

// COMMAND ----------

sc.appName

// COMMAND ----------

sc.master

// COMMAND ----------

sc.getExecutorMemoryStatus

// COMMAND ----------

sc.getConf

// COMMAND ----------

sc.getConf.toString()

// COMMAND ----------

sc.getConf.toDebugString
