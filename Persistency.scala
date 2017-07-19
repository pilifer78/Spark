// Databricks notebook source
val rdd=sc.parallelize(List(1,2,3,4,5))
val result=rdd.map(x=>x*x)
println(result.count())
println(result.collect().mkString(","))

// COMMAND ----------

//bug to review
import org.apache.spark.SparkContext._

val result=rdd.map(x=>x*x)
result.persist(StorageLevel.DISK_ONLY)
println(result.count())
println(result.collect().mkString(","))
