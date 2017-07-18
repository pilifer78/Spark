// Databricks notebook source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// COMMAND ----------

//Create a scala spark context
//val conf=new SparkConf().setMaster("local").setAppName("wordcount")
//val sc=new SparkContext(conf)
val outputFile="databricks/READMEOut.txt"
//load input data
val input=sc.textFile("/mnt/databricks/README.md")
//split it up into words
val words=input.flatMap(line=>line.split(" "))
//transform into pairs and count
val counts=words.map(word=>(word,1)).reduceByKey{case(x,y)=>x+y}
//counts.collect()
counts.saveAsTextFile(outputFile)

