// Databricks notebook source
val input=sc.parallelize(List(1,2,3,4))
val result=input.map(x=>x*x)
println(result.collect().mkString(","))

// COMMAND ----------

val input=sc.parallelize(List(2,4,6,8))
val result=input.map(x=>x+5)
println(result.collect().mkString(","))

// COMMAND ----------

val lines=sc.parallelize(List("hello world", "hi"))
val words=lines.flatMap(line=>line.split(" "))
words.first() //return hello

// COMMAND ----------

val lines=sc.parallelize(List("one", "two", "three"))
val oneword=sc.parallelize(List("one","four","five"))
val un=lines.union(oneword)
un.collect()

// COMMAND ----------

val lines=sc.parallelize(List("one", "two", "three"))
val oneword=sc.parallelize(List("one","four","five"))
val inter=lines.intersection(oneword)
inter.collect()

// COMMAND ----------

val lines=sc.parallelize(List("one", "two", "three"))
val oneword=sc.parallelize(List("one","four","five"))
val sub=lines.subtract(oneword)
sub.collect()

// COMMAND ----------

val lines=sc.parallelize(List("one", "two", "three"))
val oneword=sc.parallelize(List("one","four","five"))
val car=lines.cartesian(oneword)
car.collect()

// COMMAND ----------

val lines=sc.parallelize(List("hello world", "hi", "hello", "hi"))
val words=lines.flatMap(line=>line.split(" "))
words.first() //return hello
words.distinct().collect()

// COMMAND ----------

val lines=sc.parallelize(List("hello world", "hi"))
val words=lines.sample(false,0.5)
words.first() //return hello
