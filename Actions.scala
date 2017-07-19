// Databricks notebook source
val rdd=sc.parallelize(List(1,2,3,4))
//takes a function that operates on two elements of the type in the rdd and returns a new element
//of the same type
val sum=rdd.reduce((x,y)=>x+y)
println(sum)

// COMMAND ----------

val rdd=sc.parallelize(List(1,2,3,4))
//takes a function but in addition take a zero value to be used for initial call on each partition
val sum=rdd.fold(0)((x,y)=>x+y)
println(sum)

// COMMAND ----------

val result=input.aggregate((0,0))(
(acc,value)=>(acc._1 + value, acc._2 + 1)), 
(acc1,acc2)=>(acc1._1+ acc2._1, acc1._2 + acc2._2))
val avg= result._1 / result._2.toDouble

// COMMAND ----------

val rdd=sc.parallelize(List(1,2,3,4))
rdd.collect()
rdd.count()
rdd.countByValue()
rdd.take(2)
rdd.top(3)
rdd.takeOrdered(2)(Ordering[Int].reverse)
rdd.takeSample(false,1)
