// Databricks notebook source
val input=sc.wholeTextFiles("/mnt/SecuenceFiles/SequenceFiles/")
val result=input.mapValues{y=>val nums=y.split(" ").map(x=>x.toDouble)
nums.sum/nums.size.toDouble
}

// COMMAND ----------

display(dbutils.fs.ls("/mnt/SecuenceFiles/SequenceFiles/"))

// COMMAND ----------

result.map(line=>println(line))
result.collect().foreach(println)


// COMMAND ----------

result.take(10).foreach(println)
